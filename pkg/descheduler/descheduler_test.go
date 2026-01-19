package descheduler

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	policy "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	apiversion "k8s.io/apimachinery/pkg/version"
	fakediscovery "k8s.io/client-go/discovery/fake"
	"k8s.io/client-go/informers"
	fakeclientset "k8s.io/client-go/kubernetes/fake"
	core "k8s.io/client-go/testing"
	"k8s.io/component-base/featuregate"
	"k8s.io/klog/v2"
	"k8s.io/metrics/pkg/apis/metrics/v1beta1"
	metricsclient "k8s.io/metrics/pkg/client/clientset/versioned"
	fakemetricsclient "k8s.io/metrics/pkg/client/clientset/versioned/fake"
	utilptr "k8s.io/utils/ptr"

	"sigs.k8s.io/descheduler/cmd/descheduler/app/options"
	"sigs.k8s.io/descheduler/pkg/api"
	"sigs.k8s.io/descheduler/pkg/descheduler/evictions"
	"sigs.k8s.io/descheduler/pkg/features"
	"sigs.k8s.io/descheduler/pkg/framework/pluginregistry"
	"sigs.k8s.io/descheduler/pkg/framework/plugins/defaultevictor"
	"sigs.k8s.io/descheduler/pkg/framework/plugins/nodeutilization"
	"sigs.k8s.io/descheduler/pkg/framework/plugins/removeduplicates"
	"sigs.k8s.io/descheduler/pkg/framework/plugins/removepodsviolatingnodetaints"
	"sigs.k8s.io/descheduler/pkg/utils"
	deschedulerversion "sigs.k8s.io/descheduler/pkg/version"
	"sigs.k8s.io/descheduler/test"
)

var (
	podEvictionError     = errors.New("PodEvictionError")
	tooManyRequestsError = &apierrors.StatusError{
		ErrStatus: metav1.Status{
			Status:  metav1.StatusFailure,
			Code:    http.StatusTooManyRequests,
			Reason:  metav1.StatusReasonTooManyRequests,
			Message: "admission webhook \"virt-launcher-eviction-interceptor.kubevirt.io\" denied the request: Eviction triggered evacuation of VMI",
		},
	}
	nodesgvr = schema.GroupVersionResource{Group: "metrics.k8s.io", Version: "v1beta1", Resource: "nodes"}
	podsgvr  = schema.GroupVersionResource{Group: "metrics.k8s.io", Version: "v1beta1", Resource: "pods"}
)

func initFeatureGates() featuregate.FeatureGate {
	featureGates := featuregate.NewFeatureGate()
	featureGates.Add(map[featuregate.Feature]featuregate.FeatureSpec{
		features.EvictionsInBackground: {Default: false, PreRelease: featuregate.Alpha},
	})
	return featureGates
}

func initPluginRegistry() {
	pluginregistry.PluginRegistry = pluginregistry.NewRegistry()
	pluginregistry.Register(removeduplicates.PluginName, removeduplicates.New, &removeduplicates.RemoveDuplicates{}, &removeduplicates.RemoveDuplicatesArgs{}, removeduplicates.ValidateRemoveDuplicatesArgs, removeduplicates.SetDefaults_RemoveDuplicatesArgs, pluginregistry.PluginRegistry)
	pluginregistry.Register(defaultevictor.PluginName, defaultevictor.New, &defaultevictor.DefaultEvictor{}, &defaultevictor.DefaultEvictorArgs{}, defaultevictor.ValidateDefaultEvictorArgs, defaultevictor.SetDefaults_DefaultEvictorArgs, pluginregistry.PluginRegistry)
	pluginregistry.Register(removepodsviolatingnodetaints.PluginName, removepodsviolatingnodetaints.New, &removepodsviolatingnodetaints.RemovePodsViolatingNodeTaints{}, &removepodsviolatingnodetaints.RemovePodsViolatingNodeTaintsArgs{}, removepodsviolatingnodetaints.ValidateRemovePodsViolatingNodeTaintsArgs, removepodsviolatingnodetaints.SetDefaults_RemovePodsViolatingNodeTaintsArgs, pluginregistry.PluginRegistry)
	pluginregistry.Register(nodeutilization.LowNodeUtilizationPluginName, nodeutilization.NewLowNodeUtilization, &nodeutilization.LowNodeUtilization{}, &nodeutilization.LowNodeUtilizationArgs{}, nodeutilization.ValidateLowNodeUtilizationArgs, nodeutilization.SetDefaults_LowNodeUtilizationArgs, pluginregistry.PluginRegistry)
}

func removePodsViolatingNodeTaintsPolicy() *api.DeschedulerPolicy {
	return &api.DeschedulerPolicy{
		Profiles: []api.DeschedulerProfile{
			{
				Name: "Profile",
				PluginConfigs: []api.PluginConfig{
					{
						Name: "RemovePodsViolatingNodeTaints",
						Args: &removepodsviolatingnodetaints.RemovePodsViolatingNodeTaintsArgs{},
					},
					{
						Name: "DefaultEvictor",
						Args: &defaultevictor.DefaultEvictorArgs{},
					},
				},
				Plugins: api.Plugins{
					Filter: api.PluginSet{
						Enabled: []string{
							"DefaultEvictor",
						},
					},
					Deschedule: api.PluginSet{
						Enabled: []string{
							"RemovePodsViolatingNodeTaints",
						},
					},
				},
			},
		},
	}
}

func removeDuplicatesPolicy() *api.DeschedulerPolicy {
	return &api.DeschedulerPolicy{
		Profiles: []api.DeschedulerProfile{
			{
				Name: "Profile",
				PluginConfigs: []api.PluginConfig{
					{
						Name: "RemoveDuplicates",
						Args: &removeduplicates.RemoveDuplicatesArgs{},
					},
					{
						Name: "DefaultEvictor",
						Args: &defaultevictor.DefaultEvictorArgs{},
					},
				},
				Plugins: api.Plugins{
					Filter: api.PluginSet{
						Enabled: []string{
							"DefaultEvictor",
						},
					},
					Balance: api.PluginSet{
						Enabled: []string{
							"RemoveDuplicates",
						},
					},
				},
			},
		},
	}
}

func lowNodeUtilizationPolicy(thresholds, targetThresholds api.ResourceThresholds, metricsEnabled bool) *api.DeschedulerPolicy {
	var metricsSource api.MetricsSource = ""
	if metricsEnabled {
		metricsSource = api.KubernetesMetrics
	}
	return &api.DeschedulerPolicy{
		Profiles: []api.DeschedulerProfile{
			{
				Name: "Profile",
				PluginConfigs: []api.PluginConfig{
					{
						Name: nodeutilization.LowNodeUtilizationPluginName,
						Args: &nodeutilization.LowNodeUtilizationArgs{
							Thresholds:       thresholds,
							TargetThresholds: targetThresholds,
							MetricsUtilization: &nodeutilization.MetricsUtilization{
								Source: metricsSource,
							},
						},
					},
					{
						Name: defaultevictor.PluginName,
						Args: &defaultevictor.DefaultEvictorArgs{},
					},
				},
				Plugins: api.Plugins{
					Filter: api.PluginSet{
						Enabled: []string{
							defaultevictor.PluginName,
						},
					},
					Balance: api.PluginSet{
						Enabled: []string{
							nodeutilization.LowNodeUtilizationPluginName,
						},
					},
				},
			},
		},
	}
}

func initDescheduler(t *testing.T, ctx context.Context, featureGates featuregate.FeatureGate, internalDeschedulerPolicy *api.DeschedulerPolicy, metricsClient metricsclient.Interface, objects ...runtime.Object) (*options.DeschedulerServer, *descheduler, *fakeclientset.Clientset) {
	client := fakeclientset.NewSimpleClientset(objects...)
	eventClient := fakeclientset.NewSimpleClientset(objects...)

	rs, err := options.NewDeschedulerServer()
	if err != nil {
		t.Fatalf("Unable to initialize server: %v", err)
	}
	rs.Client = client
	rs.EventClient = eventClient
	rs.DefaultFeatureGates = featureGates
	rs.MetricsClient = metricsClient

	sharedInformerFactory := informers.NewSharedInformerFactoryWithOptions(rs.Client, 0, informers.WithTransform(trimManagedFields))
	eventBroadcaster, eventRecorder := utils.GetRecorderAndBroadcaster(ctx, client)

	descheduler, err := newDescheduler(ctx, rs, internalDeschedulerPolicy, "v1", eventRecorder, sharedInformerFactory, nil)
	if err != nil {
		eventBroadcaster.Shutdown()
		t.Fatalf("Unable to create a descheduler instance: %v", err)
	}

	sharedInformerFactory.Start(ctx.Done())
	sharedInformerFactory.WaitForCacheSync(ctx.Done())

	return rs, descheduler, client
}

func TestTaintsUpdated(t *testing.T) {
	initPluginRegistry()

	ctx := context.Background()
	n1 := test.BuildTestNode("n1", 2000, 3000, 10, nil)
	n2 := test.BuildTestNode("n2", 2000, 3000, 10, nil)

	p1 := test.BuildTestPod(fmt.Sprintf("pod_1_%s", n1.Name), 200, 0, n1.Name, nil)
	p1.ObjectMeta.OwnerReferences = test.GetReplicaSetOwnerRefList()

	client := fakeclientset.NewSimpleClientset(n1, n2, p1)
	eventClient := fakeclientset.NewSimpleClientset(n1, n2, p1)

	rs, err := options.NewDeschedulerServer()
	if err != nil {
		t.Fatalf("Unable to initialize server: %v", err)
	}
	rs.Client = client
	rs.EventClient = eventClient
	rs.DefaultFeatureGates = initFeatureGates()

	pods, err := client.CoreV1().Pods(p1.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Errorf("Unable to list pods: %v", err)
	}
	if len(pods.Items) < 1 {
		t.Errorf("The pod was evicted before a node was tained")
	}

	n1WithTaint := n1.DeepCopy()
	n1WithTaint.Spec.Taints = []v1.Taint{
		{
			Key:    "key",
			Value:  "value",
			Effect: v1.TaintEffectNoSchedule,
		},
	}

	if _, err := client.CoreV1().Nodes().Update(ctx, n1WithTaint, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("Unable to update node: %v\n", err)
	}

	var evictedPods []string
	client.PrependReactor("create", "pods", podEvictionReactionTestingFnc(&evictedPods, nil, nil))

	if err := RunDeschedulerStrategies(ctx, rs, removePodsViolatingNodeTaintsPolicy(), "v1"); err != nil {
		t.Fatalf("Unable to run descheduler strategies: %v", err)
	}

	if len(evictedPods) != 1 {
		t.Fatalf("Unable to evict pod, node taint did not get propagated to descheduler strategies %v\n", err)
	}
}

func TestDuplicate(t *testing.T) {
	initPluginRegistry()

	ctx := context.Background()
	node1 := test.BuildTestNode("n1", 2000, 3000, 10, nil)
	node2 := test.BuildTestNode("n2", 2000, 3000, 10, nil)

	p1 := test.BuildTestPod("p1", 100, 0, node1.Name, nil)
	p1.Namespace = "dev"
	p2 := test.BuildTestPod("p2", 100, 0, node1.Name, nil)
	p2.Namespace = "dev"
	p3 := test.BuildTestPod("p3", 100, 0, node1.Name, nil)
	p3.Namespace = "dev"

	ownerRef1 := test.GetReplicaSetOwnerRefList()
	p1.ObjectMeta.OwnerReferences = ownerRef1
	p2.ObjectMeta.OwnerReferences = ownerRef1
	p3.ObjectMeta.OwnerReferences = ownerRef1

	client := fakeclientset.NewSimpleClientset(node1, node2, p1, p2, p3)
	eventClient := fakeclientset.NewSimpleClientset(node1, node2, p1, p2, p3)

	rs, err := options.NewDeschedulerServer()
	if err != nil {
		t.Fatalf("Unable to initialize server: %v", err)
	}
	rs.Client = client
	rs.EventClient = eventClient
	rs.DefaultFeatureGates = initFeatureGates()

	pods, err := client.CoreV1().Pods(p1.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Errorf("Unable to list pods: %v", err)
	}

	if len(pods.Items) != 3 {
		t.Errorf("Pods number should be 3 before evict")
	}

	var evictedPods []string
	client.PrependReactor("create", "pods", podEvictionReactionTestingFnc(&evictedPods, nil, nil))

	if err := RunDeschedulerStrategies(ctx, rs, removeDuplicatesPolicy(), "v1"); err != nil {
		t.Fatalf("Unable to run descheduler strategies: %v", err)
	}

	if len(evictedPods) == 0 {
		t.Fatalf("Unable to evict pods\n")
	}
}

func TestRootCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	n1 := test.BuildTestNode("n1", 2000, 3000, 10, nil)
	n2 := test.BuildTestNode("n2", 2000, 3000, 10, nil)
	client := fakeclientset.NewSimpleClientset(n1, n2)
	eventClient := fakeclientset.NewSimpleClientset(n1, n2)
	dp := &api.DeschedulerPolicy{
		Profiles: []api.DeschedulerProfile{}, // no strategies needed for this test
	}

	rs, err := options.NewDeschedulerServer()
	if err != nil {
		t.Fatalf("Unable to initialize server: %v", err)
	}
	rs.Client = client
	rs.EventClient = eventClient
	rs.DeschedulingInterval = 100 * time.Millisecond
	rs.DefaultFeatureGates = initFeatureGates()
	errChan := make(chan error, 1)
	defer close(errChan)

	go func() {
		err := RunDeschedulerStrategies(ctx, rs, dp, "v1")
		errChan <- err
	}()
	cancel()
	select {
	case err := <-errChan:
		if err != nil {
			t.Fatalf("Unable to run descheduler strategies: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Root ctx should have canceled immediately")
	}
}

func TestRootCancelWithNoInterval(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	n1 := test.BuildTestNode("n1", 2000, 3000, 10, nil)
	n2 := test.BuildTestNode("n2", 2000, 3000, 10, nil)
	client := fakeclientset.NewSimpleClientset(n1, n2)
	eventClient := fakeclientset.NewSimpleClientset(n1, n2)
	dp := &api.DeschedulerPolicy{
		Profiles: []api.DeschedulerProfile{}, // no strategies needed for this test
	}

	rs, err := options.NewDeschedulerServer()
	if err != nil {
		t.Fatalf("Unable to initialize server: %v", err)
	}
	rs.Client = client
	rs.EventClient = eventClient
	rs.DeschedulingInterval = 0
	rs.DefaultFeatureGates = initFeatureGates()
	errChan := make(chan error, 1)
	defer close(errChan)

	go func() {
		err := RunDeschedulerStrategies(ctx, rs, dp, "v1")
		errChan <- err
	}()
	cancel()
	select {
	case err := <-errChan:
		if err != nil {
			t.Fatalf("Unable to run descheduler strategies: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Root ctx should have canceled immediately")
	}
}

func TestValidateVersionCompatibility(t *testing.T) {
	type testCase struct {
		name               string
		deschedulerVersion deschedulerversion.Info
		serverVersion      string
		expectError        bool
	}
	testCases := []testCase{
		{
			name:               "no error when descheduler minor equals to server minor",
			deschedulerVersion: deschedulerversion.Info{Major: "0", Minor: "26"},
			serverVersion:      "v1.26.1",
			expectError:        false,
		},
		{
			name:               "no error when descheduler minor is 3 behind server minor",
			deschedulerVersion: deschedulerversion.Info{Major: "0", Minor: "23"},
			serverVersion:      "v1.26.1",
			expectError:        false,
		},
		{
			name:               "no error when descheduler minor is 3 ahead of server minor",
			deschedulerVersion: deschedulerversion.Info{Major: "0", Minor: "26"},
			serverVersion:      "v1.26.1",
			expectError:        false,
		},
		{
			name:               "error when descheduler minor is 4 behind server minor",
			deschedulerVersion: deschedulerversion.Info{Major: "0", Minor: "22"},
			serverVersion:      "v1.26.1",
			expectError:        true,
		},
		{
			name:               "error when descheduler minor is 4 ahead of server minor",
			deschedulerVersion: deschedulerversion.Info{Major: "0", Minor: "27"},
			serverVersion:      "v1.23.1",
			expectError:        true,
		},
		{
			name:               "no error when using managed provider version",
			deschedulerVersion: deschedulerversion.Info{Major: "0", Minor: "25"},
			serverVersion:      "v1.25.12-eks-2d98532",
			expectError:        false,
		},
	}
	client := fakeclientset.NewSimpleClientset()
	fakeDiscovery, _ := client.Discovery().(*fakediscovery.FakeDiscovery)
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			fakeDiscovery.FakedServerVersion = &apiversion.Info{GitVersion: tc.serverVersion}
			err := validateVersionCompatibility(fakeDiscovery, tc.deschedulerVersion)

			hasError := err != nil
			if tc.expectError != hasError {
				t.Error("unexpected version compatibility behavior")
			}
		})
	}
}

func podEvictionReactionTestingFnc(evictedPods *[]string, isEvictionsInBackground func(podName string) bool, evictionErr error) func(action core.Action) (bool, runtime.Object, error) {
	return func(action core.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() == "eviction" {
			createAct, matched := action.(core.CreateActionImpl)
			if !matched {
				return false, nil, fmt.Errorf("unable to convert action to core.CreateActionImpl")
			}
			if eviction, matched := createAct.Object.(*policy.Eviction); matched {
				if isEvictionsInBackground != nil && isEvictionsInBackground(eviction.GetName()) {
					return true, nil, tooManyRequestsError
				}
				if evictionErr != nil {
					return true, nil, evictionErr
				}
				*evictedPods = append(*evictedPods, eviction.GetName())
				return true, nil, nil
			}
		}
		return false, nil, nil // fallback to the default reactor
	}
}

func taintNodeNoSchedule(node *v1.Node) {
	node.Spec.Taints = []v1.Taint{
		{
			Key:    "key",
			Value:  "value",
			Effect: v1.TaintEffectNoSchedule,
		},
	}
}

func TestPodEvictorReset(t *testing.T) {
	initPluginRegistry()

	ctx := context.Background()
	node1 := test.BuildTestNode("n1", 2000, 3000, 10, taintNodeNoSchedule)
	node2 := test.BuildTestNode("n2", 2000, 3000, 10, nil)

	ownerRef1 := test.GetReplicaSetOwnerRefList()
	updatePod := func(pod *v1.Pod) {
		pod.Namespace = "dev"
		pod.ObjectMeta.OwnerReferences = ownerRef1
	}

	p1 := test.BuildTestPod("p1", 100, 0, node1.Name, updatePod)
	p2 := test.BuildTestPod("p2", 100, 0, node1.Name, updatePod)

	internalDeschedulerPolicy := removePodsViolatingNodeTaintsPolicy()
	ctxCancel, cancel := context.WithCancel(ctx)
	_, descheduler, client := initDescheduler(t, ctxCancel, initFeatureGates(), internalDeschedulerPolicy, nil, node1, node2, p1, p2)
	defer cancel()

	var evictedPods []string
	client.PrependReactor("create", "pods", podEvictionReactionTestingFnc(&evictedPods, nil, nil))

	// First descheduling cycle
	klog.Infof("2 pod eviction expected per a descheduling cycle, 2 real evictions in total")
	if err := descheduler.runDeschedulerLoop(ctx); err != nil {
		t.Fatalf("Unable to run a descheduling loop: %v", err)
	}
	if descheduler.podEvictor.TotalEvicted() != 2 || len(evictedPods) != 2 {
		t.Fatalf("Expected (2,2) pods evicted, got (%v, %v) instead", descheduler.podEvictor.TotalEvicted(), len(evictedPods))
	}

	// Second descheduling cycle
	klog.Infof("2 pod eviction expected per a descheduling cycle, 4 real evictions in total")
	if err := descheduler.runDeschedulerLoop(ctx); err != nil {
		t.Fatalf("Unable to run a descheduling loop: %v", err)
	}
	if descheduler.podEvictor.TotalEvicted() != 2 || len(evictedPods) != 4 {
		t.Fatalf("Expected (2,4) pods evicted, got (%v, %v) instead", descheduler.podEvictor.TotalEvicted(), len(evictedPods))
	}
}

func TestPodEvictorResetDryRun(t *testing.T) {
	initPluginRegistry()

	ctx := context.Background()
	node1 := test.BuildTestNode("n1", 2000, 3000, 10, taintNodeNoSchedule)
	node2 := test.BuildTestNode("n2", 2000, 3000, 10, nil)

	ownerRef1 := test.GetReplicaSetOwnerRefList()
	updatePod := func(pod *v1.Pod) {
		pod.Namespace = "dev"
		pod.ObjectMeta.OwnerReferences = ownerRef1
	}

	p1 := test.BuildTestPod("p1", 100, 0, node1.Name, updatePod)
	p2 := test.BuildTestPod("p2", 100, 0, node1.Name, updatePod)

	internalDeschedulerPolicy := removePodsViolatingNodeTaintsPolicy()
	ctxCancel, cancel := context.WithCancel(ctx)
	defer cancel()

	// Create descheduler with DryRun mode
	client := fakeclientset.NewSimpleClientset(node1, node2, p1, p2)
	eventClient := fakeclientset.NewSimpleClientset(node1, node2, p1, p2)

	rs, err := options.NewDeschedulerServer()
	if err != nil {
		t.Fatalf("Unable to initialize server: %v", err)
	}
	rs.Client = client
	rs.EventClient = eventClient
	rs.DefaultFeatureGates = initFeatureGates()
	rs.DryRun = true // Set DryRun before creating descheduler

	sharedInformerFactory := informers.NewSharedInformerFactoryWithOptions(rs.Client, 0, informers.WithTransform(trimManagedFields))
	eventBroadcaster, eventRecorder := utils.GetRecorderAndBroadcaster(ctxCancel, client)
	defer eventBroadcaster.Shutdown()

	descheduler, err := newDescheduler(ctxCancel, rs, internalDeschedulerPolicy, "v1", eventRecorder, sharedInformerFactory, nil)
	if err != nil {
		t.Fatalf("Unable to create a descheduler instance: %v", err)
	}

	sharedInformerFactory.Start(ctxCancel.Done())
	sharedInformerFactory.WaitForCacheSync(ctxCancel.Done())
	if descheduler.fakeSharedInformerFactory != nil {
		descheduler.fakeSharedInformerFactory.Start(ctxCancel.Done())
		descheduler.fakeSharedInformerFactory.WaitForCacheSync(ctxCancel.Done())
	}

	// Set up fake eviction tracking
	var fakeEvictedPods []string
	descheduler.fakeClient.PrependReactor("create", "pods", podEvictionReactionTestingFnc(&fakeEvictedPods, nil, nil))

	// First descheduling cycle
	klog.Infof("2 pod eviction expected per a descheduling cycle, 2 fake evictions in total")
	if err := descheduler.runDeschedulerLoop(ctx); err != nil {
		t.Fatalf("Unable to run a descheduling loop: %v", err)
	}
	if descheduler.podEvictor.TotalEvicted() != 2 || len(fakeEvictedPods) != 2 {
		t.Fatalf("Expected (2,2) pods evicted, got (%v, %v) instead", descheduler.podEvictor.TotalEvicted(), len(fakeEvictedPods))
	}

	// Second descheduling cycle
	klog.Infof("2 pod eviction expected per a descheduling cycle, 4 fake evictions in total")
	if err := descheduler.runDeschedulerLoop(ctx); err != nil {
		t.Fatalf("Unable to run a descheduling loop: %v", err)
	}
	if descheduler.podEvictor.TotalEvicted() != 2 || len(fakeEvictedPods) != 4 {
		t.Fatalf("Expected (2,4) pods evicted, got (%v, %v) instead", descheduler.podEvictor.TotalEvicted(), len(fakeEvictedPods))
	}
}

func TestEvictedPodRestorationInDryRun(t *testing.T) {
	// Initialize klog flags
	klog.InitFlags(nil)

	// Set verbosity level (higher number = more verbose)
	// 0 = errors only, 1-4 = info, 5-9 = debug, 10+ = trace
	flag.Set("v", "4")

	initPluginRegistry()

	ctx := context.Background()
	node1 := test.BuildTestNode("n1", 2000, 3000, 10, taintNodeNoSchedule)
	node2 := test.BuildTestNode("n2", 2000, 3000, 10, nil)

	ownerRef1 := test.GetReplicaSetOwnerRefList()
	updatePod := func(pod *v1.Pod) {
		pod.Namespace = "dev"
		pod.ObjectMeta.OwnerReferences = ownerRef1
	}

	p1 := test.BuildTestPod("p1", 100, 0, node1.Name, updatePod)

	internalDeschedulerPolicy := removePodsViolatingNodeTaintsPolicy()
	ctxCancel, cancel := context.WithCancel(ctx)
	defer cancel()

	// Create descheduler with DryRun mode
	client := fakeclientset.NewSimpleClientset(node1, node2, p1)
	eventClient := fakeclientset.NewSimpleClientset(node1, node2, p1)

	rs, err := options.NewDeschedulerServer()
	if err != nil {
		t.Fatalf("Unable to initialize server: %v", err)
	}
	rs.Client = client
	rs.EventClient = eventClient
	rs.DefaultFeatureGates = initFeatureGates()
	rs.DryRun = true // Set DryRun before creating descheduler

	sharedInformerFactory := informers.NewSharedInformerFactoryWithOptions(rs.Client, 0, informers.WithTransform(trimManagedFields))
	eventBroadcaster, eventRecorder := utils.GetRecorderAndBroadcaster(ctxCancel, client)
	defer eventBroadcaster.Shutdown()

	descheduler, err := newDescheduler(ctxCancel, rs, internalDeschedulerPolicy, "v1", eventRecorder, sharedInformerFactory, nil)
	if err != nil {
		t.Fatalf("Unable to create a descheduler instance: %v", err)
	}

	// Verify fakeSharedInformerFactory is set in DryRun mode
	if descheduler.fakeSharedInformerFactory == nil {
		t.Fatalf("Expected fakeSharedInformerFactory to be set in DryRun mode, but it is nil")
	}

	sharedInformerFactory.Start(ctxCancel.Done())
	sharedInformerFactory.WaitForCacheSync(ctxCancel.Done())
	descheduler.fakeSharedInformerFactory.Start(ctxCancel.Done())
	descheduler.fakeSharedInformerFactory.WaitForCacheSync(ctxCancel.Done())

	// Verify the pod exists in the fake client after initialization
	pod, err := descheduler.fakeClient.CoreV1().Pods(p1.Namespace).Get(ctx, p1.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Expected pod %s to exist in fake client after initialization, but got error: %v", p1.Name, err)
	}
	if pod.Name != p1.Name {
		t.Fatalf("Expected pod name %s, got %s", p1.Name, pod.Name)
	}
	klog.Infof("Pod %s exists in fake client after initialization", p1.Name)

	// First descheduling cycle - run profiles
	klog.Infof("Running first descheduling cycle")
	descheduler.podEvictor.ResetCounters()
	descheduler.runProfiles(ctx, descheduler.client)

	// Verify the pod was evicted (should not exist in fake client anymore)
	_, err = descheduler.fakeClient.CoreV1().Pods(p1.Namespace).Get(ctx, p1.Name, metav1.GetOptions{})
	if err == nil {
		t.Fatalf("Expected pod %s to be evicted from fake client, but it still exists", p1.Name)
	}
	if !apierrors.IsNotFound(err) {
		t.Fatalf("Expected NotFound error for pod %s, got: %v", p1.Name, err)
	}
	klog.Infof("Pod %s was successfully evicted from fake client", p1.Name)

	// Verify the pod was added to the evicted pods cache
	evictedPods := descheduler.evictedPodsCache.list()
	if len(evictedPods) != 1 {
		t.Fatalf("Expected 1 pod in evicted cache, got %d", len(evictedPods))
	}
	if evictedPods[0].Namespace != p1.Namespace || evictedPods[0].Name != p1.Name {
		t.Fatalf("Expected evicted pod %s/%s, got %s/%s", p1.Namespace, p1.Name, evictedPods[0].Namespace, evictedPods[0].Name)
	}
	if evictedPods[0].UID != string(p1.UID) {
		t.Fatalf("Expected evicted pod UID %s, got %s", p1.UID, evictedPods[0].UID)
	}
	klog.Infof("Pod %s was successfully added to evicted pods cache (UID: %s)", p1.Name, evictedPods[0].UID)

	// Restore evicted pods
	klog.Infof("Restoring evicted pods from cache")
	if err := descheduler.ir.restoreEvictedPods(descheduler.evictedPodsCache); err != nil {
		t.Fatalf("Failed to restore evicted pods: %v", err)
	}
	descheduler.evictedPodsCache.clear()

	// Verify the pod was restored back to the fake client
	pod, err = descheduler.fakeClient.CoreV1().Pods(p1.Namespace).Get(ctx, p1.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Expected pod %s to be restored to fake client, but got error: %v", p1.Name, err)
	}
	if pod.Name != p1.Name {
		t.Fatalf("Expected restored pod name %s, got %s", p1.Name, pod.Name)
	}
	if string(pod.UID) != string(p1.UID) {
		t.Fatalf("Expected restored pod UID %s, got %s", p1.UID, pod.UID)
	}
	klog.Infof("Pod %s was successfully restored to fake client (UID: %s)", p1.Name, pod.UID)

	// Verify cache was cleared after restoration
	evictedPods = descheduler.evictedPodsCache.list()
	if len(evictedPods) != 0 {
		t.Fatalf("Expected evicted cache to be empty after restoration, got %d pods", len(evictedPods))
	}
	klog.Infof("Evicted pods cache was cleared after restoration")

	// p, e := descheduler.fakeSharedInformerFactory.Core().V1().Pods().Lister().Pods(p1.Namespace).Get(p1.Name)
	// fmt.Printf("p: %#v, e: %v\n", p, e)

	// Second descheduling cycle - run profiles
	klog.Infof("Running second descheduling cycle")
	descheduler.podEvictor.ResetCounters()
	descheduler.runProfiles(ctx, descheduler.client)

	// Verify the pod was evicted again (should not exist in fake client anymore)
	_, err = descheduler.fakeClient.CoreV1().Pods(p1.Namespace).Get(ctx, p1.Name, metav1.GetOptions{})
	if err == nil {
		t.Fatalf("Expected pod %s to be evicted from fake client in second cycle, but it still exists", p1.Name)
	}
	if !apierrors.IsNotFound(err) {
		t.Fatalf("Expected NotFound error for pod %s in second cycle, got: %v", p1.Name, err)
	}
	klog.Infof("Pod %s was successfully evicted from fake client in second cycle", p1.Name)

	// Verify the pod was added to the evicted pods cache again
	evictedPods = descheduler.evictedPodsCache.list()
	if len(evictedPods) != 1 {
		t.Fatalf("Expected 1 pod in evicted cache after second cycle, got %d", len(evictedPods))
	}
	if evictedPods[0].Namespace != p1.Namespace || evictedPods[0].Name != p1.Name {
		t.Fatalf("Expected evicted pod %s/%s in second cycle, got %s/%s", p1.Namespace, p1.Name, evictedPods[0].Namespace, evictedPods[0].Name)
	}
	if evictedPods[0].UID != string(p1.UID) {
		t.Fatalf("Expected evicted pod UID %s in second cycle, got %s", p1.UID, evictedPods[0].UID)
	}
	klog.Infof("Pod %s was successfully added to evicted pods cache in second cycle (UID: %s)", p1.Name, evictedPods[0].UID)

	// Restore evicted pods again
	klog.Infof("Restoring evicted pods from cache after second cycle")
	if err := descheduler.ir.restoreEvictedPods(descheduler.evictedPodsCache); err != nil {
		t.Fatalf("Failed to restore evicted pods in second cycle: %v", err)
	}
	descheduler.evictedPodsCache.clear()

	// Verify the pod was restored back to the fake client again
	pod, err = descheduler.fakeClient.CoreV1().Pods(p1.Namespace).Get(ctx, p1.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Expected pod %s to be restored to fake client after second cycle, but got error: %v", p1.Name, err)
	}
	if pod.Name != p1.Name {
		t.Fatalf("Expected restored pod name %s in second cycle, got %s", p1.Name, pod.Name)
	}
	if string(pod.UID) != string(p1.UID) {
		t.Fatalf("Expected restored pod UID %s in second cycle, got %s", p1.UID, pod.UID)
	}
	klog.Infof("Pod %s was successfully restored to fake client after second cycle (UID: %s)", p1.Name, pod.UID)

	// Verify cache was cleared after restoration in second cycle
	evictedPods = descheduler.evictedPodsCache.list()
	if len(evictedPods) != 0 {
		t.Fatalf("Expected evicted cache to be empty after second restoration, got %d pods", len(evictedPods))
	}
	klog.Infof("Evicted pods cache was cleared after second restoration")
}

func checkTotals(t *testing.T, ctx context.Context, descheduler *descheduler, totalEvictionRequests, totalEvicted uint) {
	if total := descheduler.podEvictor.TotalEvictionRequests(); total != totalEvictionRequests {
		t.Fatalf("Expected %v total eviction requests, got %v instead", totalEvictionRequests, total)
	}
	if total := descheduler.podEvictor.TotalEvicted(); total != totalEvicted {
		t.Fatalf("Expected %v total evictions, got %v instead", totalEvicted, total)
	}
	t.Logf("Total evictions: %v, total eviction requests: %v, total evictions and eviction requests: %v", totalEvicted, totalEvictionRequests, totalEvicted+totalEvictionRequests)
}

func runDeschedulingCycleAndCheckTotals(t *testing.T, ctx context.Context, nodes []*v1.Node, descheduler *descheduler, totalEvictionRequests, totalEvicted uint) {
	err := descheduler.runDeschedulerLoop(ctx)
	if err != nil {
		t.Fatalf("Unable to run a descheduling loop: %v", err)
	}
	checkTotals(t, ctx, descheduler, totalEvictionRequests, totalEvicted)
}

func TestEvictionRequestsCache(t *testing.T) {
	initPluginRegistry()

	ctx := context.Background()
	node1 := test.BuildTestNode("n1", 2000, 3000, 10, taintNodeNoSchedule)
	node2 := test.BuildTestNode("n2", 2000, 3000, 10, nil)
	nodes := []*v1.Node{node1, node2}

	ownerRef1 := test.GetReplicaSetOwnerRefList()
	updatePod := func(pod *v1.Pod) {
		pod.Namespace = "dev"
		pod.ObjectMeta.OwnerReferences = ownerRef1
		pod.Status.Phase = v1.PodRunning
	}
	updatePodWithEvictionInBackground := func(pod *v1.Pod) {
		updatePod(pod)
		pod.Annotations = map[string]string{
			evictions.EvictionRequestAnnotationKey: "",
		}
	}

	p1 := test.BuildTestPod("p1", 100, 0, node1.Name, updatePodWithEvictionInBackground)
	p2 := test.BuildTestPod("p2", 100, 0, node1.Name, updatePodWithEvictionInBackground)
	p3 := test.BuildTestPod("p3", 100, 0, node1.Name, updatePod)
	p4 := test.BuildTestPod("p4", 100, 0, node1.Name, updatePod)
	p5 := test.BuildTestPod("p5", 100, 0, node1.Name, updatePod)

	internalDeschedulerPolicy := removePodsViolatingNodeTaintsPolicy()
	ctxCancel, cancel := context.WithCancel(ctx)
	featureGates := featuregate.NewFeatureGate()
	featureGates.Add(map[featuregate.Feature]featuregate.FeatureSpec{
		features.EvictionsInBackground: {Default: true, PreRelease: featuregate.Alpha},
	})
	_, descheduler, client := initDescheduler(t, ctxCancel, featureGates, internalDeschedulerPolicy, nil, node1, node2, p1, p2, p3, p4)
	defer cancel()

	var fakeEvictedPods []string
	descheduler.podEvictionReactionFnc = func(*fakeclientset.Clientset, *evictedPodsCache) func(action core.Action) (bool, runtime.Object, error) {
		return podEvictionReactionTestingFnc(&fakeEvictedPods, nil, podEvictionError)
	}

	var evictedPods []string
	client.PrependReactor("create", "pods", podEvictionReactionTestingFnc(&evictedPods, func(name string) bool { return name == "p1" || name == "p2" }, nil))

	klog.Infof("2 evictions in background expected, 2 normal evictions")
	runDeschedulingCycleAndCheckTotals(t, ctx, nodes, descheduler, 2, 2)

	klog.Infof("Repeat the same as previously to confirm no more evictions in background are requested")
	// No evicted pod is actually deleted on purpose so the test can run the descheduling cycle repeatedly
	// without recreating the pods.
	runDeschedulingCycleAndCheckTotals(t, ctx, nodes, descheduler, 2, 2)

	klog.Infof("Scenario: Eviction in background got initiated")
	p2.Annotations[evictions.EvictionInProgressAnnotationKey] = ""
	if _, err := client.CoreV1().Pods(p2.Namespace).Update(context.TODO(), p2, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("unable to update a pod: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	klog.Infof("Repeat the same as previously to confirm no more evictions in background are requested")
	runDeschedulingCycleAndCheckTotals(t, ctx, nodes, descheduler, 2, 2)

	klog.Infof("Scenario: Another eviction in background got initiated")
	p1.Annotations[evictions.EvictionInProgressAnnotationKey] = ""
	if _, err := client.CoreV1().Pods(p1.Namespace).Update(context.TODO(), p1, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("unable to update a pod: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	klog.Infof("Repeat the same as previously to confirm no more evictions in background are requested")
	runDeschedulingCycleAndCheckTotals(t, ctx, nodes, descheduler, 2, 2)

	klog.Infof("Scenario: Eviction in background completed")
	if err := client.CoreV1().Pods(p1.Namespace).Delete(context.TODO(), p1.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("unable to delete a pod: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	klog.Infof("Check the number of evictions in background decreased")
	runDeschedulingCycleAndCheckTotals(t, ctx, nodes, descheduler, 1, 2)

	klog.Infof("Scenario: A new pod without eviction in background added")
	if _, err := client.CoreV1().Pods(p5.Namespace).Create(context.TODO(), p5, metav1.CreateOptions{}); err != nil {
		t.Fatalf("unable to create a pod: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	klog.Infof("Check the number of evictions increased after running a descheduling cycle")
	runDeschedulingCycleAndCheckTotals(t, ctx, nodes, descheduler, 1, 3)

	klog.Infof("Scenario: Eviction in background canceled => eviction in progress annotation removed")
	delete(p2.Annotations, evictions.EvictionInProgressAnnotationKey)
	if _, err := client.CoreV1().Pods(p2.Namespace).Update(context.TODO(), p2, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("unable to update a pod: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	klog.Infof("Check the number of evictions in background decreased")
	checkTotals(t, ctx, descheduler, 0, 3)

	klog.Infof("Scenario: Re-run the descheduling cycle to re-request eviction in background")
	runDeschedulingCycleAndCheckTotals(t, ctx, nodes, descheduler, 1, 3)

	klog.Infof("Scenario: Eviction in background completed with a pod in completed state")
	p2.Status.Phase = v1.PodSucceeded
	if _, err := client.CoreV1().Pods(p2.Namespace).Update(context.TODO(), p2, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("unable to delete a pod: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	klog.Infof("Check the number of evictions in background decreased")
	runDeschedulingCycleAndCheckTotals(t, ctx, nodes, descheduler, 0, 3)
}

func TestDeschedulingLimits(t *testing.T) {
	initPluginRegistry()

	tests := []struct {
		description string
		policy      *api.DeschedulerPolicy
		limit       uint
	}{
		{
			description: "limits per node",
			policy: func() *api.DeschedulerPolicy {
				policy := removePodsViolatingNodeTaintsPolicy()
				policy.MaxNoOfPodsToEvictPerNode = utilptr.To[uint](4)
				return policy
			}(),
			limit: uint(4),
		},
		{
			description: "limits per namespace",
			policy: func() *api.DeschedulerPolicy {
				policy := removePodsViolatingNodeTaintsPolicy()
				policy.MaxNoOfPodsToEvictPerNamespace = utilptr.To[uint](4)
				return policy
			}(),
			limit: uint(4),
		},
		{
			description: "limits per cycle",
			policy: func() *api.DeschedulerPolicy {
				policy := removePodsViolatingNodeTaintsPolicy()
				policy.MaxNoOfPodsToEvictTotal = utilptr.To[uint](4)
				return policy
			}(),
			limit: uint(4),
		},
	}

	ownerRef1 := test.GetReplicaSetOwnerRefList()
	updatePod := func(pod *v1.Pod) {
		pod.Namespace = "dev"
		pod.ObjectMeta.OwnerReferences = ownerRef1
	}

	updatePodWithEvictionInBackground := func(pod *v1.Pod) {
		updatePod(pod)
		pod.Annotations = map[string]string{
			evictions.EvictionRequestAnnotationKey: "",
		}
	}

	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			ctx := context.Background()
			node1 := test.BuildTestNode("n1", 2000, 3000, 10, taintNodeNoSchedule)
			node2 := test.BuildTestNode("n2", 2000, 3000, 10, nil)
			ctxCancel, cancel := context.WithCancel(ctx)
			featureGates := featuregate.NewFeatureGate()
			featureGates.Add(map[featuregate.Feature]featuregate.FeatureSpec{
				features.EvictionsInBackground: {Default: true, PreRelease: featuregate.Alpha},
			})
			_, descheduler, client := initDescheduler(t, ctxCancel, featureGates, tc.policy, nil, node1, node2)
			defer cancel()

			var fakeEvictedPods []string
			descheduler.podEvictionReactionFnc = func(*fakeclientset.Clientset, *evictedPodsCache) func(action core.Action) (bool, runtime.Object, error) {
				return podEvictionReactionTestingFnc(&fakeEvictedPods, nil, podEvictionError)
			}

			var evictedPods []string
			client.PrependReactor("create", "pods", podEvictionReactionTestingFnc(&evictedPods, func(name string) bool { return name == "p1" || name == "p2" }, nil))

			rand.Seed(time.Now().UnixNano())
			pods := []*v1.Pod{
				test.BuildTestPod("p1", 100, 0, node1.Name, updatePodWithEvictionInBackground),
				test.BuildTestPod("p2", 100, 0, node1.Name, updatePodWithEvictionInBackground),
				test.BuildTestPod("p3", 100, 0, node1.Name, updatePod),
				test.BuildTestPod("p4", 100, 0, node1.Name, updatePod),
				test.BuildTestPod("p5", 100, 0, node1.Name, updatePod),
			}

			for i := 0; i < 10; i++ {
				rand.Shuffle(len(pods), func(i, j int) { pods[i], pods[j] = pods[j], pods[i] })
				func() {
					for j := 0; j < 5; j++ {
						idx := j
						if _, err := client.CoreV1().Pods(pods[idx].Namespace).Create(context.TODO(), pods[idx], metav1.CreateOptions{}); err != nil {
							t.Fatalf("unable to create a pod: %v", err)
						}
						defer func() {
							if err := client.CoreV1().Pods(pods[idx].Namespace).Delete(context.TODO(), pods[idx].Name, metav1.DeleteOptions{}); err != nil {
								t.Fatalf("unable to delete a pod: %v", err)
							}
						}()
					}
					time.Sleep(100 * time.Millisecond)

					klog.Infof("2 evictions in background expected, 2 normal evictions")
					err := descheduler.runDeschedulerLoop(ctx)
					if err != nil {
						t.Fatalf("Unable to run a descheduling loop: %v", err)
					}
					totalERs := descheduler.podEvictor.TotalEvictionRequests()
					totalEs := descheduler.podEvictor.TotalEvicted()
					if totalERs+totalEs > tc.limit {
						t.Fatalf("Expected %v evictions and eviction requests in total, got %v instead", tc.limit, totalERs+totalEs)
					}
					t.Logf("Total evictions and eviction requests: %v (er=%v, e=%v)", totalERs+totalEs, totalERs, totalEs)
				}()
			}
		})
	}
}

func TestNodeLabelSelectorBasedEviction(t *testing.T) {
	initPluginRegistry()

	// createNodes creates 4 nodes with different labels and applies a taint to all of them
	createNodes := func() (*v1.Node, *v1.Node, *v1.Node, *v1.Node) {
		taint := []v1.Taint{
			{
				Key:    "test-taint",
				Value:  "test-value",
				Effect: v1.TaintEffectNoSchedule,
			},
		}
		node1 := test.BuildTestNode("n1", 2000, 3000, 10, func(node *v1.Node) {
			node.Labels = map[string]string{
				"zone":        "us-east-1a",
				"node-type":   "compute",
				"environment": "production",
			}
			node.Spec.Taints = taint
		})
		node2 := test.BuildTestNode("n2", 2000, 3000, 10, func(node *v1.Node) {
			node.Labels = map[string]string{
				"zone":        "us-east-1b",
				"node-type":   "compute",
				"environment": "production",
			}
			node.Spec.Taints = taint
		})
		node3 := test.BuildTestNode("n3", 2000, 3000, 10, func(node *v1.Node) {
			node.Labels = map[string]string{
				"zone":        "us-west-1a",
				"node-type":   "storage",
				"environment": "staging",
			}
			node.Spec.Taints = taint
		})
		node4 := test.BuildTestNode("n4", 2000, 3000, 10, func(node *v1.Node) {
			node.Labels = map[string]string{
				"zone":        "us-west-1b",
				"node-type":   "storage",
				"environment": "staging",
			}
			node.Spec.Taints = taint
		})
		return node1, node2, node3, node4
	}

	tests := []struct {
		description              string
		nodeSelector             string
		dryRun                   bool
		expectedEvictedFromNodes []string
	}{
		{
			description:              "Evict from n1, n2",
			nodeSelector:             "environment=production",
			dryRun:                   false,
			expectedEvictedFromNodes: []string{"n1", "n2"},
		},
		{
			description:              "Evict from n1, n2 in dry run mode",
			nodeSelector:             "environment=production",
			dryRun:                   true,
			expectedEvictedFromNodes: []string{"n1", "n2"},
		},
		{
			description:              "Evict from n3, n4",
			nodeSelector:             "environment=staging",
			dryRun:                   false,
			expectedEvictedFromNodes: []string{"n3", "n4"},
		},
		{
			description:              "Evict from n3, n4 in dry run mode",
			nodeSelector:             "environment=staging",
			dryRun:                   true,
			expectedEvictedFromNodes: []string{"n3", "n4"},
		},
		{
			description:              "Evict from n1, n4",
			nodeSelector:             "zone in (us-east-1a, us-west-1b)",
			dryRun:                   false,
			expectedEvictedFromNodes: []string{"n1", "n4"},
		},
		{
			description:              "Evict from n1, n4 in dry run mode",
			nodeSelector:             "zone in (us-east-1a, us-west-1b)",
			dryRun:                   true,
			expectedEvictedFromNodes: []string{"n1", "n4"},
		},
		{
			description:              "Evict from n2, n3",
			nodeSelector:             "zone in (us-east-1b, us-west-1a)",
			dryRun:                   false,
			expectedEvictedFromNodes: []string{"n2", "n3"},
		},
		{
			description:              "Evict from n2, n3 in dry run mode",
			nodeSelector:             "zone in (us-east-1b, us-west-1a)",
			dryRun:                   true,
			expectedEvictedFromNodes: []string{"n2", "n3"},
		},
		{
			description:              "Evict from all nodes",
			nodeSelector:             "",
			dryRun:                   false,
			expectedEvictedFromNodes: []string{"n1", "n2", "n3", "n4"},
		},
		{
			description:              "Evict from all nodes in dry run mode",
			nodeSelector:             "",
			dryRun:                   true,
			expectedEvictedFromNodes: []string{"n1", "n2", "n3", "n4"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			ctx := context.Background()

			// Create nodes with different labels and taints
			node1, node2, node3, node4 := createNodes()

			ownerRef := test.GetReplicaSetOwnerRefList()
			updatePod := func(pod *v1.Pod) {
				pod.ObjectMeta.OwnerReferences = ownerRef
				pod.Status.Phase = v1.PodRunning
			}

			// Create one pod per node
			p1 := test.BuildTestPod("p1", 200, 0, node1.Name, updatePod)
			p2 := test.BuildTestPod("p2", 200, 0, node2.Name, updatePod)
			p3 := test.BuildTestPod("p3", 200, 0, node3.Name, updatePod)
			p4 := test.BuildTestPod("p4", 200, 0, node4.Name, updatePod)

			// Map pod names to their node names for validation
			podToNode := map[string]string{
				"p1": "n1",
				"p2": "n2",
				"p3": "n3",
				"p4": "n4",
			}

			policy := removePodsViolatingNodeTaintsPolicy()
			if tc.nodeSelector != "" {
				policy.NodeSelector = &tc.nodeSelector
			}

			ctxCancel, cancel := context.WithCancel(ctx)
			defer cancel()

			// Create descheduler with DryRun set appropriately
			client := fakeclientset.NewSimpleClientset(node1, node2, node3, node4, p1, p2, p3, p4)
			eventClient := fakeclientset.NewSimpleClientset(node1, node2, node3, node4, p1, p2, p3, p4)

			rs, err := options.NewDeschedulerServer()
			if err != nil {
				t.Fatalf("Unable to initialize server: %v", err)
			}
			rs.Client = client
			rs.EventClient = eventClient
			rs.DefaultFeatureGates = initFeatureGates()
			rs.DryRun = tc.dryRun // Set DryRun before creating descheduler

			sharedInformerFactory := informers.NewSharedInformerFactoryWithOptions(rs.Client, 0, informers.WithTransform(trimManagedFields))
			eventBroadcaster, eventRecorder := utils.GetRecorderAndBroadcaster(ctxCancel, client)
			defer eventBroadcaster.Shutdown()

			deschedulerInstance, err := newDescheduler(ctxCancel, rs, policy, "v1", eventRecorder, sharedInformerFactory, nil)
			if err != nil {
				t.Fatalf("Unable to create a descheduler instance: %v", err)
			}

			sharedInformerFactory.Start(ctxCancel.Done())
			sharedInformerFactory.WaitForCacheSync(ctxCancel.Done())
			if deschedulerInstance.fakeSharedInformerFactory != nil {
				deschedulerInstance.fakeSharedInformerFactory.Start(ctxCancel.Done())
				deschedulerInstance.fakeSharedInformerFactory.WaitForCacheSync(ctxCancel.Done())
			}

			// Verify all pods are created initially
			pods, err := client.CoreV1().Pods(p1.Namespace).List(ctx, metav1.ListOptions{})
			if err != nil {
				t.Fatalf("Unable to list pods: %v", err)
			}
			if len(pods.Items) != 4 {
				t.Errorf("Expected 4 pods initially, got %d", len(pods.Items))
			}

			var evictedPods []string
			if !tc.dryRun {
				client.PrependReactor("create", "pods", podEvictionReactionTestingFnc(&evictedPods, nil, nil))
			} else {
				deschedulerInstance.podEvictionReactionFnc = func(*fakeclientset.Clientset, *evictedPodsCache) func(action core.Action) (bool, runtime.Object, error) {
					return podEvictionReactionTestingFnc(&evictedPods, nil, nil)
				}
				deschedulerInstance.fakeClient.PrependReactor("create", "pods", deschedulerInstance.podEvictionReactionFnc(deschedulerInstance.fakeClient, deschedulerInstance.evictedPodsCache))
			}

			// Run descheduler
			if err := deschedulerInstance.runDeschedulerLoop(ctx); err != nil {
				t.Fatalf("Unable to run descheduler loop: %v", err)
			}

			// Collect which nodes had pods evicted from them
			nodesWithEvictedPods := make(map[string]bool)
			for _, podName := range evictedPods {
				if nodeName, ok := podToNode[podName]; ok {
					nodesWithEvictedPods[nodeName] = true
				}
			}

			// Verify the correct number of nodes had pods evicted
			if len(nodesWithEvictedPods) != len(tc.expectedEvictedFromNodes) {
				t.Errorf("Expected pods to be evicted from %d nodes, got %d nodes: %v", len(tc.expectedEvictedFromNodes), len(nodesWithEvictedPods), nodesWithEvictedPods)
			}

			// Verify pods were evicted from the correct nodes
			for _, nodeName := range tc.expectedEvictedFromNodes {
				if !nodesWithEvictedPods[nodeName] {
					t.Errorf("Expected pod to be evicted from node %s, but it was not", nodeName)
				}
			}

			// Verify no unexpected nodes had pods evicted
			for nodeName := range nodesWithEvictedPods {
				found := false
				for _, expectedNode := range tc.expectedEvictedFromNodes {
					if nodeName == expectedNode {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("Unexpected eviction from node %s", nodeName)
				}
			}

			t.Logf("Successfully evicted pods from nodes: %v", tc.expectedEvictedFromNodes)
		})
	}
}

func TestLoadAwareDescheduling(t *testing.T) {
	initPluginRegistry()

	ownerRef1 := test.GetReplicaSetOwnerRefList()
	updatePod := func(pod *v1.Pod) {
		pod.ObjectMeta.OwnerReferences = ownerRef1
	}

	ctx := context.Background()
	node1 := test.BuildTestNode("n1", 2000, 3000, 10, taintNodeNoSchedule)
	node2 := test.BuildTestNode("n2", 2000, 3000, 10, nil)

	p1 := test.BuildTestPod("p1", 300, 0, node1.Name, updatePod)
	p2 := test.BuildTestPod("p2", 300, 0, node1.Name, updatePod)
	p3 := test.BuildTestPod("p3", 300, 0, node1.Name, updatePod)
	p4 := test.BuildTestPod("p4", 300, 0, node1.Name, updatePod)
	p5 := test.BuildTestPod("p5", 300, 0, node1.Name, updatePod)

	nodemetricses := []*v1beta1.NodeMetrics{
		test.BuildNodeMetrics("n1", 2400, 3000),
		test.BuildNodeMetrics("n2", 400, 0),
	}

	podmetricses := []*v1beta1.PodMetrics{
		test.BuildPodMetrics("p1", 400, 0),
		test.BuildPodMetrics("p2", 400, 0),
		test.BuildPodMetrics("p3", 400, 0),
		test.BuildPodMetrics("p4", 400, 0),
		test.BuildPodMetrics("p5", 400, 0),
	}

	metricsClientset := fakemetricsclient.NewSimpleClientset()
	for _, nodemetrics := range nodemetricses {
		metricsClientset.Tracker().Create(nodesgvr, nodemetrics, "")
	}
	for _, podmetrics := range podmetricses {
		metricsClientset.Tracker().Create(podsgvr, podmetrics, podmetrics.Namespace)
	}

	policy := lowNodeUtilizationPolicy(
		api.ResourceThresholds{
			v1.ResourceCPU:  30,
			v1.ResourcePods: 30,
		},
		api.ResourceThresholds{
			v1.ResourceCPU:  50,
			v1.ResourcePods: 50,
		},
		true, // enabled metrics utilization
	)
	policy.MetricsProviders = []api.MetricsProvider{{Source: api.KubernetesMetrics}}

	ctxCancel, cancel := context.WithCancel(ctx)
	_, descheduler, _ := initDescheduler(
		t,
		ctxCancel,
		initFeatureGates(),
		policy,
		metricsClientset,
		node1, node2, p1, p2, p3, p4, p5)
	defer cancel()

	// This needs to be run since the metrics collector is started
	// after newDescheduler in RunDeschedulerStrategies.
	descheduler.metricsCollector.Collect(ctx)

	err := descheduler.runDeschedulerLoop(ctx)
	if err != nil {
		t.Fatalf("Unable to run a descheduling loop: %v", err)
	}
	totalEs := descheduler.podEvictor.TotalEvicted()
	if totalEs != 2 {
		t.Fatalf("Expected %v evictions in total, got %v instead", 2, totalEs)
	}
	t.Logf("Total evictions: %v", totalEs)
}

func TestInformerResourcesRestoreEvictedPods(t *testing.T) {
	testCases := []struct {
		name               string
		setupPods          func(node *v1.Node) []*v1.Pod
		setupCache         func(pods []*v1.Pod) *evictedPodsCache
		podToDelete        string
		expectRestored     bool
		expectedPodsInFake int
	}{
		{
			name: "successfully restore evicted pod",
			setupPods: func(node *v1.Node) []*v1.Pod {
				ownerRef1 := test.GetReplicaSetOwnerRefList()
				updatePod := func(pod *v1.Pod) {
					pod.Namespace = "default"
					pod.ObjectMeta.OwnerReferences = ownerRef1
				}
				p1 := test.BuildTestPod("p1", 100, 0, node.Name, updatePod)
				p2 := test.BuildTestPod("p2", 100, 0, node.Name, updatePod)
				return []*v1.Pod{p1, p2}
			},
			setupCache: func(pods []*v1.Pod) *evictedPodsCache {
				cache := newEvictedPodsCache()
				cache.add(pods[0]) // Add p1
				return cache
			},
			podToDelete:        "p1",
			expectRestored:     true,
			expectedPodsInFake: 2, // p1 restored, p2 still there
		},
		{
			name: "skip restoration due to UID mismatch",
			setupPods: func(node *v1.Node) []*v1.Pod {
				ownerRef1 := test.GetReplicaSetOwnerRefList()
				updatePod := func(pod *v1.Pod) {
					pod.Namespace = "default"
					pod.ObjectMeta.OwnerReferences = ownerRef1
				}
				p1 := test.BuildTestPod("p1", 100, 0, node.Name, updatePod)
				return []*v1.Pod{p1}
			},
			setupCache: func(pods []*v1.Pod) *evictedPodsCache {
				cache := newEvictedPodsCache()
				// Add pod with wrong UID
				wrongUID := "wrong-uid-12345"
				cache.pods[wrongUID] = &evictedPodInfo{
					Namespace: pods[0].Namespace,
					Name:      pods[0].Name,
					UID:       wrongUID,
				}
				return cache
			},
			podToDelete:        "p1",
			expectRestored:     false,
			expectedPodsInFake: 0, // p1 not restored due to UID mismatch
		},
		{
			name: "skip restoration when pod not in real client",
			setupPods: func(node *v1.Node) []*v1.Pod {
				return []*v1.Pod{} // No pods in real client
			},
			setupCache: func(pods []*v1.Pod) *evictedPodsCache {
				cache := newEvictedPodsCache()
				// Add pod that doesn't exist in real client
				cache.pods["missing-pod-uid"] = &evictedPodInfo{
					Namespace: "default",
					Name:      "missing-pod",
					UID:       "missing-pod-uid",
				}
				return cache
			},
			podToDelete:        "", // No pod to delete
			expectRestored:     false,
			expectedPodsInFake: 0, // No pods
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()

			// Create test node
			node1 := test.BuildTestNode("n1", 2000, 3000, 10, nil)

			// Setup pods
			pods := tc.setupPods(node1)

			// Create real client with the pods
			objects := []runtime.Object{node1}
			for _, pod := range pods {
				objects = append(objects, pod)
			}
			realClient := fakeclientset.NewSimpleClientset(objects...)
			realInformerFactory := informers.NewSharedInformerFactory(realClient, 0)

			// Create fake client
			fakeClient := fakeclientset.NewSimpleClientset()
			fakeInformerFactory := informers.NewSharedInformerFactory(fakeClient, 0)

			// Create informerResources
			ir := newInformerResources(realInformerFactory, fakeClient, fakeInformerFactory)

			// Register pods resource
			err := ir.uses(v1.SchemeGroupVersion.WithResource("pods"))
			if err != nil {
				t.Fatalf("Failed to register pods resource: %v", err)
			}

			// Start informers - this will trigger event handlers which sync the initial state
			realInformerFactory.Start(ctx.Done())
			realInformerFactory.WaitForCacheSync(ctx.Done())

			fakeInformerFactory.Start(ctx.Done())
			fakeInformerFactory.WaitForCacheSync(ctx.Done())

			// Setup evicted cache
			evictedCache := tc.setupCache(pods)

			// Delete pod from fake client if specified
			if tc.podToDelete != "" {
				err = fakeClient.CoreV1().Pods("default").Delete(ctx, tc.podToDelete, metav1.DeleteOptions{})
				if err != nil {
					t.Fatalf("Failed to delete pod from fake client: %v", err)
				}

				// Verify pod is deleted
				_, err = fakeClient.CoreV1().Pods("default").Get(ctx, tc.podToDelete, metav1.GetOptions{})
				if err == nil {
					t.Fatalf("Expected pod %s to be deleted from fake client", tc.podToDelete)
				}
				if !apierrors.IsNotFound(err) {
					t.Fatalf("Expected NotFound error, got: %v", err)
				}
			}

			// Restore evicted pods
			err = ir.restoreEvictedPods(evictedCache)
			if err != nil {
				t.Fatalf("Failed to restore evicted pods: %v", err)
			}
			evictedCache.clear()

			// Verify restoration result
			if tc.expectRestored {
				restoredPod, err := fakeClient.CoreV1().Pods("default").Get(ctx, tc.podToDelete, metav1.GetOptions{})
				if err != nil {
					t.Fatalf("Expected pod %s to be restored to fake client, got error: %v", tc.podToDelete, err)
				}
				if restoredPod.Name != tc.podToDelete {
					t.Fatalf("Expected restored pod name %s, got %s", tc.podToDelete, restoredPod.Name)
				}
				t.Logf("Successfully restored pod %s with UID %s", restoredPod.Name, restoredPod.UID)
			} else if tc.podToDelete != "" {
				_, err := fakeClient.CoreV1().Pods("default").Get(ctx, tc.podToDelete, metav1.GetOptions{})
				if err == nil {
					t.Fatalf("Expected pod %s to NOT be restored", tc.podToDelete)
				}
				if !apierrors.IsNotFound(err) {
					t.Fatalf("Expected NotFound error, got: %v", err)
				}
				t.Logf("Successfully skipped restoration for pod %s", tc.podToDelete)
			}

			// Verify expected number of pods in fake client
			podsList, err := fakeClient.CoreV1().Pods("default").List(ctx, metav1.ListOptions{})
			if err != nil {
				t.Fatalf("Failed to list pods from fake client: %v", err)
			}
			if len(podsList.Items) != tc.expectedPodsInFake {
				t.Fatalf("Expected %d pods in fake client, got %d", tc.expectedPodsInFake, len(podsList.Items))
			}

			// Verify cache is cleared
			cachedPods := evictedCache.list()
			if len(cachedPods) != 0 {
				t.Fatalf("Expected evicted cache to be empty, got %d pods", len(cachedPods))
			}
		})
	}
}
