package descheduler

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	policy "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/runtime"
	apiversion "k8s.io/apimachinery/pkg/version"
	fakediscovery "k8s.io/client-go/discovery/fake"
	"k8s.io/client-go/informers"
	fakeclientset "k8s.io/client-go/kubernetes/fake"
	core "k8s.io/client-go/testing"
	"k8s.io/klog/v2"
	"k8s.io/utils/pointer"
	"sigs.k8s.io/descheduler/cmd/descheduler/app/options"
	"sigs.k8s.io/descheduler/pkg/api"
	"sigs.k8s.io/descheduler/pkg/api/v1alpha1"
	"sigs.k8s.io/descheduler/pkg/descheduler/evictions"
	"sigs.k8s.io/descheduler/pkg/framework/pluginregistry"
	"sigs.k8s.io/descheduler/pkg/framework/plugins/defaultevictor"
	"sigs.k8s.io/descheduler/pkg/framework/plugins/removeduplicates"
	"sigs.k8s.io/descheduler/pkg/framework/plugins/removepodsviolatingnodetaints"
	"sigs.k8s.io/descheduler/pkg/utils"
	deschedulerversion "sigs.k8s.io/descheduler/pkg/version"
	"sigs.k8s.io/descheduler/test"
)

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

// scope contains information about an ongoing conversion.
type scope struct {
	converter *conversion.Converter
	meta      *conversion.Meta
}

// Convert continues a conversion.
func (s scope) Convert(src, dest interface{}) error {
	return s.converter.Convert(src, dest, s.meta)
}

// Meta returns the meta object that was originally passed to Convert.
func (s scope) Meta() *conversion.Meta {
	return s.meta
}

func TestTaintsUpdated(t *testing.T) {
	initPluginRegistry()

	ctx := context.Background()
	n1 := test.BuildTestNode("n1", 2000, 3000, 10, nil)
	n2 := test.BuildTestNode("n2", 2000, 3000, 10, nil)

	p1 := test.BuildTestPod(fmt.Sprintf("pod_1_%s", n1.Name), 200, 0, n1.Name, nil)
	p1.ObjectMeta.OwnerReferences = test.GetReplicaSetOwnerRefList()

	internalDeschedulerPolicy := removePodsViolatingNodeTaintsPolicy()

	client := fakeclientset.NewSimpleClientset(n1, n2, p1)
	eventClient := fakeclientset.NewSimpleClientset(n1, n2, p1)

	rs, err := options.NewDeschedulerServer()
	if err != nil {
		t.Fatalf("Unable to initialize server: %v", err)
	}
	rs.Client = client
	rs.EventClient = eventClient

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
	client.PrependReactor("create", "pods", podEvictionReactionTestingFnc(&evictedPods, true))

	if err := RunDeschedulerStrategies(ctx, rs, internalDeschedulerPolicy, "v1"); err != nil {
		t.Fatalf("Unable to run descheduler strategies: %v", err)
	}

	if len(evictedPods) != 1 {
		t.Fatalf("Unable to evict pod, node taint did not get propagated to descheduler strategies %v\n", err)
	}
}

func TestDuplicate(t *testing.T) {
	pluginregistry.PluginRegistry = pluginregistry.NewRegistry()
	pluginregistry.Register(removeduplicates.PluginName, removeduplicates.New, &removeduplicates.RemoveDuplicates{}, &removeduplicates.RemoveDuplicatesArgs{}, removeduplicates.ValidateRemoveDuplicatesArgs, removeduplicates.SetDefaults_RemoveDuplicatesArgs, pluginregistry.PluginRegistry)
	pluginregistry.Register(defaultevictor.PluginName, defaultevictor.New, &defaultevictor.DefaultEvictor{}, &defaultevictor.DefaultEvictorArgs{}, defaultevictor.ValidateDefaultEvictorArgs, defaultevictor.SetDefaults_DefaultEvictorArgs, pluginregistry.PluginRegistry)

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
	dp := &v1alpha1.DeschedulerPolicy{
		Strategies: v1alpha1.StrategyList{
			"RemoveDuplicates": v1alpha1.DeschedulerStrategy{
				Enabled: true,
			},
		},
	}

	rs, err := options.NewDeschedulerServer()
	if err != nil {
		t.Fatalf("Unable to initialize server: %v", err)
	}
	rs.Client = client
	rs.EventClient = eventClient

	pods, err := client.CoreV1().Pods(p1.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Errorf("Unable to list pods: %v", err)
	}

	if len(pods.Items) != 3 {
		t.Errorf("Pods number should be 3 before evict")
	}

	var evictedPods []string
	client.PrependReactor("create", "pods", podEvictionReactionTestingFnc(&evictedPods, true))

	internalDeschedulerPolicy := &api.DeschedulerPolicy{}
	scope := scope{}
	err = v1alpha1.V1alpha1ToInternal(dp, pluginregistry.PluginRegistry, internalDeschedulerPolicy, scope)
	if err != nil {
		t.Fatalf("Unable to convert v1alpha1 to v1alpha2: %v", err)
	}
	if err := RunDeschedulerStrategies(ctx, rs, internalDeschedulerPolicy, "v1"); err != nil {
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
		deschedulerVersion string
		serverVersion      string
		expectError        bool
	}
	testCases := []testCase{
		{
			name:               "no error when descheduler minor equals to server minor",
			deschedulerVersion: "v0.26",
			serverVersion:      "v1.26.1",
			expectError:        false,
		},
		{
			name:               "no error when descheduler minor is 3 behind server minor",
			deschedulerVersion: "0.23",
			serverVersion:      "v1.26.1",
			expectError:        false,
		},
		{
			name:               "no error when descheduler minor is 3 ahead of server minor",
			deschedulerVersion: "v0.26",
			serverVersion:      "v1.26.1",
			expectError:        false,
		},
		{
			name:               "error when descheduler minor is 4 behind server minor",
			deschedulerVersion: "v0.22",
			serverVersion:      "v1.26.1",
			expectError:        true,
		},
		{
			name:               "error when descheduler minor is 4 ahead of server minor",
			deschedulerVersion: "v0.27",
			serverVersion:      "v1.23.1",
			expectError:        true,
		},
		{
			name:               "no error when using managed provider version",
			deschedulerVersion: "v0.25",
			serverVersion:      "v1.25.12-eks-2d98532",
			expectError:        false,
		},
	}
	client := fakeclientset.NewSimpleClientset()
	fakeDiscovery, _ := client.Discovery().(*fakediscovery.FakeDiscovery)
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			fakeDiscovery.FakedServerVersion = &apiversion.Info{GitVersion: tc.serverVersion}
			deschedulerVersion := deschedulerversion.Info{GitVersion: tc.deschedulerVersion}
			err := validateVersionCompatibility(fakeDiscovery, deschedulerVersion)

			hasError := err != nil
			if tc.expectError != hasError {
				t.Error("unexpected version compatibility behavior")
			}
		})
	}
}

func podEvictionReactionTestingFnc(evictedPods *[]string, evictionErr bool) func(action core.Action) (bool, runtime.Object, error) {
	return func(action core.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() == "eviction" {
			createAct, matched := action.(core.CreateActionImpl)
			if !matched {
				return false, nil, fmt.Errorf("unable to convert action to core.CreateActionImpl")
			}
			if eviction, matched := createAct.Object.(*policy.Eviction); matched {
				*evictedPods = append(*evictedPods, eviction.GetName())
				if evictionErr {
					return true, nil, fmt.Errorf("podEvictionReactionTestingFnc error")
				}
				return true, nil, nil
			}
		}
		return false, nil, nil // fallback to the default reactor
	}
}

func podEvictionRequestReactionTestingFnc(evictionsInBackground map[string]bool, evictedPods []string) func(action core.Action) (bool, runtime.Object, error) {
	return func(action core.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() == "eviction" {
			createAct, matched := action.(core.CreateActionImpl)
			if !matched {
				return false, nil, fmt.Errorf("unable to convert action to core.CreateActionImpl")
			}
			if eviction, matched := createAct.Object.(*policy.Eviction); matched {
				// eviction in background
				if _, exists := evictionsInBackground[eviction.GetName()]; exists {
					return true, nil, &apierrors.StatusError{metav1.Status{
						Status:  metav1.StatusFailure,
						Code:    http.StatusTooManyRequests,
						Reason:  metav1.StatusReasonTooManyRequests,
						Message: fmt.Sprintf("admission webhook \"virt-launcher-eviction-interceptor.kubevirt.io\" denied the request: Eviction triggered evacuation of VMI \"dev/%v\"", eviction.GetName()),
					}}
				}
				evictedPods = append(evictedPods, eviction.GetName())
				return true, nil, nil
			}
		}
		return false, nil, nil // fallback to the default reactor
	}
}

func initPluginRegistry() {
	pluginregistry.PluginRegistry = pluginregistry.NewRegistry()
	pluginregistry.Register(removeduplicates.PluginName, removeduplicates.New, &removeduplicates.RemoveDuplicates{}, &removeduplicates.RemoveDuplicatesArgs{}, removeduplicates.ValidateRemoveDuplicatesArgs, removeduplicates.SetDefaults_RemoveDuplicatesArgs, pluginregistry.PluginRegistry)
	pluginregistry.Register(defaultevictor.PluginName, defaultevictor.New, &defaultevictor.DefaultEvictor{}, &defaultevictor.DefaultEvictorArgs{}, defaultevictor.ValidateDefaultEvictorArgs, defaultevictor.SetDefaults_DefaultEvictorArgs, pluginregistry.PluginRegistry)
	pluginregistry.Register(removepodsviolatingnodetaints.PluginName, removepodsviolatingnodetaints.New, &removepodsviolatingnodetaints.RemovePodsViolatingNodeTaints{}, &removepodsviolatingnodetaints.RemovePodsViolatingNodeTaintsArgs{}, removepodsviolatingnodetaints.ValidateRemovePodsViolatingNodeTaintsArgs, removepodsviolatingnodetaints.SetDefaults_RemovePodsViolatingNodeTaintsArgs, pluginregistry.PluginRegistry)
}

func initDescheduler(t *testing.T, ctx context.Context, internalDeschedulerPolicy *api.DeschedulerPolicy, assumedRequestTimeout uint, objects ...runtime.Object) (*options.DeschedulerServer, *descheduler, *fakeclientset.Clientset, func()) {
	client := fakeclientset.NewSimpleClientset(objects...)
	eventClient := fakeclientset.NewSimpleClientset(objects...)

	rs, err := options.NewDeschedulerServer()
	if err != nil {
		t.Fatalf("Unable to initialize server: %v", err)
	}
	rs.Client = client
	rs.EventClient = eventClient

	sharedInformerFactory := informers.NewSharedInformerFactoryWithOptions(rs.Client, 0, informers.WithTransform(trimManagedFields))
	eventBroadcaster, eventRecorder := utils.GetRecorderAndBroadcaster(ctx, client)

	descheduler, err := newDescheduler(rs, internalDeschedulerPolicy, "v1", eventRecorder, sharedInformerFactory, assumedRequestTimeout)
	if err != nil {
		eventBroadcaster.Shutdown()
		t.Fatalf("Unable to create a descheduler instance: %v", err)
	}

	ctx, cancel := context.WithCancel(ctx)

	sharedInformerFactory.Start(ctx.Done())
	sharedInformerFactory.WaitForCacheSync(ctx.Done())

	return rs, descheduler, client, func() {
		cancel()
		eventBroadcaster.Shutdown()
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
	nodes := []*v1.Node{node1, node2}

	ownerRef1 := test.GetReplicaSetOwnerRefList()
	updatePod := func(pod *v1.Pod) {
		pod.Namespace = "dev"
		pod.ObjectMeta.OwnerReferences = ownerRef1
	}

	p1 := test.BuildTestPod("p1", 100, 0, node1.Name, updatePod)
	p2 := test.BuildTestPod("p2", 100, 0, node1.Name, updatePod)

	internalDeschedulerPolicy := removePodsViolatingNodeTaintsPolicy()
	rs, descheduler, client, cancel := initDescheduler(t, ctx, internalDeschedulerPolicy, evictions.AssumedEvictionRequestTimeoutSeconds, node1, node2, p1, p2)
	defer cancel()

	var evictedPods []string
	client.PrependReactor("create", "pods", podEvictionReactionTestingFnc(&evictedPods, false))

	var fakeEvictedPods []string
	descheduler.podEvictionReactionFnc = func(*fakeclientset.Clientset) func(action core.Action) (bool, runtime.Object, error) {
		return podEvictionReactionTestingFnc(&fakeEvictedPods, false)
	}

	klog.Infof("2 pod eviction expected per a descheduling cycle, 2 real evictions in total")
	if err := descheduler.runDeschedulerLoop(ctx, nodes); err != nil {
		t.Fatalf("Unable to run a descheduling loop: %v", err)
	}
	if descheduler.podEvictor.TotalEvicted() != 2 || len(evictedPods) != 2 || len(fakeEvictedPods) != 0 {
		t.Fatalf("Expected (2,2,0) pods evicted, got (%v, %v, %v) instead", descheduler.podEvictor.TotalEvicted(), len(evictedPods), len(fakeEvictedPods))
	}

	klog.Infof("2 pod eviction expected per a descheduling cycle, 4 real evictions in total")
	if err := descheduler.runDeschedulerLoop(ctx, nodes); err != nil {
		t.Fatalf("Unable to run a descheduling loop: %v", err)
	}
	if descheduler.podEvictor.TotalEvicted() != 2 || len(evictedPods) != 4 || len(fakeEvictedPods) != 0 {
		t.Fatalf("Expected (2,4,0) pods evicted, got (%v, %v, %v) instead", descheduler.podEvictor.TotalEvicted(), len(evictedPods), len(fakeEvictedPods))
	}

	// check the fake client syncing and the right pods evicted
	klog.Infof("Enabling the dry run mode")
	rs.DryRun = true
	evictedPods = []string{}

	klog.Infof("2 pod eviction expected per a descheduling cycle, 2 fake evictions in total")
	if err := descheduler.runDeschedulerLoop(ctx, nodes); err != nil {
		t.Fatalf("Unable to run a descheduling loop: %v", err)
	}
	if descheduler.podEvictor.TotalEvicted() != 2 || len(evictedPods) != 0 || len(fakeEvictedPods) != 2 {
		t.Fatalf("Expected (2,0,2) pods evicted, got (%v, %v, %v) instead", descheduler.podEvictor.TotalEvicted(), len(evictedPods), len(fakeEvictedPods))
	}

	klog.Infof("2 pod eviction expected per a descheduling cycle, 4 fake evictions in total")
	if err := descheduler.runDeschedulerLoop(ctx, nodes); err != nil {
		t.Fatalf("Unable to run a descheduling loop: %v", err)
	}
	if descheduler.podEvictor.TotalEvicted() != 2 || len(evictedPods) != 0 || len(fakeEvictedPods) != 4 {
		t.Fatalf("Expected (2,0,4) pods evicted, got (%v, %v, %v) instead", descheduler.podEvictor.TotalEvicted(), len(evictedPods), len(fakeEvictedPods))
	}
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
	err := descheduler.runDeschedulerLoop(ctx, nodes)
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

	_, descheduler, client, cancel := initDescheduler(t, ctx, internalDeschedulerPolicy, evictions.AssumedEvictionRequestTimeoutSeconds, node1, node2, p1, p2, p3, p4)
	defer cancel()

	var fakeEvictedPods []string
	descheduler.podEvictionReactionFnc = func(*fakeclientset.Clientset) func(action core.Action) (bool, runtime.Object, error) {
		return podEvictionReactionTestingFnc(&fakeEvictedPods, true)
	}

	var evictedPods []string
	client.PrependReactor("create", "pods", podEvictionRequestReactionTestingFnc(map[string]bool{"p1": true, "p2": true}, evictedPods))

	klog.Infof("2 evictions in backgroud expected, 2 normal evictions")
	runDeschedulingCycleAndCheckTotals(t, ctx, nodes, descheduler, 2, 2)

	klog.Infof("Repeat the same as previously to confirm no more evictions in background are requested")
	// No evicted pod is actually deleted on purpose so the test can run the descheduling cycle repeteadly
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

	klog.Infof("Scenario: Eviction in background completed")
	if err := client.CoreV1().Pods(p2.Namespace).Delete(context.TODO(), p2.Name, metav1.DeleteOptions{}); err != nil {
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
				policy.MaxNoOfPodsToEvictPerNode = pointer.Uint(4)
				return policy
			}(),
			limit: uint(4),
		},
		{
			description: "limits per namespace",
			policy: func() *api.DeschedulerPolicy {
				policy := removePodsViolatingNodeTaintsPolicy()
				policy.MaxNoOfPodsToEvictPerNamespace = pointer.Uint(4)
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
			nodes := []*v1.Node{node1, node2}

			_, descheduler, client, cancel := initDescheduler(t, ctx, tc.policy, evictions.AssumedEvictionRequestTimeoutSeconds, node1, node2) //, p1, p2, p3, p4, p5)
			defer cancel()

			var fakeEvictedPods []string
			descheduler.podEvictionReactionFnc = func(*fakeclientset.Clientset) func(action core.Action) (bool, runtime.Object, error) {
				return podEvictionReactionTestingFnc(&fakeEvictedPods, true)
			}

			var evictedPods []string
			client.PrependReactor("create", "pods", podEvictionRequestReactionTestingFnc(map[string]bool{"p1": true, "p2": true}, evictedPods))

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
				for j := 0; j < 5; j++ {
					if _, err := client.CoreV1().Pods(pods[j].Namespace).Create(context.TODO(), pods[j], metav1.CreateOptions{}); err != nil {
						t.Fatalf("unable to create a pod: %v", err)
					}
				}
				time.Sleep(100 * time.Millisecond)

				klog.Infof("2 evictions in backgroud expected, 2 normal evictions")
				err := descheduler.runDeschedulerLoop(ctx, nodes)
				if err != nil {
					t.Fatalf("Unable to run a descheduling loop: %v", err)
				}
				totalERs := descheduler.podEvictor.TotalEvictionRequests()
				totalEs := descheduler.podEvictor.TotalEvicted()
				if totalERs+totalEs > tc.limit {
					t.Fatalf("Expected %v evictions and eviction requests in total, got %v instead", tc.limit, totalERs+totalEs)
				}
				t.Logf("Total evictions and eviction requests: %v (er=%v, e=%v)", totalERs+totalEs, totalERs, totalEs)
				for j := 0; j < 5; j++ {
					if err := client.CoreV1().Pods(pods[j].Namespace).Delete(context.TODO(), pods[j].Name, metav1.DeleteOptions{}); err != nil {
						t.Fatalf("unable to delete a pod: %v", err)
					}
				}
			}
		})
	}
}

func TestEvictionRequestsCacheCleanup(t *testing.T) {
	initPluginRegistry()

	ctx := context.Background()
	node1 := test.BuildTestNode("n1", 2000, 3000, 10, taintNodeNoSchedule)
	node2 := test.BuildTestNode("n2", 2000, 3000, 10, nil)
	nodes := []*v1.Node{node1, node2}

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

	p1 := test.BuildTestPod("p1", 100, 0, node1.Name, updatePodWithEvictionInBackground)
	p2 := test.BuildTestPod("p2", 100, 0, node1.Name, updatePodWithEvictionInBackground)
	p3 := test.BuildTestPod("p3", 100, 0, node1.Name, updatePod)
	p4 := test.BuildTestPod("p4", 100, 0, node1.Name, updatePod)

	internalDeschedulerPolicy := removePodsViolatingNodeTaintsPolicy()

	_, descheduler, client, cancel := initDescheduler(t, ctx, internalDeschedulerPolicy, 1, node1, node2, p1, p2, p3, p4)
	defer cancel()

	var fakeEvictedPods []string
	descheduler.podEvictionReactionFnc = func(*fakeclientset.Clientset) func(action core.Action) (bool, runtime.Object, error) {
		return podEvictionReactionTestingFnc(&fakeEvictedPods, true)
	}

	var evictedPods []string
	client.PrependReactor("create", "pods", podEvictionRequestReactionTestingFnc(map[string]bool{"p1": true, "p2": true}, evictedPods))

	klog.Infof("2 evictions in backgroud expected, 2 normal evictions")
	runDeschedulingCycleAndCheckTotals(t, ctx, nodes, descheduler, 2, 2)

	klog.Infof("2 evictions in backgroud assumed. Wait for few seconds and check the assumed requests timed out")
	time.Sleep(3 * time.Second) // it usually takes something over 2 seconds
	klog.Infof("Checking the assumed requests timed out and were deleted")
	if totalERs := descheduler.podEvictor.TotalEvictionRequests(); totalERs > 0 {
		t.Fatalf("Expected 0 eviction requests, got %v instead", totalERs)
	}

}
