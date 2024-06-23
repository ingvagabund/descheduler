package e2e

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/spf13/pflag"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	componentbaseconfig "k8s.io/component-base/config"
	utilptr "k8s.io/utils/ptr"
	kvcorev1 "kubevirt.io/api/core/v1"
	"kubevirt.io/client-go/kubecli"
	"sigs.k8s.io/descheduler/cmd/descheduler/app/options"
	"sigs.k8s.io/descheduler/pkg/api"
	"sigs.k8s.io/descheduler/pkg/descheduler"
	"sigs.k8s.io/descheduler/pkg/descheduler/client"
	"sigs.k8s.io/descheduler/pkg/framework/pluginregistry"
	"sigs.k8s.io/descheduler/pkg/framework/plugins/defaultevictor"
	"sigs.k8s.io/descheduler/pkg/framework/plugins/podlifetime"
)

const (
	vmiCount = 3
)

func virtualMachineInstance(idx int) *kvcorev1.VirtualMachineInstance {
	return &kvcorev1.VirtualMachineInstance{
		ObjectMeta: metav1.ObjectMeta{
			Name: fmt.Sprintf("vmi-masquerade-%v", idx),
			Annotations: map[string]string{
				"descheduler.alpha.kubernetes.io/request-evict-only": "",
			},
		},
		Spec: kvcorev1.VirtualMachineInstanceSpec{
			EvictionStrategy: utilptr.To[kvcorev1.EvictionStrategy](kvcorev1.EvictionStrategyLiveMigrate),
			Domain: kvcorev1.DomainSpec{
				Devices: kvcorev1.Devices{
					Disks: []kvcorev1.Disk{
						{
							Name: "containerdisk",
							DiskDevice: kvcorev1.DiskDevice{
								Disk: &kvcorev1.DiskTarget{
									Bus: kvcorev1.DiskBusVirtio,
								},
							},
						},
						{
							Name: "cloudinitdisk",
							DiskDevice: kvcorev1.DiskDevice{
								Disk: &kvcorev1.DiskTarget{
									Bus: kvcorev1.DiskBusVirtio,
								},
							},
						},
					},
					Interfaces: []kvcorev1.Interface{
						{
							InterfaceBindingMethod: kvcorev1.InterfaceBindingMethod{
								Masquerade: &kvcorev1.InterfaceMasquerade{},
							},
							Name: "testmasquerade",
							Ports: []kvcorev1.Port{
								{
									Name:     "http",
									Port:     80,
									Protocol: "TCP",
								},
							},
						},
					},
					Rng: &kvcorev1.Rng{},
				},
				Resources: kvcorev1.ResourceRequirements{
					Requests: v1.ResourceList{
						v1.ResourceMemory: resource.MustParse("1024M"),
					},
				},
			},
			Networks: []kvcorev1.Network{
				{
					Name: "testmasquerade",
					NetworkSource: kvcorev1.NetworkSource{
						Pod: &kvcorev1.PodNetwork{},
					},
				},
			},
			TerminationGracePeriodSeconds: utilptr.To[int64](0),
			Volumes: []kvcorev1.Volume{
				{
					Name: "containerdisk",
					VolumeSource: kvcorev1.VolumeSource{
						ContainerDisk: &kvcorev1.ContainerDiskSource{
							Image: "quay.io/kubevirt/fedora-with-test-tooling-container-disk:20240710_1265d1090",
						},
					},
				},
				{
					Name: "cloudinitdisk",
					VolumeSource: kvcorev1.VolumeSource{
						CloudInitNoCloud: &kvcorev1.CloudInitNoCloudSource{
							UserData: `#cloud-config
password: fedora
chpasswd: { expire: False }
packages:
  - nginx
runcmd:
  - [ "systemctl", "enable", "--now", "nginx" ]`,
							NetworkData: `version: 2
ethernets:
  eth0:
    addresses: [ fd10:0:2::2/120 ]
    dhcp4: true
    gateway6: fd10:0:2::1`,
						},
					},
				},
			},
		},
	}
}

func waitForKubevirtReady(t *testing.T, ctx context.Context, kvClient kubecli.KubevirtClient) {
	obj, err := kvClient.KubeVirt("kubevirt").Get(ctx, "kubevirt", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Unable to get kubevirt/kubevirt: %v", err)
	}
	available := false
	for _, condition := range obj.Status.Conditions {
		if condition.Type == kvcorev1.KubeVirtConditionAvailable {
			if condition.Status == corev1.ConditionTrue {
				available = true
			}
		}
	}
	if !available {
		t.Fatalf("Kubevirt is not available")
	}
	t.Logf("Kubevirt is available")
}

func allVMIsHaveRunningPods(t *testing.T, ctx context.Context, kubeClient clientset.Interface, kvClient kubecli.KubevirtClient) (bool, error) {
	t.Logf("Checking all vmi active pods are running")
	uidMap := make(map[types.UID]*corev1.Pod)
	podList, err := kubeClient.CoreV1().Pods("default").List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Logf("Unable to list pods: %v", err)
		return false, err
	}

	for _, item := range podList.Items {
		pod := item
		fmt.Printf("item: %#v\n", item.UID)
		uidMap[item.UID] = &pod
	}

	vmiList, err := kvClient.VirtualMachineInstance("default").List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Logf("Unable to list VMIs: %v", err)
		return false, err
	}
	if len(vmiList.Items) != vmiCount {
		t.Logf("Expected %v VMIs, got %v instead", vmiCount, len(vmiList.Items))
		return false, nil
	}

	for _, item := range vmiList.Items {
		atLeastOneVmiIsRunning := false
		for activePod := range item.Status.ActivePods {
			if _, exists := uidMap[activePod]; !exists {
				t.Logf("Active pod %v not found", activePod)
				return false, nil
			}
			t.Logf("Checking whether active pod %v (uid=%v) is running", uidMap[activePod].Name, activePod)
			// ignore completed/failed pods
			if uidMap[activePod].Status.Phase == corev1.PodFailed || uidMap[activePod].Status.Phase == corev1.PodSucceeded {
				t.Logf("Ignoring active pod %v, phase=%v", uidMap[activePod].Name, uidMap[activePod].Status.Phase)
				continue
			}
			if uidMap[activePod].Status.Phase != corev1.PodRunning {
				t.Logf("activePod %v is not running: %v\n", uidMap[activePod].Name, uidMap[activePod].Status.Phase)
				return false, nil
			}
			atLeastOneVmiIsRunning = true
		}
		if !atLeastOneVmiIsRunning {
			t.Logf("vmi %v does not have any activePod running\n", item.Name)
			return false, nil
		}
	}

	return true, nil
}

func initPluginRegistry() {
	pluginregistry.PluginRegistry = pluginregistry.NewRegistry()
	pluginregistry.Register(defaultevictor.PluginName, defaultevictor.New, &defaultevictor.DefaultEvictor{}, &defaultevictor.DefaultEvictorArgs{}, defaultevictor.ValidateDefaultEvictorArgs, defaultevictor.SetDefaults_DefaultEvictorArgs, pluginregistry.PluginRegistry)
	pluginregistry.Register(podlifetime.PluginName, podlifetime.New, &podlifetime.PodLifeTime{}, &podlifetime.PodLifeTimeArgs{}, podlifetime.ValidatePodLifeTimeArgs, podlifetime.SetDefaults_PodLifeTimeArgs, pluginregistry.PluginRegistry)
}

func podLifeTimePolicy() *api.DeschedulerPolicy {
	return &api.DeschedulerPolicy{
		MaxNoOfPodsToEvictPerNamespace: utilptr.To[uint](1),
		Profiles: []api.DeschedulerProfile{
			{
				Name: "KubeVirtPodLifetimeProfile",
				PluginConfigs: []api.PluginConfig{
					{
						Name: podlifetime.PluginName,
						Args: &podlifetime.PodLifeTimeArgs{
							MaxPodLifeTimeSeconds: utilptr.To[uint](1), // set it to immidiate eviction
							Namespaces: &api.Namespaces{
								Include: []string{"default"},
							},
						},
					},
					{
						Name: defaultevictor.PluginName,
						Args: &defaultevictor.DefaultEvictorArgs{
							EvictLocalStoragePods: true,
						},
					},
				},
				Plugins: api.Plugins{
					Filter: api.PluginSet{
						Enabled: []string{
							defaultevictor.PluginName,
						},
					},
					Deschedule: api.PluginSet{
						Enabled: []string{
							podlifetime.PluginName,
						},
					},
				},
			},
		},
	}
}

func kVirtRunningPodNames(t *testing.T, ctx context.Context, kubeClient clientset.Interface) []string {
	podList, err := kubeClient.CoreV1().Pods("default").List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Logf("Unable to list pods: %v", err)
		return nil
	}

	names := []string{}
	for _, item := range podList.Items {
		if !strings.HasPrefix(item.Name, "virt-launcher-vmi-masquerade-") {
			t.Fatalf("Only pod names with 'virt-launcher-vmi-masquerade-' prefix are expected, got %q instead", item.Name)
		}
		if item.Status.Phase == corev1.PodRunning {
			names = append(names, item.Name)
		}
	}
	return names
}

func TestLiveMigrationInBackground(t *testing.T) {
	initPluginRegistry()

	ctx := context.Background()

	kubeClient, err := client.CreateClient(componentbaseconfig.ClientConnectionConfiguration{Kubeconfig: os.Getenv("KUBECONFIG")}, "")
	if err != nil {
		t.Errorf("Error during kubernetes client creation with %v", err)
	}

	eventClient, err := client.CreateClient(componentbaseconfig.ClientConnectionConfiguration{Kubeconfig: os.Getenv("KUBECONFIG")}, "")
	if err != nil {
		t.Errorf("Error during event client creation with %v", err)
	}

	clientConfig := kubecli.DefaultClientConfig(&pflag.FlagSet{})
	kvClient, err := kubecli.GetKubevirtClientFromClientConfig(clientConfig)
	if err != nil {
		t.Fatalf("Unable to obtain KubeVirt client: %v\n", err)
	}

	waitForKubevirtReady(t, ctx, kvClient)

	// Delete all VMIs
	defer func() {
		for i := 1; i <= vmiCount; i++ {
			vmi := virtualMachineInstance(i)
			err := kvClient.VirtualMachineInstance("default").Delete(context.Background(), vmi.Name, metav1.DeleteOptions{})
			if err != nil && !apierrors.IsNotFound(err) {
				t.Logf("Unable to delete vmi %v: %v", vmi.Name, err)
			}
		}
		wait.PollUntilContextTimeout(ctx, 5*time.Second, 60*time.Second, true, func(ctx context.Context) (bool, error) {
			podList, err := kubeClient.CoreV1().Pods("default").List(ctx, metav1.ListOptions{})
			if err != nil {
				return false, err
			}
			lPods := len(podList.Items)
			if lPods > 0 {
				t.Logf("Waiting until all pods under default namespace are gone, %v remaining", lPods)
				return false, nil
			}
			return true, nil
		})
	}()

	// Create N vmis and wait for the corresponding vm pods to be ready and running
	for i := 1; i <= vmiCount; i++ {
		vmi := virtualMachineInstance(i)
		_, err = kvClient.VirtualMachineInstance("default").Create(context.Background(), vmi, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("Unable to create KubeVirt vmi: %v\n", err)
		}
	}

	// Wait until all VMIs have running pods
	if err := wait.PollUntilContextTimeout(ctx, 5*time.Second, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		return allVMIsHaveRunningPods(t, ctx, kubeClient, kvClient)
	}); err != nil {
		t.Fatalf("Error waiting for all vmi active pods to be running: %v", err)
	}

	// configure the descheduler policy for PodLifetime with max num of evictions per namespace to be 1
	// info: the descheduling internal is not set so each cycle can be run explicitly
	rs, err := options.NewDeschedulerServer()
	if err != nil {
		t.Fatalf("unable to initialize server: %v\n", err)
	}
	rs.Client = kubeClient
	rs.EventClient = eventClient

	usedRunningPodNames := make(map[string]struct{})
	// vmiCount number of names is expected
	names := kVirtRunningPodNames(t, ctx, kubeClient)
	t.Logf("vmi pods: %#v\n", names)
	if len(names) != vmiCount {
		t.Fatalf("Expected %v vmi pods, got %v instead", vmiCount, len(names))
	}
	for _, name := range names {
		usedRunningPodNames[name] = struct{}{}
	}

	policy := podLifeTimePolicy()

	prevTotal := uint(0)
	jumps := 0
	// keep running the descheduling cycle until the migration is triggered and completed few times or timeout
	for i := 0; i < 120; i++ {
		err := descheduler.RunDeschedulerStrategies(ctx, rs, policy, "v1")
		if err != nil {
			t.Fatalf("Failed running a descheduling cycle: %v", err)
		}
		// monitor how many pods get evicted
		names := kVirtRunningPodNames(t, ctx, kubeClient)
		t.Logf("vmi pods: %#v\n", names)
		// The number of pods need to be kept between vmiCount and vmiCount+1.
		// At most two pods are expected to have virt-launcher-vmi-masquerade-X prefix name in common.
		prefixes := make(map[string]uint)
		for _, name := range names {
			// "virt-launcher-vmi-masquerade-"
			str := strings.Split(name, "-")[4]
			prefixes[str]++
			usedRunningPodNames[name] = struct{}{}
		}

		hasDouble := false
		total := uint(0)
		for idx, count := range prefixes {
			total += count
			if count > 2 {
				t.Fatalf("A vmi vmi-masquerade-%v has more than 2 running active pods (%v), not expected", idx, count)
			}
			if count == 2 {
				if !hasDouble {
					hasDouble = true
					continue
				}
				t.Fatalf("Another vmi with 2 running active pods, not expected")
			}
		}
		// The total sum can not be higher than vmiCount+1
		if total > vmiCount+1 {
			t.Fatalf("Total running pods (%v) are higher than expected vmiCount+1 (%v)", total, vmiCount+1)
		}

		if prevTotal != 0 && prevTotal != total {
			jumps++
		}
		// Expect at least 3 finished live migrations (two should be enough as well, though ...)
		if jumps >= 6 {
			break
		}
		prevTotal = total
		time.Sleep(time.Second)
	}

	if jumps < 6 {
		t.Fatalf("Expected at least 3 finished live migrations, got less: %v", jumps/2.0)
	}
	t.Logf("The live migration finished 3 times")

	// len(usedRunningPodNames) is expected to be vmiCount + jumps/2 + 1 (one more live migration could still be initiated)
	fmt.Printf("len(usedRunningPodNames): %v, upper limit: %v\n", len(usedRunningPodNames), vmiCount+jumps/2+1)
	if len(usedRunningPodNames) > vmiCount+jumps/2+1 {
		t.Fatalf("Expected vmiCount + jumps/2 + 1 = %v running pods, got %v instead", vmiCount+jumps/2+1, len(usedRunningPodNames))
	}

	if err := wait.PollUntilContextTimeout(ctx, 5*time.Second, 60*time.Second, true, func(ctx context.Context) (bool, error) {
		names := kVirtRunningPodNames(t, ctx, kubeClient)
		t.Logf("vmi pods: %#v\n", names)
		lNames := len(names)
		if lNames != vmiCount {
			t.Logf("Waiting for the number of running vmi pods to be %v, got %v instead", vmiCount, lNames)
			return false, nil
		}
		t.Logf("The number of running vmi pods is %v as expected", vmiCount)
		return true, nil
	}); err != nil {
		t.Fatalf("Error waiting for %v vmi active pods to be running: %v", vmiCount, err)
	}

	t.Logf("Configuring the descheduler policy for PodLifetime with no limits")
	policy.MaxNoOfPodsToEvictPerNamespace = nil

	remainingPods := make(map[string]struct{})
	for _, name := range kVirtRunningPodNames(t, ctx, kubeClient) {
		remainingPods[name] = struct{}{}
	}

	t.Logf("Waiting until all pods are evicted (no limit set)")
	err = descheduler.RunDeschedulerStrategies(ctx, rs, policy, "v1")
	if err := wait.PollUntilContextTimeout(ctx, 5*time.Second, 60*time.Second, true, func(ctx context.Context) (bool, error) {
		names := kVirtRunningPodNames(t, ctx, kubeClient)
		for _, name := range names {
			if _, exists := remainingPods[name]; exists {
				t.Logf("Waiting for %v to disappear", name)
				return false, nil
			}
		}
		lNames := len(names)
		if lNames != vmiCount {
			t.Logf("Waiting for the number of newly running vmi pods to be %v, got %v instead", vmiCount, lNames)
			return false, nil
		}
		t.Logf("The number of newly running vmi pods is %v as expected", vmiCount)
		return true, nil
	}); err != nil {
		t.Fatalf("Error waiting for %v new vmi active pods to be running: %v", vmiCount, err)
	}
}
