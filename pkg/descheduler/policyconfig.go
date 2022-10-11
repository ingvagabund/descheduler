/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package descheduler

import (
	"fmt"
	"io/ioutil"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/klog/v2"

	"sigs.k8s.io/descheduler/pkg/api/v1alpha1"
	"sigs.k8s.io/descheduler/pkg/api/v1alpha2"
	"sigs.k8s.io/descheduler/pkg/descheduler/scheme"
	"sigs.k8s.io/descheduler/pkg/framework/plugins/defaultevictor"
	"sigs.k8s.io/descheduler/pkg/framework/plugins/nodeutilization"
	"sigs.k8s.io/descheduler/pkg/framework/plugins/podlifetime"
	"sigs.k8s.io/descheduler/pkg/framework/plugins/removeduplicates"
	"sigs.k8s.io/descheduler/pkg/framework/plugins/removefailedpods"
	"sigs.k8s.io/descheduler/pkg/framework/plugins/removepodshavingtoomanyrestarts"
	"sigs.k8s.io/descheduler/pkg/framework/plugins/removepodsviolatinginterpodantiaffinity"
	"sigs.k8s.io/descheduler/pkg/framework/plugins/removepodsviolatingnodeaffinity"
	"sigs.k8s.io/descheduler/pkg/framework/plugins/removepodsviolatingnodetaints"
	"sigs.k8s.io/descheduler/pkg/framework/plugins/removepodsviolatingtopologyspreadconstraint"
)

func LoadPolicyConfig(policyConfigFile string) (*v1alpha2.DeschedulerPolicy, error) {
	if policyConfigFile == "" {
		klog.V(1).InfoS("Policy config file not specified")
		return nil, nil
	}

	policy, err := ioutil.ReadFile(policyConfigFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read policy config file %q: %+v", policyConfigFile, err)
	}

	decoder := scheme.Codecs.UniversalDecoder(v1alpha1.SchemeGroupVersion, v1alpha2.SchemeGroupVersion)
	obj, err := runtime.Decode(decoder, policy)
	if err != nil {
		return nil, fmt.Errorf("failed decoding descheduler's policy config %q: %v", policyConfigFile, err)
	}
	versionedPolicy, err := decodeVersionedPolicy(obj.GetObjectKind(), decoder, policy)
	if err != nil {
		return nil, fmt.Errorf("failed decoding descheduler's policy config %q: %v", policyConfigFile, err)
	}

	internalPolicy := &v1alpha2.DeschedulerPolicy{}
	if err := scheme.Scheme.Convert(versionedPolicy, internalPolicy, nil); err != nil {
		return nil, fmt.Errorf("failed converting versioned policy to internal policy version: %v", err)
	}

	return internalPolicy, nil
}

func decodeVersionedPolicy(kind schema.ObjectKind, decoder runtime.Decoder, policy []byte) (*v1alpha2.DeschedulerPolicy, error) {
	v2Policy := &v1alpha2.DeschedulerPolicy{}
	var err error
	if kind.GroupVersionKind().Version == "v1alpha1" {
		v1Policy := &v1alpha1.DeschedulerPolicy{}
		if err := runtime.DecodeInto(decoder, policy, v1Policy); err != nil {
			return nil, err
		}
		v2Policy, err = convertV1ToV2Policy(v1Policy)
		if err != nil {
			return nil, err
		}
	} else {
		if err = runtime.DecodeInto(decoder, policy, v2Policy); err != nil {
			return nil, err
		}
	}
	v2Policy = setDefaults(*v2Policy)
	err = validateDeschedulerConfiguration(*v2Policy)
	if err != nil {
		return nil, err
	}
	return v2Policy, nil
}

func convertV1ToV2Policy(in *v1alpha1.DeschedulerPolicy) (*v1alpha2.DeschedulerPolicy, error) {
	profiles, err := strategyToProfiles(in.Strategies)
	if err != nil {
		return nil, err
	}

	profilesWithDefaultEvictor := policyToDefaultEvictor(in, *profiles)

	return &v1alpha2.DeschedulerPolicy{
		TypeMeta:                       *&in.TypeMeta,
		Profiles:                       *profilesWithDefaultEvictor,
		NodeSelector:                   in.NodeSelector,
		MaxNoOfPodsToEvictPerNode:      in.MaxNoOfPodsToEvictPerNode,
		MaxNoOfPodsToEvictPerNamespace: in.MaxNoOfPodsToEvictPerNamespace,
	}, nil
}

func policyToDefaultEvictor(in *v1alpha1.DeschedulerPolicy, profiles []v1alpha2.Profile) *[]v1alpha2.Profile {
	var defaultEvictorArgs *defaultevictor.DefaultEvictorArgs
	// LabelSelector, PriorityThreshold and Nodefit are passed through the strategy
	// parameters from v1alpha1 while processing those in pkg/descheduler/descheduler.go
	defaultEvictorArgs.NodeSelector = *in.NodeSelector
	defaultEvictorArgs.EvictLocalStoragePods = *in.EvictLocalStoragePods
	defaultEvictorArgs.EvictSystemCriticalPods = *in.EvictSystemCriticalPods
	defaultEvictorArgs.IgnorePvcPods = *in.IgnorePVCPods
	defaultEvictorArgs.EvictFailedBarePods = *in.EvictFailedBarePods
	profiles[0].PluginConfig = append(profiles[0].PluginConfig, configurePlugin(defaultEvictorArgs, defaultevictor.PluginName))
	profiles[0].Plugins.Filter.Enabled = append(profiles[0].Plugins.Filter.Enabled, defaultevictor.PluginName)
	profiles[0].Plugins.PreEvictionFilter.Enabled = append(profiles[0].Plugins.PreEvictionFilter.Enabled, defaultevictor.PluginName)
	profiles[0].Plugins.Evict.Enabled = append(profiles[0].Plugins.Evict.Enabled, defaultevictor.PluginName)
	return &profiles
}

func setDefaults(in v1alpha2.DeschedulerPolicy) *v1alpha2.DeschedulerPolicy {
	out := &v1alpha2.DeschedulerPolicy{}
	out = setDefaultEvictor(in)
	return out
}

func setDefaultEvictor(in v1alpha2.DeschedulerPolicy) *v1alpha2.DeschedulerPolicy {
	for idx, profile := range in.Profiles {
		if len(profile.Plugins.Filter.Enabled) == 0 {
			in.Profiles[idx].Plugins.Filter.Enabled = append(in.Profiles[0].Plugins.Filter.Enabled, defaultevictor.PluginName)
			in.Profiles[idx].PluginConfig = append(
				in.Profiles[idx].PluginConfig, v1alpha2.PluginConfig{
					Name: defaultevictor.PluginName,
					Args: &defaultevictor.DefaultEvictorArgs{
						EvictLocalStoragePods:   false,
						EvictSystemCriticalPods: false,
						IgnorePvcPods:           false,
						EvictFailedBarePods:     false,
					},
				},
			)
		}

	}
	return &in
}

func validateDeschedulerConfiguration(in v1alpha2.DeschedulerPolicy) error {
	// v1alpha2.DeschedulerPolicy needs only 1 evictor plugin enabled
	for _, profile := range in.Profiles {
		if len(profile.Plugins.Evict.Enabled) > 1 {
			return fmt.Errorf("profile with multiple evictor plugins enable found. Please enable a single evictor plugin.")
		}
	}
	return nil
}

func strategyToProfiles(strategies v1alpha1.StrategyList) (*[]v1alpha2.Profile, error) {
	var profiles []v1alpha2.Profile
	for name, strategy := range strategies {
		switch name {
		case "RemoveDuplicates":
			removeduplicatesArgs := convertRemoveDuplicatesArgs(strategy.Params)
			profiles[0].PluginConfig = append(profiles[0].PluginConfig, configurePlugin(removeduplicatesArgs, string(name)))
			if strategy.Enabled {
				profiles[0].Plugins.Balance.Enabled = append(profiles[0].Plugins.Balance.Enabled, string(name))
			} else {
				profiles[0].Plugins.Balance.Disabled = append(profiles[0].Plugins.Balance.Enabled, string(name))
			}
		case "LowNodeUtilization":
			lowNodeUtilizationArgs := convertLowNodeUtilizationArgs(strategy.Params)
			profiles[0].PluginConfig = append(profiles[0].PluginConfig, configurePlugin(lowNodeUtilizationArgs, string(name)))
			if strategy.Enabled {
				profiles[0].Plugins.Balance.Enabled = append(profiles[0].Plugins.Balance.Enabled, string(name))
			} else {
				profiles[0].Plugins.Balance.Disabled = append(profiles[0].Plugins.Balance.Enabled, string(name))
			}
		case "HighNodeUtilization":
			highNodeUtilizationArgs := convertHighNodeUtilizationArgs(strategy.Params)
			profiles[0].PluginConfig = append(profiles[0].PluginConfig, configurePlugin(highNodeUtilizationArgs, string(name)))
			if strategy.Enabled {
				profiles[0].Plugins.Balance.Enabled = append(profiles[0].Plugins.Balance.Enabled, string(name))
			} else {
				profiles[0].Plugins.Balance.Disabled = append(profiles[0].Plugins.Balance.Enabled, string(name))
			}
		case "RemovePodsViolatingInterPodAntiAffinity":
			removePodsViolatingInterPodAntiAffinityArgs := convertRemovePodsViolatingInterPodAntiAffinityArgs(strategy.Params)
			profiles[0].PluginConfig = append(profiles[0].PluginConfig, configurePlugin(removePodsViolatingInterPodAntiAffinityArgs, string(name)))
			if strategy.Enabled {
				profiles[0].Plugins.Deschedule.Enabled = append(profiles[0].Plugins.Deschedule.Enabled, string(name))
			} else {
				profiles[0].Plugins.Deschedule.Disabled = append(profiles[0].Plugins.Deschedule.Enabled, string(name))
			}
		case "RemovePodsViolatingNodeAffinity":
			removePodsViolatingNodeAffinityArgs := convertRemovePodsViolatingNodeAffinityArgs(strategy.Params)
			profiles[0].PluginConfig = append(profiles[0].PluginConfig, configurePlugin(removePodsViolatingNodeAffinityArgs, string(name)))
			if strategy.Enabled {
				profiles[0].Plugins.Deschedule.Enabled = append(profiles[0].Plugins.Deschedule.Enabled, string(name))
			} else {
				profiles[0].Plugins.Deschedule.Disabled = append(profiles[0].Plugins.Deschedule.Enabled, string(name))
			}
		case "RemovePodsViolatingNodeTaints":
			removePodsViolatingNodeTaintsArgs := convertRemovePodsViolatingNodeTaintsArgs(strategy.Params)
			profiles[0].PluginConfig = append(profiles[0].PluginConfig, configurePlugin(removePodsViolatingNodeTaintsArgs, string(name)))
			if strategy.Enabled {
				profiles[0].Plugins.Deschedule.Enabled = append(profiles[0].Plugins.Deschedule.Enabled, string(name))
			} else {
				profiles[0].Plugins.Deschedule.Disabled = append(profiles[0].Plugins.Deschedule.Enabled, string(name))
			}
		case "RemovePodsViolatingTopologySpreadConstraint":
			removePodsViolatingTopologySpreadConstraintArgs := convertRemovePodsViolatingTopologySpreadConstraintArgs(strategy.Params)
			profiles[0].PluginConfig = append(profiles[0].PluginConfig, configurePlugin(removePodsViolatingTopologySpreadConstraintArgs, string(name)))
			if strategy.Enabled {
				profiles[0].Plugins.Balance.Enabled = append(profiles[0].Plugins.Balance.Enabled, string(name))
			} else {
				profiles[0].Plugins.Balance.Disabled = append(profiles[0].Plugins.Balance.Enabled, string(name))
			}
		case "RemovePodsHavingTooManyRestarts":
			removePodsHavingTooManyRestartsArgs := convertRemovePodsHavingTooManyRestartsArgs(strategy.Params)
			profiles[0].PluginConfig = append(profiles[0].PluginConfig, configurePlugin(removePodsHavingTooManyRestartsArgs, string(name)))
			if strategy.Enabled {
				profiles[0].Plugins.Deschedule.Enabled = append(profiles[0].Plugins.Deschedule.Enabled, string(name))
			} else {
				profiles[0].Plugins.Deschedule.Disabled = append(profiles[0].Plugins.Deschedule.Enabled, string(name))
			}
		case "PodLifeTime":
			podLifeTimeArgs := convertPodLifeTimeArgs(strategy.Params)
			profiles[0].PluginConfig = append(profiles[0].PluginConfig, configurePlugin(podLifeTimeArgs, string(name)))
			if strategy.Enabled {
				profiles[0].Plugins.Deschedule.Enabled = append(profiles[0].Plugins.Deschedule.Enabled, string(name))
			} else {
				profiles[0].Plugins.Deschedule.Disabled = append(profiles[0].Plugins.Deschedule.Enabled, string(name))
			}
		case "RemoveFailedPods":
			RemoveFailedPodsArgs := convertRemoveFailedPodsArgs(strategy.Params)
			profiles[0].PluginConfig = append(profiles[0].PluginConfig, configurePlugin(RemoveFailedPodsArgs, string(name)))
			if strategy.Enabled {
				profiles[0].Plugins.Deschedule.Enabled = append(profiles[0].Plugins.Deschedule.Enabled, string(name))
			} else {
				profiles[0].Plugins.Deschedule.Disabled = append(profiles[0].Plugins.Deschedule.Enabled, string(name))
			}
		default:
			return nil, fmt.Errorf("could not process strategy: %s", string(name))
		}
	}
	return &profiles, nil
}

func configurePlugin(args runtime.Object, name string) v1alpha2.PluginConfig {
	var pluginConfig v1alpha2.PluginConfig
	pluginConfig.Args = args
	pluginConfig.Name = name
	return pluginConfig
}

func convertRemoveDuplicatesArgs(params *v1alpha1.StrategyParameters) *removeduplicates.RemoveDuplicatesArgs {
	var removeduplicatesArgs *removeduplicates.RemoveDuplicatesArgs
	removeduplicatesArgs.ExcludeOwnerKinds = params.RemoveDuplicates.ExcludeOwnerKinds
	removeduplicatesArgs.Namespaces.Include = params.Namespaces.Include
	removeduplicatesArgs.Namespaces.Exclude = params.Namespaces.Exclude
	return removeduplicatesArgs
}

func convertLowNodeUtilizationArgs(params *v1alpha1.StrategyParameters) *nodeutilization.LowNodeUtilizationArgs {
	var lowNodeUtilizationArgs *nodeutilization.LowNodeUtilizationArgs
	lowNodeUtilizationArgs.TargetThresholds = params.NodeResourceUtilizationThresholds.TargetThresholds
	lowNodeUtilizationArgs.Thresholds = params.NodeResourceUtilizationThresholds.Thresholds
	lowNodeUtilizationArgs.UseDeviationThresholds = params.NodeResourceUtilizationThresholds.UseDeviationThresholds
	lowNodeUtilizationArgs.NumberOfNodes = params.NodeResourceUtilizationThresholds.NumberOfNodes
	return lowNodeUtilizationArgs
}

func convertHighNodeUtilizationArgs(params *v1alpha1.StrategyParameters) *nodeutilization.HighNodeUtilizationArgs {
	var highNodeUtilizationArgs *nodeutilization.HighNodeUtilizationArgs
	highNodeUtilizationArgs.NumberOfNodes = params.NodeResourceUtilizationThresholds.NumberOfNodes
	return highNodeUtilizationArgs
}

func convertRemovePodsViolatingInterPodAntiAffinityArgs(params *v1alpha1.StrategyParameters) *removepodsviolatinginterpodantiaffinity.RemovePodsViolatingInterPodAntiAffinityArgs {
	var removePodsViolatingInterPodAntiAffinityArgs *removepodsviolatinginterpodantiaffinity.RemovePodsViolatingInterPodAntiAffinityArgs
	removePodsViolatingInterPodAntiAffinityArgs.Namespaces.Include = params.Namespaces.Include
	removePodsViolatingInterPodAntiAffinityArgs.Namespaces.Exclude = params.Namespaces.Exclude
	removePodsViolatingInterPodAntiAffinityArgs.LabelSelector = params.LabelSelector
	return removePodsViolatingInterPodAntiAffinityArgs
}

func convertRemovePodsViolatingNodeAffinityArgs(params *v1alpha1.StrategyParameters) *removepodsviolatingnodeaffinity.RemovePodsViolatingNodeAffinityArgs {
	var removePodsViolatingNodeAffinityArgs *removepodsviolatingnodeaffinity.RemovePodsViolatingNodeAffinityArgs
	removePodsViolatingNodeAffinityArgs.Namespaces.Include = params.Namespaces.Include
	removePodsViolatingNodeAffinityArgs.Namespaces.Exclude = params.Namespaces.Exclude
	removePodsViolatingNodeAffinityArgs.LabelSelector = params.LabelSelector
	return removePodsViolatingNodeAffinityArgs
}

func convertRemovePodsViolatingNodeTaintsArgs(params *v1alpha1.StrategyParameters) *removepodsviolatingnodetaints.RemovePodsViolatingNodeTaintsArgs {
	var removePodsViolatingNodeTaintsArgs *removepodsviolatingnodetaints.RemovePodsViolatingNodeTaintsArgs
	removePodsViolatingNodeTaintsArgs.Namespaces.Include = params.Namespaces.Include
	removePodsViolatingNodeTaintsArgs.Namespaces.Exclude = params.Namespaces.Exclude
	removePodsViolatingNodeTaintsArgs.LabelSelector = params.LabelSelector
	removePodsViolatingNodeTaintsArgs.IncludePreferNoSchedule = params.IncludePreferNoSchedule
	removePodsViolatingNodeTaintsArgs.ExcludedTaints = params.ExcludedTaints
	return removePodsViolatingNodeTaintsArgs
}

func convertRemovePodsViolatingTopologySpreadConstraintArgs(params *v1alpha1.StrategyParameters) *removepodsviolatingtopologyspreadconstraint.RemovePodsViolatingTopologySpreadConstraintArgs {
	var removePodsViolatingTopologySpreadConstraintArgs *removepodsviolatingtopologyspreadconstraint.RemovePodsViolatingTopologySpreadConstraintArgs
	removePodsViolatingTopologySpreadConstraintArgs.Namespaces.Include = params.Namespaces.Include
	removePodsViolatingTopologySpreadConstraintArgs.Namespaces.Exclude = params.Namespaces.Exclude
	removePodsViolatingTopologySpreadConstraintArgs.LabelSelector = params.LabelSelector
	removePodsViolatingTopologySpreadConstraintArgs.IncludeSoftConstraints = params.IncludeSoftConstraints
	return removePodsViolatingTopologySpreadConstraintArgs
}

func convertRemovePodsHavingTooManyRestartsArgs(params *v1alpha1.StrategyParameters) *removepodshavingtoomanyrestarts.RemovePodsHavingTooManyRestartsArgs {
	var removePodsHavingTooManyRestartsArgs *removepodshavingtoomanyrestarts.RemovePodsHavingTooManyRestartsArgs
	removePodsHavingTooManyRestartsArgs.Namespaces.Include = params.Namespaces.Include
	removePodsHavingTooManyRestartsArgs.Namespaces.Exclude = params.Namespaces.Exclude
	removePodsHavingTooManyRestartsArgs.LabelSelector = params.LabelSelector
	removePodsHavingTooManyRestartsArgs.PodRestartThreshold = params.PodsHavingTooManyRestarts.PodRestartThreshold
	removePodsHavingTooManyRestartsArgs.IncludingInitContainers = params.PodsHavingTooManyRestarts.IncludingInitContainers
	return removePodsHavingTooManyRestartsArgs
}

func convertPodLifeTimeArgs(params *v1alpha1.StrategyParameters) *podlifetime.PodLifeTimeArgs {
	var podLifeTimeArgs *podlifetime.PodLifeTimeArgs
	podLifeTimeArgs.Namespaces.Include = params.Namespaces.Include
	podLifeTimeArgs.Namespaces.Exclude = params.Namespaces.Exclude
	podLifeTimeArgs.LabelSelector = params.LabelSelector
	podLifeTimeArgs.MaxPodLifeTimeSeconds = params.PodLifeTime.MaxPodLifeTimeSeconds
	podLifeTimeArgs.States = params.PodLifeTime.States
	return podLifeTimeArgs
}

func convertRemoveFailedPodsArgs(params *v1alpha1.StrategyParameters) *removefailedpods.RemoveFailedPodsArgs {
	var removeFailedPodsArgs *removefailedpods.RemoveFailedPodsArgs
	removeFailedPodsArgs.Namespaces.Include = params.Namespaces.Include
	removeFailedPodsArgs.Namespaces.Exclude = params.Namespaces.Exclude
	removeFailedPodsArgs.LabelSelector = params.LabelSelector
	removeFailedPodsArgs.ExcludeOwnerKinds = params.FailedPods.ExcludeOwnerKinds
	removeFailedPodsArgs.MinPodLifetimeSeconds = params.FailedPods.MinPodLifetimeSeconds
	removeFailedPodsArgs.Reasons = params.FailedPods.Reasons
	removeFailedPodsArgs.IncludingInitContainers = params.FailedPods.IncludingInitContainers
	return removeFailedPodsArgs
}
