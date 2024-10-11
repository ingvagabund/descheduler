/*
Copyright 2022 The Kubernetes Authors.

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

package nodeutilization

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	"sigs.k8s.io/descheduler/pkg/descheduler/evictions"
	nodeutil "sigs.k8s.io/descheduler/pkg/descheduler/node"
	podutil "sigs.k8s.io/descheduler/pkg/descheduler/pod"
	frameworktypes "sigs.k8s.io/descheduler/pkg/framework/types"
	"sigs.k8s.io/descheduler/pkg/utils"
)

const LowNodeUtilizationPluginName = "LowNodeUtilization"

// LowNodeUtilization evicts pods from overutilized nodes to underutilized nodes. Note that CPU/Memory requests are used
// to calculate nodes' utilization and not the actual resource usage.

type LowNodeUtilization struct {
	handle                   frameworktypes.Handle
	args                     *LowNodeUtilizationArgs
	podFilter                func(pod *v1.Pod) bool
	podUtilizationFnc        utils.PodUtilizationFnc
	resourceNames            []v1.ResourceName
	underutilizationCriteria []interface{}
	overutilizationCriteria  []interface{}
}

var _ frameworktypes.BalancePlugin = &LowNodeUtilization{}

// NewLowNodeUtilization builds plugin from its arguments while passing a handle
func NewLowNodeUtilization(args runtime.Object, handle frameworktypes.Handle) (frameworktypes.Plugin, error) {
	lowNodeUtilizationArgsArgs, ok := args.(*LowNodeUtilizationArgs)
	if !ok {
		return nil, fmt.Errorf("want args to be of type LowNodeUtilizationArgs, got %T", args)
	}

	// check if Pods/CPU/Mem are set, if not, set them to 100
	if _, ok := lowNodeUtilizationArgsArgs.Thresholds[v1.ResourcePods]; !ok {
		if lowNodeUtilizationArgsArgs.UseDeviationThresholds {
			lowNodeUtilizationArgsArgs.Thresholds[v1.ResourcePods] = MinResourcePercentage
			lowNodeUtilizationArgsArgs.TargetThresholds[v1.ResourcePods] = MinResourcePercentage
		} else {
			lowNodeUtilizationArgsArgs.Thresholds[v1.ResourcePods] = MaxResourcePercentage
			lowNodeUtilizationArgsArgs.TargetThresholds[v1.ResourcePods] = MaxResourcePercentage
		}
	}
	if _, ok := lowNodeUtilizationArgsArgs.Thresholds[v1.ResourceCPU]; !ok {
		if lowNodeUtilizationArgsArgs.UseDeviationThresholds {
			lowNodeUtilizationArgsArgs.Thresholds[v1.ResourceCPU] = MinResourcePercentage
			lowNodeUtilizationArgsArgs.TargetThresholds[v1.ResourceCPU] = MinResourcePercentage
		} else {
			lowNodeUtilizationArgsArgs.Thresholds[v1.ResourceCPU] = MaxResourcePercentage
			lowNodeUtilizationArgsArgs.TargetThresholds[v1.ResourceCPU] = MaxResourcePercentage
		}
	}
	if _, ok := lowNodeUtilizationArgsArgs.Thresholds[v1.ResourceMemory]; !ok {
		if lowNodeUtilizationArgsArgs.UseDeviationThresholds {
			lowNodeUtilizationArgsArgs.Thresholds[v1.ResourceMemory] = MinResourcePercentage
			lowNodeUtilizationArgsArgs.TargetThresholds[v1.ResourceMemory] = MinResourcePercentage
		} else {
			lowNodeUtilizationArgsArgs.Thresholds[v1.ResourceMemory] = MaxResourcePercentage
			lowNodeUtilizationArgsArgs.TargetThresholds[v1.ResourceMemory] = MaxResourcePercentage
		}
	}

	podFilter, err := podutil.NewOptions().
		WithFilter(handle.Evictor().Filter).
		BuildFilterFunc()
	if err != nil {
		return nil, fmt.Errorf("error initializing pod filter function: %v", err)
	}

	underutilizationCriteria := []interface{}{
		"CPU", lowNodeUtilizationArgsArgs.Thresholds[v1.ResourceCPU],
		"Mem", lowNodeUtilizationArgsArgs.Thresholds[v1.ResourceMemory],
		"Pods", lowNodeUtilizationArgsArgs.Thresholds[v1.ResourcePods],
	}
	for name := range lowNodeUtilizationArgsArgs.Thresholds {
		if !nodeutil.IsBasicResource(name) {
			underutilizationCriteria = append(underutilizationCriteria, string(name), int64(lowNodeUtilizationArgsArgs.Thresholds[name]))
		}
	}

	overutilizationCriteria := []interface{}{
		"CPU", lowNodeUtilizationArgsArgs.TargetThresholds[v1.ResourceCPU],
		"Mem", lowNodeUtilizationArgsArgs.TargetThresholds[v1.ResourceMemory],
		"Pods", lowNodeUtilizationArgsArgs.TargetThresholds[v1.ResourcePods],
	}
	for name := range lowNodeUtilizationArgsArgs.TargetThresholds {
		if !nodeutil.IsBasicResource(name) {
			overutilizationCriteria = append(overutilizationCriteria, string(name), int64(lowNodeUtilizationArgsArgs.TargetThresholds[name]))
		}
	}

	return &LowNodeUtilization{
		handle:    handle,
		args:      lowNodeUtilizationArgsArgs,
		podFilter: podFilter,
		podUtilizationFnc: func(pod *v1.Pod) (v1.ResourceList, error) {
			req, _ := utils.PodRequestsAndLimits(pod)
			return req, nil
		},
		resourceNames:            getResourceNames(lowNodeUtilizationArgsArgs.Thresholds),
		underutilizationCriteria: underutilizationCriteria,
		overutilizationCriteria:  overutilizationCriteria,
	}, nil
}

// Name retrieves the plugin name
func (l *LowNodeUtilization) Name() string {
	return LowNodeUtilizationPluginName
}

// Balance extension point implementation for the plugin
func (l *LowNodeUtilization) Balance(ctx context.Context, nodes []*v1.Node) *frameworktypes.Status {
	usageSnapshot, err := newUsageSnapshot(nodes, l.resourceNames, l.handle.GetPodsAssignedToNodeFunc(), l.podUtilizationFnc)
	if err != nil {
		return &frameworktypes.Status{
			Err: fmt.Errorf("error getting node usage: %v", err),
		}
	}

	nodeThresholds, err := getNodeThresholds(nodes, l.args.Thresholds, l.args.TargetThresholds, l.resourceNames, l.handle.GetPodsAssignedToNodeFunc(), l.args.UseDeviationThresholds, l.podUtilizationFnc)
	if err != nil {
		return &frameworktypes.Status{
			Err: fmt.Errorf("error getting node thresholds: %v", err),
		}
	}

	lowNodes, sourceNodes := classifyNodes(
		getNodeUsage(nodes, usageSnapshot),
		nodeThresholds,
		// The node has to be schedulable (to be able to move workload there)
		func(node *v1.Node, usage NodeUsage, threshold NodeThresholds) bool {
			if nodeutil.IsNodeUnschedulable(node) {
				klog.V(2).InfoS("Node is unschedulable, thus not considered as underutilized", "node", klog.KObj(node))
				return false
			}
			return isNodeWithLowUtilization(usage, threshold.lowResourceThreshold)
		},
		func(node *v1.Node, usage NodeUsage, threshold NodeThresholds) bool {
			return isNodeAboveTargetUtilization(usage, threshold.highResourceThreshold)
		},
	)

	// log message for nodes with low utilization
	klog.V(1).InfoS("Criteria for a node under utilization", l.underutilizationCriteria...)
	klog.V(1).InfoS("Number of underutilized nodes", "totalNumber", len(lowNodes))

	// log message for over utilized nodes
	klog.V(1).InfoS("Criteria for a node above target utilization", l.overutilizationCriteria...)
	klog.V(1).InfoS("Number of overutilized nodes", "totalNumber", len(sourceNodes))

	if len(lowNodes) == 0 {
		klog.V(1).InfoS("No node is underutilized, nothing to do here, you might tune your thresholds further")
		return nil
	}

	if len(lowNodes) <= l.args.NumberOfNodes {
		klog.V(1).InfoS("Number of nodes underutilized is less or equal than NumberOfNodes, nothing to do here", "underutilizedNodes", len(lowNodes), "numberOfNodes", l.args.NumberOfNodes)
		return nil
	}

	if len(lowNodes) == len(nodes) {
		klog.V(1).InfoS("All nodes are underutilized, nothing to do here")
		return nil
	}

	if len(sourceNodes) == 0 {
		klog.V(1).InfoS("All nodes are under target utilization, nothing to do here")
		return nil
	}

	// stop if node utilization drops below target threshold or any of required capacity (cpu, memory, pods) is moved
	continueEvictionCond := func(nodeInfo NodeInfo, totalAvailableUsage map[v1.ResourceName]*resource.Quantity) bool {
		if !isNodeAboveTargetUtilization(nodeInfo.NodeUsage, nodeInfo.thresholds.highResourceThreshold) {
			return false
		}
		for name := range totalAvailableUsage {
			if totalAvailableUsage[name].CmpInt64(0) < 1 {
				return false
			}
		}

		return true
	}

	// Sort the nodes by the usage in descending order
	sortNodesByUsage(sourceNodes, false)

	evictPodsFromSourceNodes(
		ctx,
		l.args.EvictableNamespaces,
		sourceNodes,
		lowNodes,
		l.handle.Evictor(),
		evictions.EvictOptions{StrategyName: LowNodeUtilizationPluginName},
		l.podFilter,
		l.resourceNames,
		continueEvictionCond)

	return nil
}
