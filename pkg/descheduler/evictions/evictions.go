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

package evictions

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	v1 "k8s.io/api/core/v1"
	policy "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/events"
	"k8s.io/klog/v2"
	"sigs.k8s.io/descheduler/metrics"

	eutils "sigs.k8s.io/descheduler/pkg/descheduler/evictions/utils"
	"sigs.k8s.io/descheduler/pkg/tracing"
)

var (
	AssumedEvictionRequestTimeoutSeconds uint = 10 * 60 // 10 minutes
)

type evictionRequestItem struct {
	pod               *v1.Pod
	assumed           bool
	creationTimestamp metav1.Time
}

type evictionRequestsCache struct {
	mu                    sync.Mutex
	requests              map[string]evictionRequestItem
	rPerNode              map[string]uint
	rPerNamespace         map[string]uint
	assumedRequestTimeout uint
}

func newEvictionRequestsCache(assumedRequestTimeout uint) *evictionRequestsCache {
	return &evictionRequestsCache{
		requests:              make(map[string]evictionRequestItem),
		rPerNode:              make(map[string]uint),
		rPerNamespace:         make(map[string]uint),
		assumedRequestTimeout: assumedRequestTimeout,
	}
}

// cleanCache removes all assumed entries that has not beed confirmed
// for more than a specified timeout
func (erc *evictionRequestsCache) cleanCache(ctx context.Context) {
	erc.mu.Lock()
	defer erc.mu.Unlock()
	klog.V(4).Infof("Cleaning cache of assumed eviction requests in background")
	for uid, item := range erc.requests {
		if item.assumed {
			requestAgeSeconds := uint(metav1.Now().Sub(item.creationTimestamp.Local()).Seconds())
			if requestAgeSeconds > erc.assumedRequestTimeout {
				klog.V(4).Infof("Assumed eviction request in background timed out after %vs for %v, deleting", erc.assumedRequestTimeout, klog.KObj(item.pod))
				erc.deleteItem(uid)
			}
		}
	}
}

func (erc *evictionRequestsCache) evictionRequestsPerNode(nodeName string) uint {
	erc.mu.Lock()
	defer erc.mu.Unlock()
	return erc.rPerNode[nodeName]
}

func (erc *evictionRequestsCache) evictionRequestsPerNamespace(ns string) uint {
	erc.mu.Lock()
	defer erc.mu.Unlock()
	return erc.rPerNamespace[ns]
}

func (erc *evictionRequestsCache) TotalEvictionRequests() uint {
	erc.mu.Lock()
	defer erc.mu.Unlock()
	return uint(len(erc.requests))
}

func (erc *evictionRequestsCache) addPod(pod *v1.Pod) error {
	erc.mu.Lock()
	defer erc.mu.Unlock()
	uid, err := getPodKey(pod)
	if err != nil {
		return fmt.Errorf("unable to get pod key: %v", err)
	}
	if _, exists := erc.requests[uid]; exists {
		return nil
	}
	erc.requests[uid] = evictionRequestItem{pod: pod}
	// TODO(ingvagabund): node could be empty => relist the cache once in a while
	erc.rPerNode[pod.Spec.NodeName]++
	erc.rPerNamespace[pod.Namespace]++
	return nil
}

func (erc *evictionRequestsCache) assumePod(pod *v1.Pod) error {
	erc.mu.Lock()
	defer erc.mu.Unlock()
	uid, err := getPodKey(pod)
	if err != nil {
		return fmt.Errorf("unable to get pod key: %v", err)
	}
	if _, exists := erc.requests[uid]; exists {
		return nil
	}
	// TODO(ingvagabund): timeout for any assumed pod to handle the case
	// when an eviction in background was confirmed, yet the controller
	// responsible for the eviction crashed before it was able to apply
	// the annotation for eviction-in-progress. Or, relist every 10 minutes
	// and drop any assume pod that has not been confimed for more than a minute.
	erc.requests[uid] = evictionRequestItem{
		pod:     pod,
		assumed: true,
		// creationTimestamp: metav1.NewTime(time.Now().Add(-time.Second * 605)),
		creationTimestamp: metav1.NewTime(time.Now()),
	}
	erc.rPerNode[pod.Spec.NodeName]++
	erc.rPerNamespace[pod.Namespace]++
	return nil
}

// no locking, expected to be invoked from protected methods only
func (erc *evictionRequestsCache) deleteItem(uid string) {
	erc.rPerNode[erc.requests[uid].pod.Spec.NodeName]--
	if erc.rPerNode[erc.requests[uid].pod.Spec.NodeName] == 0 {
		delete(erc.rPerNode, erc.requests[uid].pod.Spec.NodeName)
	}
	erc.rPerNamespace[erc.requests[uid].pod.Namespace]--

	if erc.rPerNamespace[erc.requests[uid].pod.Namespace] == 0 {
		delete(erc.rPerNamespace, erc.requests[uid].pod.Namespace)
	}
	delete(erc.requests, uid)
}

func (erc *evictionRequestsCache) deletePod(pod *v1.Pod) error {
	erc.mu.Lock()
	defer erc.mu.Unlock()
	uid, err := getPodKey(pod)
	if err != nil {
		return fmt.Errorf("unable to get pod key: %v", err)
	}
	if _, exists := erc.requests[uid]; exists {
		erc.deleteItem(uid)
	}
	return nil
}

func (erc evictionRequestsCache) hasPod(pod *v1.Pod) (bool, error) {
	erc.mu.Lock()
	defer erc.mu.Unlock()
	uid, err := getPodKey(pod)
	if err != nil {
		return false, fmt.Errorf("unable to get pod key: %v", err)
	}
	_, exists := erc.requests[uid]
	return exists, nil
}

var (
	EvictionRequestAnnotationKey    = "descheduler.alpha.kubernetes.io/request-evict-only"
	EvictionInProgressAnnotationKey = "descheduler.alpha.kubernetes.io/eviction-in-progress"
)

// GetPodKey returns the string key of a pod.
func getPodKey(pod *v1.Pod) (string, error) {
	uid := string(pod.UID)
	if len(uid) == 0 {
		return "", errors.New("cannot get cache key for pod with empty UID")
	}
	return uid, nil
}

// nodePodEvictedCount keeps count of pods evicted on node
type (
	nodePodEvictedCount    map[string]uint
	namespacePodEvictCount map[string]uint
)

type PodEvictor struct {
	mu                         sync.Mutex
	client                     clientset.Interface
	policyGroupVersion         string
	dryRun                     bool
	maxPodsToEvictPerNode      *uint
	maxPodsToEvictPerNamespace *uint
	nodepodCount               nodePodEvictedCount
	namespacePodCount          namespacePodEvictCount
	metricsEnabled             bool
	eventRecorder              events.EventRecorder
	erCache                    *evictionRequestsCache
}

func NewPodEvictor(
	ctx context.Context,
	client clientset.Interface,
	eventRecorder events.EventRecorder,
	podInformer cache.SharedIndexInformer,
	options *Options,
) *PodEvictor {
	if options == nil {
		options = NewOptions()
	}

	erCache := newEvictionRequestsCache(options.assumedRequestTimeout)

	podInformer.AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				pod, ok := obj.(*v1.Pod)
				if !ok {
					klog.Error(nil, "Cannot convert to *v1.Pod", "obj", obj)
					return
				}
				if _, exists := pod.Annotations[EvictionRequestAnnotationKey]; exists {
					if _, exists := pod.Annotations[EvictionInProgressAnnotationKey]; exists {
						klog.V(4).Infof("Eviction in background detected. Adding pod %q to the cache.", klog.KObj(pod))
						if err := erCache.addPod(pod); err != nil {
							klog.ErrorS(err, "Unable to add pod to cache", "pod", pod)
						}
					}
				}
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				oldPod, ok := oldObj.(*v1.Pod)
				if !ok {
					klog.Error(nil, "Cannot convert oldObj to *v1.Pod", "oldObj", oldObj)
					return
				}
				newPod, ok := newObj.(*v1.Pod)
				if !ok {
					klog.Error(nil, "Cannot convert newObj to *v1.Pod", "newObj", newObj)
					return
				}
				if newPod.Annotations == nil {
					return
				}
				// Ignore pod's that are not subject to an eviction in background
				if _, exists := newPod.Annotations[EvictionRequestAnnotationKey]; !exists {
					return
				}
				// Ignore any pod that does not have eviction in progress
				if _, exists := newPod.Annotations[EvictionInProgressAnnotationKey]; !exists {
					// In case EvictionInProgressAnnotationKey annotation is not present/removed
					// it's unclear whether the eviction was restarted or terminated.
					// If the eviction gets restarted the pod needs to be removed from the cache
					// to allow re-triggerring the eviction.
					if oldPod.Annotations == nil {
						return
					}
					if _, exists := oldPod.Annotations[EvictionInProgressAnnotationKey]; exists {
						// the annotation was removed -> remove the pod from the cache to allow to
						// request for eviction again. In case the eviction got restarted requesting
						// the eviction again is expected to be a no-op. In case the eviction
						// got terminated with no-retry, requesting a new eviction is a normal
						// operation.
						klog.V(4).Infof("Eviction in background canceled (%q annotation removed). Removing pod %q from the cache.", EvictionInProgressAnnotationKey, klog.KObj(newPod))
						if err := erCache.deletePod(newPod); err != nil {
							// If the deletion fails the cache may block eviction
							klog.ErrorS(err, "Unable to delete updated pod from cache", "pod", newPod)
							return
						}
					}
					return
				}
				// Pick up the eviction in progress
				if err := erCache.addPod(newPod); err != nil {
					klog.ErrorS(err, "Unable to add pod to cache", "pod", newPod)
					return
				}
			},
			DeleteFunc: func(obj interface{}) {
				var pod *v1.Pod
				switch t := obj.(type) {
				case *v1.Pod:
					pod = t
				case cache.DeletedFinalStateUnknown:
					var ok bool
					pod, ok = t.Obj.(*v1.Pod)
					if !ok {
						klog.Error(nil, "Cannot convert to *v1.Pod", "obj", t.Obj)
						return
					}
				default:
					klog.Error(nil, "Cannot convert to *v1.Pod", "obj", t)
					return
				}
				// Ignore pod's that are not subject to an eviction in background
				if _, exists := pod.Annotations[EvictionRequestAnnotationKey]; !exists {
					return
				}
				klog.V(4).Infof("Pod with eviction in background deleted/evicted. Removing pod %q from the cache.", klog.KObj(pod))
				if err := erCache.deletePod(pod); err != nil {
					klog.ErrorS(err, "Unable to delete pod from cache", "pod", pod)
					return
				}
			},
		},
	)

	go wait.UntilWithContext(ctx, erCache.cleanCache, 100*time.Millisecond)

	return &PodEvictor{
		client:                     client,
		eventRecorder:              eventRecorder,
		policyGroupVersion:         options.policyGroupVersion,
		dryRun:                     options.dryRun,
		maxPodsToEvictPerNode:      options.maxPodsToEvictPerNode,
		maxPodsToEvictPerNamespace: options.maxPodsToEvictPerNamespace,
		metricsEnabled:             options.metricsEnabled,
		nodepodCount:               make(nodePodEvictedCount),
		namespacePodCount:          make(namespacePodEvictCount),
		erCache:                    erCache,
	}
}

// NodeEvicted gives a number of pods evicted for node
func (pe *PodEvictor) NodeEvicted(node *v1.Node) uint {
	pe.mu.Lock()
	defer pe.mu.Unlock()
	return pe.nodepodCount[node.Name]
}

// TotalEvicted gives a number of pods evicted through all nodes
func (pe *PodEvictor) TotalEvicted() uint {
	pe.mu.Lock()
	defer pe.mu.Unlock()
	var total uint
	for _, count := range pe.nodepodCount {
		total += count
	}
	return total
}

func (pe *PodEvictor) ResetCounters() {
	pe.mu.Lock()
	defer pe.mu.Unlock()
	pe.nodepodCount = make(nodePodEvictedCount)
	pe.namespacePodCount = make(namespacePodEvictCount)
}

func (pe *PodEvictor) SetClient(client clientset.Interface) {
	pe.mu.Lock()
	defer pe.mu.Unlock()
	pe.client = client
}

func (pe *PodEvictor) EvictionRequests(node *v1.Node) uint {
	pe.mu.Lock()
	defer pe.mu.Unlock()
	return pe.erCache.evictionRequestsPerNode(node.Name)
}

func (pe *PodEvictor) TotalEvictionRequests() uint {
	pe.mu.Lock()
	defer pe.mu.Unlock()
	return pe.erCache.TotalEvictionRequests()
}

// EvictOptions provides a handle for passing additional info to EvictPod
type EvictOptions struct {
	// Reason allows for passing details about the specific eviction for logging.
	Reason string
	// ProfileName allows for passing details about profile for observability.
	ProfileName string
	// StrategyName allows for passing details about strategy for observability.
	StrategyName string
}

// EvictPod evicts a pod while exercising eviction limits.
// Returns true when the pod is evicted on the server side.
func (pe *PodEvictor) EvictPod(ctx context.Context, pod *v1.Pod, opts EvictOptions) error {
	pe.mu.Lock()
	defer pe.mu.Unlock()
	if len(pod.UID) == 0 {
		klog.InfoS("Ignoring pod eviction due to missing UID", "pod", pod)
		return fmt.Errorf("Pod %v is missing UID", klog.KObj(pod))
	}

	var span trace.Span
	ctx, span = tracing.Tracer().Start(ctx, "EvictPod", trace.WithAttributes(attribute.String("podName", pod.Name), attribute.String("podNamespace", pod.Namespace), attribute.String("reason", opts.Reason), attribute.String("operation", tracing.EvictOperation)))
	defer span.End()

	if pod.Spec.NodeName != "" {
		if pe.maxPodsToEvictPerNode != nil && pe.nodepodCount[pod.Spec.NodeName]+pe.erCache.evictionRequestsPerNode(pod.Spec.NodeName)+1 > *pe.maxPodsToEvictPerNode {
			err := NewEvictionNodeLimitError(pod.Spec.NodeName)
			if pe.metricsEnabled {
				metrics.PodsEvicted.With(map[string]string{"result": err.Error(), "strategy": opts.StrategyName, "namespace": pod.Namespace, "node": pod.Spec.NodeName, "profile": opts.ProfileName}).Inc()
			}
			span.AddEvent("Eviction Failed", trace.WithAttributes(attribute.String("node", pod.Spec.NodeName), attribute.String("err", err.Error())))
			klog.ErrorS(err, "Error evicting pod", "limit", *pe.maxPodsToEvictPerNode, "node", pod.Spec.NodeName)
			return err
		}
	}

	if pe.maxPodsToEvictPerNamespace != nil && pe.namespacePodCount[pod.Namespace]+pe.erCache.evictionRequestsPerNamespace(pod.Namespace)+1 > *pe.maxPodsToEvictPerNamespace {
		err := NewEvictionNamespaceLimitError(pod.Namespace)
		if pe.metricsEnabled {
			metrics.PodsEvicted.With(map[string]string{"result": err.Error(), "strategy": opts.StrategyName, "namespace": pod.Namespace, "node": pod.Spec.NodeName, "profile": opts.ProfileName}).Inc()
		}
		span.AddEvent("Eviction Failed", trace.WithAttributes(attribute.String("node", pod.Spec.NodeName), attribute.String("err", err.Error())))
		klog.ErrorS(err, "Error evicting pod", "limit", *pe.maxPodsToEvictPerNamespace, "namespace", pod.Namespace)
		return err
	}

	ignore, err := pe.evictPod(ctx, pod)
	if err != nil {
		// err is used only for logging purposes
		span.AddEvent("Eviction Failed", trace.WithAttributes(attribute.String("node", pod.Spec.NodeName), attribute.String("err", err.Error())))
		klog.ErrorS(err, "Error evicting pod", "pod", klog.KObj(pod), "reason", opts.Reason)
		if pe.metricsEnabled {
			metrics.PodsEvicted.With(map[string]string{"result": "error", "strategy": opts.StrategyName, "namespace": pod.Namespace, "node": pod.Spec.NodeName, "profile": opts.ProfileName}).Inc()
		}
		return err
	}

	if !ignore {
		if pod.Spec.NodeName != "" {
			pe.nodepodCount[pod.Spec.NodeName]++
		}
		pe.namespacePodCount[pod.Namespace]++
	}

	if pe.metricsEnabled {
		metrics.PodsEvicted.With(map[string]string{"result": "success", "strategy": opts.StrategyName, "namespace": pod.Namespace, "node": pod.Spec.NodeName, "profile": opts.ProfileName}).Inc()
	}

	if pe.dryRun {
		klog.V(1).InfoS("Evicted pod in dry run mode", "pod", klog.KObj(pod), "reason", opts.Reason, "strategy", opts.StrategyName, "node", pod.Spec.NodeName, "profile", opts.ProfileName)
	} else {
		klog.V(1).InfoS("Evicted pod", "pod", klog.KObj(pod), "reason", opts.Reason, "strategy", opts.StrategyName, "node", pod.Spec.NodeName, "profile", opts.ProfileName)
		reason := opts.Reason
		if len(reason) == 0 {
			reason = opts.StrategyName
			if len(reason) == 0 {
				reason = "NotSet"
			}
		}
		pe.eventRecorder.Eventf(pod, nil, v1.EventTypeNormal, reason, "Descheduled", "pod evicted from %v node by sigs.k8s.io/descheduler", pod.Spec.NodeName)
	}
	return nil
}

// return (ignore, err)
func (pe *PodEvictor) evictPod(ctx context.Context, pod *v1.Pod) (bool, error) {
	if pod.Annotations != nil {
		// eviction in background requested
		if _, exists := pod.Annotations[EvictionRequestAnnotationKey]; exists {
			exists, err := pe.erCache.hasPod(pod)
			if err != nil {
				return false, fmt.Errorf("unable to check whether a pod exists in the cache of eviction requests: %v", err)
			}
			if exists {
				klog.V(0).InfoS("Eviction in background already requested (ignoring)", "pod", klog.KObj(pod))
				return true, nil
			}
		}
	}

	deleteOptions := &metav1.DeleteOptions{}
	// GracePeriodSeconds ?
	eviction := &policy.Eviction{
		TypeMeta: metav1.TypeMeta{
			APIVersion: pe.policyGroupVersion,
			Kind:       eutils.EvictionKind,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      pod.Name,
			Namespace: pod.Namespace,
		},
		DeleteOptions: deleteOptions,
	}
	err := pe.client.PolicyV1().Evictions(eviction.Namespace).Evict(ctx, eviction)

	if pod.Annotations != nil {
		// eviction in background requested
		if _, exists := pod.Annotations[EvictionRequestAnnotationKey]; exists {
			// Simulating https://github.com/kubevirt/kubevirt/pull/11532/files#diff-059cc1fc09e8b469143348cc3aa80b40de987670e008fa18a6fe010061f973c9R77
			if apierrors.IsTooManyRequests(err) && strings.Contains(err.Error(), "Eviction triggered evacuation") {
				if err := pe.erCache.assumePod(pod); err != nil {
					klog.ErrorS(err, "eviction request: unable to assume pod to cache", "pod", pod)
				}
				return true, nil
			}
		}
	}

	if apierrors.IsTooManyRequests(err) {
		return false, fmt.Errorf("error when evicting pod (ignoring) %q: %v", pod.Name, err)
	}
	if apierrors.IsNotFound(err) {
		return false, fmt.Errorf("pod not found when evicting %q: %v", pod.Name, err)
	}
	return false, err
}
