package evictions

import (
	policy "k8s.io/api/policy/v1"
)

type Options struct {
	policyGroupVersion         string
	dryRun                     bool
	maxPodsToEvictPerNode      *uint
	maxPodsToEvictPerNamespace *uint
	metricsEnabled             bool
	assumedRequestTimeout      uint
}

// NewOptions returns an Options with default values.
func NewOptions() *Options {
	return &Options{
		policyGroupVersion:    policy.SchemeGroupVersion.String(),
		assumedRequestTimeout: AssumedEvictionRequestTimeoutSeconds,
	}
}

func (o *Options) WithPolicyGroupVersion(policyGroupVersion string) *Options {
	o.policyGroupVersion = policyGroupVersion
	return o
}

func (o *Options) WithDryRun(dryRun bool) *Options {
	o.dryRun = dryRun
	return o
}

func (o *Options) WithMaxPodsToEvictPerNode(maxPodsToEvictPerNode *uint) *Options {
	o.maxPodsToEvictPerNode = maxPodsToEvictPerNode
	return o
}

func (o *Options) WithMaxPodsToEvictPerNamespace(maxPodsToEvictPerNamespace *uint) *Options {
	o.maxPodsToEvictPerNamespace = maxPodsToEvictPerNamespace
	return o
}

func (o *Options) WithMetricsEnabled(metricsEnabled bool) *Options {
	o.metricsEnabled = metricsEnabled
	return o
}

func (o *Options) WithAssumedRequestTimeout(assumedRequestTimeout uint) *Options {
	o.assumedRequestTimeout = assumedRequestTimeout
	return o
}
