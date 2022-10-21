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

package removepodsviolatingnodetaints

import (
	"k8s.io/apimachinery/pkg/runtime"
)

func addDefaultingFuncs(scheme *runtime.Scheme) error {
	return RegisterDefaults(scheme)
}

// SetDefaults_RemovePodsViolatingNodeTaintsArgs
// TODO: the final default values would be discussed in community
func SetDefaults_RemovePodsViolatingNodeTaintsArgs(obj *RemovePodsViolatingNodeTaintsArgs) {
	if obj.Namespaces == nil {
		obj.Namespaces = nil
	}
	if obj.LabelSelector == nil {
		obj.LabelSelector = nil
	}
	if !obj.IncludePreferNoSchedule {
		obj.IncludePreferNoSchedule = false
	}
	if obj.ExcludedTaints == nil {
		obj.ExcludedTaints = nil
	}
}
