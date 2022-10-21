//go:build !ignore_autogenerated
// +build !ignore_autogenerated

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

// Code generated by conversion-gen. DO NOT EDIT.

package v1alpha2

import (
	unsafe "unsafe"

	conversion "k8s.io/apimachinery/pkg/conversion"
	runtime "k8s.io/apimachinery/pkg/runtime"
	api "sigs.k8s.io/descheduler/pkg/api"
)

func init() {
	localSchemeBuilder.Register(RegisterConversions)
}

// RegisterConversions adds conversion functions to the given scheme.
// Public to allow building arbitrary schemes.
func RegisterConversions(s *runtime.Scheme) error {
	if err := s.AddGeneratedConversionFunc((*Namespaces)(nil), (*api.Namespaces)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return Convert_v1alpha2_Namespaces_To_api_Namespaces(a.(*Namespaces), b.(*api.Namespaces), scope)
	}); err != nil {
		return err
	}
	if err := s.AddGeneratedConversionFunc((*api.Namespaces)(nil), (*Namespaces)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return Convert_api_Namespaces_To_v1alpha2_Namespaces(a.(*api.Namespaces), b.(*Namespaces), scope)
	}); err != nil {
		return err
	}
	if err := s.AddGeneratedConversionFunc((*api.PluginConfig)(nil), (*PluginConfig)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return Convert_api_PluginConfig_To_v1alpha2_PluginConfig(a.(*api.PluginConfig), b.(*PluginConfig), scope)
	}); err != nil {
		return err
	}
	if err := s.AddGeneratedConversionFunc((*PluginSet)(nil), (*api.PluginSet)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return Convert_v1alpha2_PluginSet_To_api_PluginSet(a.(*PluginSet), b.(*api.PluginSet), scope)
	}); err != nil {
		return err
	}
	if err := s.AddGeneratedConversionFunc((*api.PluginSet)(nil), (*PluginSet)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return Convert_api_PluginSet_To_v1alpha2_PluginSet(a.(*api.PluginSet), b.(*PluginSet), scope)
	}); err != nil {
		return err
	}
	if err := s.AddGeneratedConversionFunc((*Plugins)(nil), (*api.Plugins)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return Convert_v1alpha2_Plugins_To_api_Plugins(a.(*Plugins), b.(*api.Plugins), scope)
	}); err != nil {
		return err
	}
	if err := s.AddGeneratedConversionFunc((*api.Plugins)(nil), (*Plugins)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return Convert_api_Plugins_To_v1alpha2_Plugins(a.(*api.Plugins), b.(*Plugins), scope)
	}); err != nil {
		return err
	}
	if err := s.AddGeneratedConversionFunc((*Profile)(nil), (*api.Profile)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return Convert_v1alpha2_Profile_To_api_Profile(a.(*Profile), b.(*api.Profile), scope)
	}); err != nil {
		return err
	}
	if err := s.AddGeneratedConversionFunc((*api.Profile)(nil), (*Profile)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return Convert_api_Profile_To_v1alpha2_Profile(a.(*api.Profile), b.(*Profile), scope)
	}); err != nil {
		return err
	}
	if err := s.AddConversionFunc((*api.DeschedulerPolicy)(nil), (*DeschedulerPolicy)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return Convert_api_DeschedulerPolicy_To_v1alpha2_DeschedulerPolicy(a.(*api.DeschedulerPolicy), b.(*DeschedulerPolicy), scope)
	}); err != nil {
		return err
	}
	if err := s.AddConversionFunc((*DeschedulerPolicy)(nil), (*api.DeschedulerPolicy)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return Convert_v1alpha2_DeschedulerPolicy_To_api_DeschedulerPolicy(a.(*DeschedulerPolicy), b.(*api.DeschedulerPolicy), scope)
	}); err != nil {
		return err
	}
	if err := s.AddConversionFunc((*PluginConfig)(nil), (*api.PluginConfig)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return Convert_v1alpha2_PluginConfig_To_api_PluginConfig(a.(*PluginConfig), b.(*api.PluginConfig), scope)
	}); err != nil {
		return err
	}
	return nil
}

func autoConvert_v1alpha2_DeschedulerPolicy_To_api_DeschedulerPolicy(in *DeschedulerPolicy, out *api.DeschedulerPolicy, s conversion.Scope) error {
	if in.Profiles != nil {
		in, out := &in.Profiles, &out.Profiles
		*out = make([]api.Profile, len(*in))
		for i := range *in {
			if err := Convert_v1alpha2_Profile_To_api_Profile(&(*in)[i], &(*out)[i], s); err != nil {
				return err
			}
		}
	} else {
		out.Profiles = nil
	}
	out.NodeSelector = (*string)(unsafe.Pointer(in.NodeSelector))
	out.MaxNoOfPodsToEvictPerNode = (*uint)(unsafe.Pointer(in.MaxNoOfPodsToEvictPerNode))
	out.MaxNoOfPodsToEvictPerNamespace = (*uint)(unsafe.Pointer(in.MaxNoOfPodsToEvictPerNamespace))
	return nil
}

func autoConvert_api_DeschedulerPolicy_To_v1alpha2_DeschedulerPolicy(in *api.DeschedulerPolicy, out *DeschedulerPolicy, s conversion.Scope) error {
	if in.Profiles != nil {
		in, out := &in.Profiles, &out.Profiles
		*out = make([]Profile, len(*in))
		for i := range *in {
			if err := Convert_api_Profile_To_v1alpha2_Profile(&(*in)[i], &(*out)[i], s); err != nil {
				return err
			}
		}
	} else {
		out.Profiles = nil
	}
	out.NodeSelector = (*string)(unsafe.Pointer(in.NodeSelector))
	out.MaxNoOfPodsToEvictPerNode = (*uint)(unsafe.Pointer(in.MaxNoOfPodsToEvictPerNode))
	out.MaxNoOfPodsToEvictPerNamespace = (*uint)(unsafe.Pointer(in.MaxNoOfPodsToEvictPerNamespace))
	return nil
}

func autoConvert_v1alpha2_Namespaces_To_api_Namespaces(in *Namespaces, out *api.Namespaces, s conversion.Scope) error {
	out.Include = *(*[]string)(unsafe.Pointer(&in.Include))
	out.Exclude = *(*[]string)(unsafe.Pointer(&in.Exclude))
	return nil
}

// Convert_v1alpha2_Namespaces_To_api_Namespaces is an autogenerated conversion function.
func Convert_v1alpha2_Namespaces_To_api_Namespaces(in *Namespaces, out *api.Namespaces, s conversion.Scope) error {
	return autoConvert_v1alpha2_Namespaces_To_api_Namespaces(in, out, s)
}

func autoConvert_api_Namespaces_To_v1alpha2_Namespaces(in *api.Namespaces, out *Namespaces, s conversion.Scope) error {
	out.Include = *(*[]string)(unsafe.Pointer(&in.Include))
	out.Exclude = *(*[]string)(unsafe.Pointer(&in.Exclude))
	return nil
}

// Convert_api_Namespaces_To_v1alpha2_Namespaces is an autogenerated conversion function.
func Convert_api_Namespaces_To_v1alpha2_Namespaces(in *api.Namespaces, out *Namespaces, s conversion.Scope) error {
	return autoConvert_api_Namespaces_To_v1alpha2_Namespaces(in, out, s)
}

func autoConvert_v1alpha2_PluginConfig_To_api_PluginConfig(in *PluginConfig, out *api.PluginConfig, s conversion.Scope) error {
	out.Name = in.Name
	if err := runtime.Convert_runtime_RawExtension_To_runtime_Object(&in.Args, &out.Args, s); err != nil {
		return err
	}
	return nil
}

func autoConvert_api_PluginConfig_To_v1alpha2_PluginConfig(in *api.PluginConfig, out *PluginConfig, s conversion.Scope) error {
	out.Name = in.Name
	if err := runtime.Convert_runtime_Object_To_runtime_RawExtension(&in.Args, &out.Args, s); err != nil {
		return err
	}
	return nil
}

// Convert_api_PluginConfig_To_v1alpha2_PluginConfig is an autogenerated conversion function.
func Convert_api_PluginConfig_To_v1alpha2_PluginConfig(in *api.PluginConfig, out *PluginConfig, s conversion.Scope) error {
	return autoConvert_api_PluginConfig_To_v1alpha2_PluginConfig(in, out, s)
}

func autoConvert_v1alpha2_PluginSet_To_api_PluginSet(in *PluginSet, out *api.PluginSet, s conversion.Scope) error {
	out.Enabled = *(*[]string)(unsafe.Pointer(&in.Enabled))
	out.Disabled = *(*[]string)(unsafe.Pointer(&in.Disabled))
	return nil
}

// Convert_v1alpha2_PluginSet_To_api_PluginSet is an autogenerated conversion function.
func Convert_v1alpha2_PluginSet_To_api_PluginSet(in *PluginSet, out *api.PluginSet, s conversion.Scope) error {
	return autoConvert_v1alpha2_PluginSet_To_api_PluginSet(in, out, s)
}

func autoConvert_api_PluginSet_To_v1alpha2_PluginSet(in *api.PluginSet, out *PluginSet, s conversion.Scope) error {
	out.Enabled = *(*[]string)(unsafe.Pointer(&in.Enabled))
	out.Disabled = *(*[]string)(unsafe.Pointer(&in.Disabled))
	return nil
}

// Convert_api_PluginSet_To_v1alpha2_PluginSet is an autogenerated conversion function.
func Convert_api_PluginSet_To_v1alpha2_PluginSet(in *api.PluginSet, out *PluginSet, s conversion.Scope) error {
	return autoConvert_api_PluginSet_To_v1alpha2_PluginSet(in, out, s)
}

func autoConvert_v1alpha2_Plugins_To_api_Plugins(in *Plugins, out *api.Plugins, s conversion.Scope) error {
	if err := Convert_v1alpha2_PluginSet_To_api_PluginSet(&in.PreSort, &out.PreSort, s); err != nil {
		return err
	}
	if err := Convert_v1alpha2_PluginSet_To_api_PluginSet(&in.Sort, &out.Sort, s); err != nil {
		return err
	}
	if err := Convert_v1alpha2_PluginSet_To_api_PluginSet(&in.Deschedule, &out.Deschedule, s); err != nil {
		return err
	}
	if err := Convert_v1alpha2_PluginSet_To_api_PluginSet(&in.Balance, &out.Balance, s); err != nil {
		return err
	}
	if err := Convert_v1alpha2_PluginSet_To_api_PluginSet(&in.Evict, &out.Evict, s); err != nil {
		return err
	}
	if err := Convert_v1alpha2_PluginSet_To_api_PluginSet(&in.Filter, &out.Filter, s); err != nil {
		return err
	}
	if err := Convert_v1alpha2_PluginSet_To_api_PluginSet(&in.PreEvictionFilter, &out.PreEvictionFilter, s); err != nil {
		return err
	}
	return nil
}

// Convert_v1alpha2_Plugins_To_api_Plugins is an autogenerated conversion function.
func Convert_v1alpha2_Plugins_To_api_Plugins(in *Plugins, out *api.Plugins, s conversion.Scope) error {
	return autoConvert_v1alpha2_Plugins_To_api_Plugins(in, out, s)
}

func autoConvert_api_Plugins_To_v1alpha2_Plugins(in *api.Plugins, out *Plugins, s conversion.Scope) error {
	if err := Convert_api_PluginSet_To_v1alpha2_PluginSet(&in.PreSort, &out.PreSort, s); err != nil {
		return err
	}
	if err := Convert_api_PluginSet_To_v1alpha2_PluginSet(&in.Sort, &out.Sort, s); err != nil {
		return err
	}
	if err := Convert_api_PluginSet_To_v1alpha2_PluginSet(&in.Deschedule, &out.Deschedule, s); err != nil {
		return err
	}
	if err := Convert_api_PluginSet_To_v1alpha2_PluginSet(&in.Balance, &out.Balance, s); err != nil {
		return err
	}
	if err := Convert_api_PluginSet_To_v1alpha2_PluginSet(&in.Evict, &out.Evict, s); err != nil {
		return err
	}
	if err := Convert_api_PluginSet_To_v1alpha2_PluginSet(&in.Filter, &out.Filter, s); err != nil {
		return err
	}
	if err := Convert_api_PluginSet_To_v1alpha2_PluginSet(&in.PreEvictionFilter, &out.PreEvictionFilter, s); err != nil {
		return err
	}
	return nil
}

// Convert_api_Plugins_To_v1alpha2_Plugins is an autogenerated conversion function.
func Convert_api_Plugins_To_v1alpha2_Plugins(in *api.Plugins, out *Plugins, s conversion.Scope) error {
	return autoConvert_api_Plugins_To_v1alpha2_Plugins(in, out, s)
}

func autoConvert_v1alpha2_Profile_To_api_Profile(in *Profile, out *api.Profile, s conversion.Scope) error {
	out.Name = in.Name
	if in.PluginConfig != nil {
		in, out := &in.PluginConfig, &out.PluginConfig
		*out = make([]api.PluginConfig, len(*in))
		for i := range *in {
			if err := Convert_v1alpha2_PluginConfig_To_api_PluginConfig(&(*in)[i], &(*out)[i], s); err != nil {
				return err
			}
		}
	} else {
		out.PluginConfig = nil
	}
	if err := Convert_v1alpha2_Plugins_To_api_Plugins(&in.Plugins, &out.Plugins, s); err != nil {
		return err
	}
	return nil
}

// Convert_v1alpha2_Profile_To_api_Profile is an autogenerated conversion function.
func Convert_v1alpha2_Profile_To_api_Profile(in *Profile, out *api.Profile, s conversion.Scope) error {
	return autoConvert_v1alpha2_Profile_To_api_Profile(in, out, s)
}

func autoConvert_api_Profile_To_v1alpha2_Profile(in *api.Profile, out *Profile, s conversion.Scope) error {
	out.Name = in.Name
	if in.PluginConfig != nil {
		in, out := &in.PluginConfig, &out.PluginConfig
		*out = make([]PluginConfig, len(*in))
		for i := range *in {
			if err := Convert_api_PluginConfig_To_v1alpha2_PluginConfig(&(*in)[i], &(*out)[i], s); err != nil {
				return err
			}
		}
	} else {
		out.PluginConfig = nil
	}
	if err := Convert_api_Plugins_To_v1alpha2_Plugins(&in.Plugins, &out.Plugins, s); err != nil {
		return err
	}
	return nil
}

// Convert_api_Profile_To_v1alpha2_Profile is an autogenerated conversion function.
func Convert_api_Profile_To_v1alpha2_Profile(in *api.Profile, out *Profile, s conversion.Scope) error {
	return autoConvert_api_Profile_To_v1alpha2_Profile(in, out, s)
}
