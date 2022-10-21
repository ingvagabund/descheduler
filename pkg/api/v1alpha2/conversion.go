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

package v1alpha2

import (
	"fmt"
	"sync"
	unsafe "unsafe"

	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/klog/v2"
	api "sigs.k8s.io/descheduler/pkg/api"

	"sigs.k8s.io/descheduler/pkg/framework/plugins/defaultevictor"
	"sigs.k8s.io/descheduler/pkg/framework/plugins/removepodshavingtoomanyrestarts"
)

var (
	// pluginArgConversionScheme is a scheme with internal and v1beta2 registered,
	// used for defaulting/converting typed PluginConfig Args.
	// Access via getPluginArgConversionScheme()
	pluginArgConversionScheme     *runtime.Scheme
	initPluginArgConversionScheme sync.Once

	Scheme = runtime.NewScheme()
	Codecs = serializer.NewCodecFactory(Scheme, serializer.EnableStrict)
)

func GetPluginArgConversionScheme() *runtime.Scheme {
	initPluginArgConversionScheme.Do(func() {
		// set up the scheme used for plugin arg conversion
		pluginArgConversionScheme = runtime.NewScheme()
		utilruntime.Must(AddToScheme(pluginArgConversionScheme))
		utilruntime.Must(api.AddToScheme(pluginArgConversionScheme))
	})
	return pluginArgConversionScheme
}

func Convert_v1alpha2_DeschedulerPolicy_To_api_DeschedulerPolicy(in *DeschedulerPolicy, out *api.DeschedulerPolicy, s conversion.Scope) error {
	if err := manualConvert_v1alpha2_DeschedulerPolicy_To_api_DeschedulerPolicy(in, out, s); err != nil {
		return err
	}
	return convertToInternalPluginConfigArgs(out)
}

func manualConvert_v1alpha2_DeschedulerPolicy_To_api_DeschedulerPolicy(in *DeschedulerPolicy, out *api.DeschedulerPolicy, s conversion.Scope) error {
	hasRawData := false
	for _, profile := range in.Profiles {
		newOutProfile := api.Profile{}
		newOutProfile.Name = profile.Name
		newOutPluginconfigs := []api.PluginConfig{}
		for _, pluginConfig := range profile.PluginConfig {
			newOutPluginConfig := api.PluginConfig{
				Name: pluginConfig.Name,
			}
			if pluginConfig.Args.Raw == nil {
				newOutPluginConfig.Args = pluginConfig.Args.Object
			} else {
				hasRawData = true
				break
			}
			newOutPluginconfigs = append(newOutPluginconfigs, newOutPluginConfig)
		}
		if hasRawData {
			break
		}
		newOutProfile.PluginConfig = newOutPluginconfigs
		newOutProfile.Plugins = *(*api.Plugins)(unsafe.Pointer(&profile.Plugins))
		out.Profiles = append(out.Profiles, newOutProfile)
	}
	if hasRawData {
		autoConvert_v1alpha2_DeschedulerPolicy_To_api_DeschedulerPolicy(in, out, s)
	} else {
		out.NodeSelector = (*string)(unsafe.Pointer(in.NodeSelector))
		out.MaxNoOfPodsToEvictPerNode = (*uint)(unsafe.Pointer(in.MaxNoOfPodsToEvictPerNode))
		out.MaxNoOfPodsToEvictPerNamespace = (*uint)(unsafe.Pointer(in.MaxNoOfPodsToEvictPerNamespace))
	}
	return nil
}

// convertToInternalPluginConfigArgs converts PluginConfig#Args into internal
// types using a scheme, after applying defaults.
func convertToInternalPluginConfigArgs(out *api.DeschedulerPolicy) error {
	scheme := GetPluginArgConversionScheme()
	for i := range out.Profiles {
		prof := &out.Profiles[i]
		for j := range prof.PluginConfig {
			args := prof.PluginConfig[j].Args
			if args == nil {
				continue
			}
			// if _, isUnknown := args.(*runtime.Unknown); isUnknown {
			// 	continue
			// }
			internalArgs, err := scheme.ConvertToVersion(args, api.SchemeGroupVersion)
			if err != nil {
				err = nil
				internalArgs = args
				if err != nil {
					return fmt.Errorf("converting .Profiles[%d].PluginConfig[%d].Args into internal type: %w", i, j, err)
				}
			}
			prof.PluginConfig[j].Args = internalArgs
		}
	}
	return nil
}

func Convert_api_DeschedulerPolicy_To_v1alpha2_DeschedulerPolicy(in *api.DeschedulerPolicy, out *DeschedulerPolicy, s conversion.Scope) error {
	if err := autoConvert_api_DeschedulerPolicy_To_v1alpha2_DeschedulerPolicy(in, out, s); err != nil {
		return err
	}
	return convertToExternalPluginConfigArgs(out)
}

func Convert_v1alpha2_PluginConfig_To_api_PluginConfig(in *PluginConfig, out *api.PluginConfig, s conversion.Scope) error {
	klog.Infof("Convert_v1alpha2_PluginConfig_To_api_PluginConfig: %v", in.Name)
	out.Name = in.Name
	switch in.Name {
	case "DefaultEvictor":
		out.Args = &defaultevictor.DefaultEvictorArgs{}
		klog.Infof("&defaultevictor.DefaultEvictorArgs{}")
		klog.Infof("%#v", string(in.Args.Raw))
		_, _, err := Codecs.UniversalDecoder().Decode(in.Args.Raw, nil, out.Args)
		klog.Infof("obj: %#v\nerr: %v\n", out.Args, err)
		klog.Infof("out.Args: %#v\n", out.Args)
		return nil
	case "RemovePodsHavingTooManyRestarts":
		out.Args = &removepodshavingtoomanyrestarts.RemovePodsHavingTooManyRestartsArgs{}
		klog.Infof("&removepodshavingtoomanyrestarts.RemovePodsHavingTooManyRestartsArgs{}")
		klog.Infof("%#v", string(in.Args.Raw))
		_, _, err := Codecs.UniversalDecoder().Decode(in.Args.Raw, nil, out.Args)
		klog.Infof("obj: %#v\nerr: %v\n", out.Args, err)
		klog.Infof("out.Args: %#v\n", out.Args)
		return nil
	default:
		if err := runtime.Convert_runtime_RawExtension_To_runtime_Object(&in.Args, &out.Args, s); err != nil {
			return err
		}
		return nil
	}
}

// convertToExternalPluginConfigArgs converts PluginConfig#Args into
// external (versioned) types using a scheme.
func convertToExternalPluginConfigArgs(out *DeschedulerPolicy) error {
	scheme := GetPluginArgConversionScheme()
	for i := range out.Profiles {
		for j := range out.Profiles[i].PluginConfig {
			args := out.Profiles[i].PluginConfig[j].Args
			if args.Object == nil {
				continue
			}
			if _, isUnknown := args.Object.(*runtime.Unknown); isUnknown {
				continue
			}
			externalArgs, err := scheme.ConvertToVersion(args.Object, SchemeGroupVersion)
			if err != nil {
				return err
			}
			out.Profiles[i].PluginConfig[j].Args.Object = externalArgs
		}
	}
	return nil
}
