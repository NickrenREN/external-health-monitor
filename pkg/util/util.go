/*
Copyright 2020 The Kubernetes Authors.

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

package util

import (
	"fmt"
	"path"

	v1 "k8s.io/api/core/v1"

	utilstrings "k8s.io/utils/strings"
	"path/filepath"
)

const (
	VolumeHealthy = "HEALTHY"
)

const (
	CSIPluginName                = "kubernetes.io/csi"
	DefaultKubeletPluginsDirName = "plugins"
	persistentVolumeInGlobalPath = "pv"
	globalMountInGlobalPath      = "globalmount"
	DefaultKubeletPodsDirName    = "pods"
	DefaultKubeletVolumesDirName = "volumes"
)

func MakeDeviceMountPath(kubeletRootDir string, pv *v1.PersistentVolume) (string, error) {
	if pv.Name == "" {
		return "", fmt.Errorf("makeDeviceMountPath failed, pv name empty")
	}

	pluginsDir := path.Join(kubeletRootDir, DefaultKubeletPluginsDirName)
	csiPluginDir := path.Join(pluginsDir, CSIPluginName)

	return path.Join(csiPluginDir, persistentVolumeInGlobalPath, pv.Name, globalMountInGlobalPath), nil
}

func GetVolumePath(kubeletRootDir, pvName, podUID string) string {
	volID := utilstrings.EscapeQualifiedName(pvName)

	podsDir := path.Join(kubeletRootDir, DefaultKubeletPodsDirName)
	podDir := path.Join(podsDir, podUID)
	podVolumesDir := path.Join(podDir, DefaultKubeletVolumesDirName)
	podVolumeDir := filepath.Join(podVolumesDir, utilstrings.EscapeQualifiedName(CSIPluginName), volID)

	return path.Join(podVolumeDir, "/mount")
}
