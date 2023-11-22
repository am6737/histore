/*
Copyright 2021.

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

package config

import (
	"context"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"strings"
)

// CephConfig holds the CSI driver configuration.
type CephConfig struct {
	MasterStorageClass string
	SlaveStorageClass  string
	MasterCephCsiCfg   *CephCsiConfig
	SlaveCephCsiCfg    *CephCsiConfig
}

// NewDriverConfig returns the newly initialized CephConfig.
func NewDriverConfig() *CephConfig {
	return &CephConfig{}
}

// Validate operation configurations.
func (cfg *CephConfig) Validate() error {
	// check driver name is set
	//if cfg.DriverName == "" {
	//	return errors.New("driverName is empty")
	//}

	return nil
}

type CephCsiConfig struct {
	ClusterID                       string
	Driver                          string
	Pool                            string
	ControllerExpandSecretName      string
	ControllerExpandSecretNamespace string
	NodeStageSecretName             string
	NodeStageSecretNamespace        string
	ProvisionerSecretName           string
	ProvisionerSecretNamespace      string
	SecretMap                       map[string]string
	Parameters                      map[string]string
	ImageFeatures                   map[string]string
}

func GetCephCsiConfigForSC(client client.Client, scName string) (*CephCsiConfig, error) {
	sc := &storagev1.StorageClass{}
	if err := client.Get(context.Background(), types.NamespacedName{Name: scName}, sc); err != nil {
		return nil, err
	}
	return NewCephCsiConfig(sc), nil
}

func NewCephCsiConfig(sc *storagev1.StorageClass) *CephCsiConfig {
	config := &CephCsiConfig{
		SecretMap:     map[string]string{},
		Parameters:    map[string]string{},
		ImageFeatures: map[string]string{},
	}
	config.Driver = sc.Provisioner
	if sc.Parameters != nil {
		if clusterID, ok := sc.Parameters["clusterID"]; ok {
			config.ClusterID = clusterID
		}
		if pool, ok := sc.Parameters["pool"]; ok {
			config.Pool = pool
		}
		if controllerExpandSecretName, ok := sc.Parameters["csi.storage.k8s.io/controller-expand-secret-name"]; ok {
			config.ControllerExpandSecretName = controllerExpandSecretName
		}
		if controllerExpandSecretNamespace, ok := sc.Parameters["csi.storage.k8s.io/controller-expand-secret-namespace"]; ok {
			config.ControllerExpandSecretNamespace = controllerExpandSecretNamespace
		}
		if nodeStageSecretName, ok := sc.Parameters["csi.storage.k8s.io/node-stage-secret-name"]; ok {
			config.NodeStageSecretName = nodeStageSecretName
		}
		if nodeStageSecretNamespace, ok := sc.Parameters["csi.storage.k8s.io/node-stage-secret-namespace"]; ok {
			config.NodeStageSecretNamespace = nodeStageSecretNamespace
		}
		if provisionerSecretName, ok := sc.Parameters["csi.storage.k8s.io/provisioner-secret-name"]; ok {
			config.ProvisionerSecretName = provisionerSecretName
		}
		if provisionerSecretNamespace, ok := sc.Parameters["csi.storage.k8s.io/provisioner-secret-namespace"]; ok {
			config.ProvisionerSecretNamespace = provisionerSecretNamespace
		}
		for key, value := range sc.Parameters {
			config.Parameters[key] = value
		}
	}
	// Update config fields based on StorageClass imageFeatures
	if imageFeatures, ok := sc.Parameters["imageFeatures"]; ok {
		// Assuming imageFeatures is a comma-separated list
		features := strings.Split(imageFeatures, ",")
		for _, feature := range features {
			config.ImageFeatures[feature] = feature
		}
	}
	return config
}
