/*
Copyright 2023.

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

package v1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// VirtualMachineRestoreSpec defines the desired state of VirtualMachineRestore
type VirtualMachineRestoreSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// initially only VirtualMachine type supported
	Target corev1.TypedLocalObjectReference `json:"target"`

	VirtualMachineSnapshotName string `json:"virtualMachineSnapshotName"`
}

// VirtualMachineRestoreStatus defines the observed state of VirtualMachineRestore
type VirtualMachineRestoreStatus struct {
	// +optional
	Restores []VolumeRestore `json:"restores,omitempty"`

	// +optional
	RestoreTime *metav1.Time `json:"restoreTime,omitempty"`

	// +optional
	DeletedDataVolumes []string `json:"deletedDataVolumes,omitempty"`

	// +optional
	Complete *bool `json:"complete,omitempty"`

	// +optional
	Conditions []Condition `json:"conditions,omitempty"`

	// +optional
	Error *Error `json:"error,omitempty"`
}

// VolumeRestore contains the data neeed to restore a PVC
type VolumeRestore struct {
	VolumeName string `json:"volumeName"`

	PersistentVolumeClaimName string `json:"persistentVolumeClaim"`

	VolumeSnapshotName string `json:"volumeSnapshotName"`

	// +optional
	DataVolumeName *string `json:"dataVolumeName,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+kubebuilder:printcolumn:name="TargetName",type="string",JSONPath=".spec.target.name"
//+kubebuilder:printcolumn:name="Complete",type="boolean",JSONPath=".status.complete"
//+kubebuilder:printcolumn:name="Complete",type="string",JSONPath=".status.restoreTime"
//+kubebuilder:printcolumn:name="Error",type="string",JSONPath=".status.error"

// VirtualMachineRestore is the Schema for the virtualmachinerestores API
type VirtualMachineRestore struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   VirtualMachineRestoreSpec    `json:"spec,omitempty"`
	Status *VirtualMachineRestoreStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// VirtualMachineRestoreList contains a list of VirtualMachineRestore
type VirtualMachineRestoreList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []VirtualMachineRestore `json:"items"`
}

func init() {
	SchemeBuilder.Register(&VirtualMachineRestore{}, &VirtualMachineRestoreList{})
}
