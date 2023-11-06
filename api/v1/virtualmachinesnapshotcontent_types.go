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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubevirtv1 "kubevirt.io/api/core/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// VirtualMachineSnapshotContentSpec defines the desired state of VirtualMachineSnapshotContent
type VirtualMachineSnapshotContentSpec struct {
	VirtualMachineSnapshotName *string `json:"virtualMachineSnapshotName,omitempty"`

	Source SourceSpec `json:"source"`

	// +optional
	VolumeBackups []VolumeBackup `json:"volumeBackups,omitempty"`
}

// VolumeBackup contains the data neeed to restore a PVC
type VolumeBackup struct {
	VolumeName string `json:"volumeName"`

	PersistentVolumeClaim PersistentVolumeClaim `json:"persistentVolumeClaim"`

	// +optional
	VolumeSnapshotName *string `json:"volumeSnapshotName,omitempty"`
}

// SourceSpec contains the appropriate spec for the resource being snapshotted
type SourceSpec struct {
	// +optional
	VirtualMachine *kubevirtv1.VirtualMachine `json:"virtualMachine,omitempty"`
}

// VirtualMachineSnapshotContentStatus defines the observed state of VirtualMachineSnapshotContent
type VirtualMachineSnapshotContentStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file
	// +optional
	// +nullable
	CreationTime *metav1.Time `json:"creationTime,omitempty"`

	// +optional
	ReadyToUse *bool `json:"readyToUse,omitempty"`

	// +optional
	Error *Error `json:"error,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

// VirtualMachineSnapshotContent is the Schema for the virtualmachinesnapshotcontents API
type VirtualMachineSnapshotContent struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   VirtualMachineSnapshotContentSpec   `json:"spec,omitempty"`
	Status VirtualMachineSnapshotContentStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// VirtualMachineSnapshotContentList contains a list of VirtualMachineSnapshotContent
type VirtualMachineSnapshotContentList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []VirtualMachineSnapshotContent `json:"items"`
}

func init() {
	SchemeBuilder.Register(&VirtualMachineSnapshotContent{}, &VirtualMachineSnapshotContentList{})
}
