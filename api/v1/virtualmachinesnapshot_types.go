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
	"k8s.io/apimachinery/pkg/types"
	"time"
)

const DefaultFailureDeadline = 15 * time.Minute

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// DeletionPolicy defines that to do with VirtualMachineSnapshot
// when VirtualMachineSnapshot is deleted
type DeletionPolicy string

const (
	// VirtualMachineSnapshotContentDelete causes the
	// VirtualMachineSnapshotContent to be deleted
	VirtualMachineSnapshotContentDelete DeletionPolicy = "Delete"

	// VirtualMachineSnapshotContentRetain causes the
	// VirtualMachineSnapshotContent to stay around
	VirtualMachineSnapshotContentRetain DeletionPolicy = "Retain"
)

// VirtualMachineSnapshotSpec defines the desired state of VirtualMachineSnapshot
type VirtualMachineSnapshotSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file
	Source corev1.TypedLocalObjectReference `json:"source"`

	// +optional
	DeletionPolicy *DeletionPolicy `json:"deletionPolicy,omitempty"`

	// This time represents the number of seconds we permit the vm snapshot
	// to take. In case we pass this deadline we mark this snapshot
	// as failed.
	// Defaults to DefaultFailureDeadline - 15min
	// +optional
	FailureDeadline *metav1.Duration `json:"failureDeadline,omitempty"`
}

// VirtualMachineSnapshotStatus defines the observed state of VirtualMachineSnapshot
type VirtualMachineSnapshotStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// +optional
	SourceUID *types.UID `json:"sourceUID,omitempty"`

	// +optional
	VirtualMachineSnapshotContentName *string `json:"virtualMachineSnapshotContentName,omitempty"`

	// +optional
	// +nullable
	CreationTime *metav1.Time `json:"creationTime,omitempty"`

	// +optional
	Phase VirtualMachineSnapshotPhase `json:"phase,omitempty"`

	// +optional
	ReadyToUse *bool `json:"readyToUse,omitempty"`

	// +optional
	Error *Error `json:"error,omitempty"`

	// +optional
	Conditions []Condition `json:"conditions,omitempty"`
	//
	//// +optional
	//// +listType=set
	//Indications []Indication `json:"indications,omitempty"`

	// +optional
	SnapshotVolumes *SnapshotVolumesLists `json:"snapshotVolumes,omitempty"`
}

// SnapshotVolumesLists includes the list of volumes which were included in the snapshot and volumes which were excluded from the snapshot
type SnapshotVolumesLists struct {
	// +optional
	// +listType=set
	IncludedVolumes []string `json:"includedVolumes,omitempty"`

	// +optional
	// +listType=set
	ExcludedVolumes []string `json:"excludedVolumes,omitempty"`
}

// ConditionType is the const type for Conditions
type ConditionType string

const (
	// ConditionReady is the "ready" condition type
	ConditionReady ConditionType = "Ready"

	// ConditionProgressing is the "progressing" condition type
	ConditionProgressing ConditionType = "Progressing"

	// ConditionFailure is the "failure" condition type
	ConditionFailure ConditionType = "Failure"
)

// Condition defines conditions
type Condition struct {
	Type ConditionType `json:"type"`

	Status corev1.ConditionStatus `json:"status"`

	// +optional
	// +nullable
	LastProbeTime metav1.Time `json:"lastProbeTime,omitempty"`

	// +optional
	// +nullable
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty"`

	// +optional
	Reason string `json:"reason,omitempty"`

	// +optional
	Message string `json:"message,omitempty"`
}

// VirtualMachineSnapshotPhase is the current phase of the VirtualMachineSnapshot
type VirtualMachineSnapshotPhase string

const (
	PhaseUnset VirtualMachineSnapshotPhase = ""
	InProgress VirtualMachineSnapshotPhase = "InProgress"
	Succeeded  VirtualMachineSnapshotPhase = "Succeeded"
	Failed     VirtualMachineSnapshotPhase = "Failed"
	Deleting   VirtualMachineSnapshotPhase = "Deleting"
	Unknown    VirtualMachineSnapshotPhase = "Unknown"
)

// Error is the last error encountered during the snapshot/restore
type Error struct {
	// +optional
	Time *metav1.Time `json:"time,omitempty"`

	// +optional
	Message *string `json:"message,omitempty"`
}

type PersistentVolumeClaim struct {
	// Standard object's metadata.
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +kubebuilder:pruning:PreserveUnknownFields
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// Spec defines the desired characteristics of a volume requested by a pod author.
	// More info: https://kubernetes.io/docs/concepts/storage/persistent-volumes#persistentvolumeclaims
	// +optional
	Spec corev1.PersistentVolumeClaimSpec `json:"spec,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+kubebuilder:resource:shortName=hvms
//+kubebuilder:printcolumn:name="AGE",type="date",JSONPath=".metadata.creationTimestamp"
//+kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase"
//+kubebuilder:printcolumn:name="SourceKind",type="string",JSONPath=".spec.source.kind"
//+kubebuilder:printcolumn:name="SourceName",type="string",JSONPath=".spec.source.name"
//+kubebuilder:printcolumn:name="ReadyToUse",type="boolean",JSONPath=".status.readyToUse"
//+kubebuilder:printcolumn:name="creationTime",type="string",JSONPath=".status.creationTime"

// VirtualMachineSnapshot is the Schema for the virtualmachinesnapshots API
type VirtualMachineSnapshot struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   VirtualMachineSnapshotSpec    `json:"spec,omitempty"`
	Status *VirtualMachineSnapshotStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// VirtualMachineSnapshotList contains a list of VirtualMachineSnapshot
type VirtualMachineSnapshotList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []VirtualMachineSnapshot `json:"items"`
}

func init() {
	SchemeBuilder.Register(&VirtualMachineSnapshot{}, &VirtualMachineSnapshotList{})
}
