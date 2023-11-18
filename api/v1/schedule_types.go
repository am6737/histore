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
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

type BackupSpec struct {
	// IncludedNamespaces is a slice of namespace names to include objects
	// from. If empty, all namespaces are included.
	// +optional
	// +nullable
	IncludedNamespaces []string `json:"includedNamespaces,omitempty"`

	// ExcludedNamespaces contains a list of namespaces that are not
	// included in the backup.
	// +optional
	// +nullable
	ExcludedNamespaces []string `json:"excludedNamespaces,omitempty"`

	// LabelSelector is a metav1.labelSelector to filter with
	// when adding individual objects to the backup. If empty
	// or nil, all objects are included. Optional.
	// +optional
	// +nullable
	LabelSelector *metav1.LabelSelector `json:"labelSelector,omitempty"`

	// OrLabelSelectors is list of metav1.labelSelector to filter with
	// when adding individual objects to the backup. If multiple provided
	// they will be joined by the OR operator. LabelSelector as well as
	// OrLabelSelectors cannot co-exist in backup request, only one of them
	// can be used.
	// +optional
	// +nullable
	OrLabelSelectors []*metav1.LabelSelector `json:"orLabelSelectors,omitempty"`

	// TTL is a time.Duration-parseable string describing how long
	// Snapshot lifecycle
	// +optional
	TTL metav1.Duration `json:"ttl,omitempty"`
}

// ScheduleSpec defines the desired state of Schedule
type ScheduleSpec struct {
	// Template is the definition of the Backup to be run
	// on the provided schedule
	// +optional
	Template BackupSpec `json:"template"`

	// Schedule is a time.Duration-parseable string describing how long
	// the Backup.
	// +optional
	Schedule string `json:"schedule"`

	// Paused specifies whether the schedule is paused or not
	// +optional
	Paused bool `json:"paused,omitempty"`
}

// SchedulePhase is a string representation of the lifecycle phase
// +kubebuilder:validation:Enum=Pause;Enabled;FailedValidation
type SchedulePhase string

const (
	SchedulePhasePause SchedulePhase = "Pause"

	SchedulePhaseEnabled SchedulePhase = "Enabled"

	SchedulePhaseFailedValidation SchedulePhase = "FailedValidation"
)

// ScheduleStatus defines the observed state of Schedule
type ScheduleStatus struct {
	// Phase is the current phase of the Schedule
	// +optional
	Phase SchedulePhase `json:"phase,omitempty"`

	// Schedule is a time.Duration-parseable string describing how long
	// the Backup.
	// +optional
	Schedule string `json:"schedule"`

	// TTL is a time.Duration-parseable string describing how long
	// Snapshot lifecycle
	// +optional
	TTL metav1.Duration `json:"ttl,omitempty"`

	// LastBackup is the last time a Backup was run for this
	// +optional
	// +nullable
	LastBackup *metav1.Time `json:"lastBackup,omitempty"`

	// NextBackup is the next time a Backup was run for this
	// +optional
	// +nullable
	NextBackup *metav1.Time `json:"nextBackup,omitempty"`

	// LastDelete is the last time a Delete was run for this
	// +optional
	// +nullable
	LastDelete *metav1.Time `json:"lastDelete,omitempty"`

	// NextDelete is the next time a Delete was run for this
	// +optional
	// +nullable
	NextDelete *metav1.Time `json:"nextDelete,omitempty"`

	// ValidationErrors is a slice of all validation errors (if
	// applicable)
	// +optional
	ValidationErrors []string `json:"validationErrors,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

// Schedule is the Schema for the schedules API
type Schedule struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ScheduleSpec   `json:"spec,omitempty"`
	Status ScheduleStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// ScheduleList contains a list of Schedule
type ScheduleList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Schedule `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Schedule{}, &ScheduleList{})
}
