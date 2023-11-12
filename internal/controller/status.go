package controller

import (
	hitoseacomv1 "github.com/am6737/histore/api/v1"
	corev1 "k8s.io/api/core/v1"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	ConditionCompleted = "Completed"
)

const (
	Success          = "Success"
	Enabled          = "Enabled"
	Disabled         = "Disabled"
	Promoted         = "Promoted"
	Demoted          = "Demoted"
	FailedToEnabled  = "FailedToEnabled"
	FailedToDisabled = "FailedToDisabled"
	FailedToPromote  = "FailedToPromote"
	FailedToDemote   = "FailedToDemote"
	Error            = "Error"
)

// sets conditions when volume was promoted successfully.
func setEnabledCondition(conditions *[]metav1.Condition, observedGeneration int64) {
	setStatusCondition(conditions, &metav1.Condition{
		Type:               ConditionCompleted,
		Reason:             Enabled,
		ObservedGeneration: observedGeneration,
		Status:             metav1.ConditionTrue,
	})
}

// sets conditions when volume promotion was failed.
func setFailedEnabledCondition(conditions *[]metav1.Condition, observedGeneration int64) {
	setStatusCondition(conditions, &metav1.Condition{
		Type:               ConditionCompleted,
		Reason:             FailedToEnabled,
		ObservedGeneration: observedGeneration,
		Status:             metav1.ConditionFalse,
	})
}

// sets conditions when volume was promoted successfully.
func setDisabledCondition(conditions *[]metav1.Condition, observedGeneration int64) {
	setStatusCondition(conditions, &metav1.Condition{
		Type:               ConditionCompleted,
		Reason:             Disabled,
		ObservedGeneration: observedGeneration,
		Status:             metav1.ConditionTrue,
	})
}

// sets conditions when volume promotion was failed.
func setFailedDisabledCondition(conditions *[]metav1.Condition, observedGeneration int64) {
	setStatusCondition(conditions, &metav1.Condition{
		Type:               ConditionCompleted,
		Reason:             FailedToDisabled,
		ObservedGeneration: observedGeneration,
		Status:             metav1.ConditionFalse,
	})
}

// sets conditions when volume was promoted successfully.
func setPromotedCondition(conditions *[]metav1.Condition, observedGeneration int64) {
	setStatusCondition(conditions, &metav1.Condition{
		Type:               ConditionCompleted,
		Reason:             Promoted,
		ObservedGeneration: observedGeneration,
		Status:             metav1.ConditionTrue,
	})
}

// sets conditions when volume promotion was failed.
func setFailedPromotionCondition(conditions *[]metav1.Condition, observedGeneration int64) {
	setStatusCondition(conditions, &metav1.Condition{
		Type:               ConditionCompleted,
		Reason:             FailedToPromote,
		ObservedGeneration: observedGeneration,
		Status:             metav1.ConditionFalse,
	})
}

// sets conditions when volume was demoted successfully.
func setDemotedCondition(conditions *[]metav1.Condition, observedGeneration int64) {
	setStatusCondition(conditions, &metav1.Condition{
		Type:               ConditionCompleted,
		Reason:             Demoted,
		ObservedGeneration: observedGeneration,
		Status:             metav1.ConditionTrue,
	})
}

// sets conditions when volume demotion was failed.
func setFailedDemotionCondition(conditions *[]metav1.Condition, observedGeneration int64) {
	setStatusCondition(conditions, &metav1.Condition{
		Type:               ConditionCompleted,
		Reason:             FailedToDemote,
		ObservedGeneration: observedGeneration,
		Status:             metav1.ConditionFalse,
	})
}

// InitializeConditions initializes all possible conditions with a given ObservedGeneration.
func InitializeConditions(observedGeneration int64) []metav1.Condition {
	return []metav1.Condition{
		createCondition(Enabled, ConditionCompleted, metav1.ConditionTrue, observedGeneration),
		createCondition(FailedToEnabled, ConditionCompleted, metav1.ConditionFalse, observedGeneration),
		createCondition(Disabled, ConditionCompleted, metav1.ConditionTrue, observedGeneration),
		createCondition(FailedToDisabled, ConditionCompleted, metav1.ConditionFalse, observedGeneration),
		createCondition(Promoted, ConditionCompleted, metav1.ConditionTrue, observedGeneration),
		createCondition(FailedToPromote, ConditionCompleted, metav1.ConditionFalse, observedGeneration),
		createCondition(Demoted, ConditionCompleted, metav1.ConditionTrue, observedGeneration),
		createCondition(FailedToDemote, ConditionCompleted, metav1.ConditionFalse, observedGeneration),
	}
}

func createCondition(reason, conditionType string, status metav1.ConditionStatus, observedGeneration int64) metav1.Condition {
	return metav1.Condition{
		Type:               conditionType,
		Reason:             reason,
		ObservedGeneration: observedGeneration,
		Status:             status,
		LastTransitionTime: metav1.NewTime(time.Now()),
	}
}

func setStatusCondition(existingConditions *[]metav1.Condition, newCondition *metav1.Condition) {
	if existingConditions == nil {
		existingConditions = &[]metav1.Condition{}
	}

	existingCondition := findCondition(*existingConditions, newCondition.Type)
	if existingCondition == nil {
		newCondition.LastTransitionTime = metav1.NewTime(time.Now())
		*existingConditions = append(*existingConditions, *newCondition)

		return
	}

	if existingCondition.Status != newCondition.Status {
		existingCondition.Status = newCondition.Status
		existingCondition.LastTransitionTime = metav1.NewTime(time.Now())
	}

	existingCondition.Reason = newCondition.Reason
	existingCondition.ObservedGeneration = newCondition.ObservedGeneration
}

func findCondition(existingConditions []metav1.Condition, conditionType string) *metav1.Condition {
	for i := range existingConditions {
		if existingConditions[i].Type == conditionType {
			return &existingConditions[i]
		}
	}

	return nil
}

func newProgressingCondition(status corev1.ConditionStatus, reason string) hitoseacomv1.Condition {
	return hitoseacomv1.Condition{
		Type:               hitoseacomv1.ConditionProgressing,
		Status:             status,
		Reason:             reason,
		LastTransitionTime: *currentTime(),
	}
}

func newFailureCondition(status corev1.ConditionStatus, reason string) hitoseacomv1.Condition {
	return hitoseacomv1.Condition{
		Type:               hitoseacomv1.ConditionFailure,
		Status:             status,
		Reason:             reason,
		LastTransitionTime: *currentTime(),
	}
}

func newReadyCondition(status corev1.ConditionStatus, reason string) hitoseacomv1.Condition {
	return hitoseacomv1.Condition{
		Type:               hitoseacomv1.ConditionReady,
		Status:             status,
		Reason:             reason,
		LastTransitionTime: *currentTime(),
	}
}

func updateSnapshotSnapshotableVolumes(snapshot *hitoseacomv1.VirtualMachineSnapshot, content *hitoseacomv1.VirtualMachineSnapshotContent) {
	if content == nil {
		return
	}
	vm := content.Spec.Source.VirtualMachine
	if vm == nil || vm.Spec.Template == nil {
		return
	}
	volumes := vm.Spec.Template.Spec.Volumes

	volumeBackups := make(map[string]bool)
	for _, volumeBackup := range content.Spec.VolumeBackups {
		volumeBackups[volumeBackup.VolumeName] = true
	}

	var excludedVolumes []string
	var includedVolumes []string
	for _, volume := range volumes {
		if _, ok := volumeBackups[volume.Name]; ok {
			includedVolumes = append(includedVolumes, volume.Name)
		} else {
			excludedVolumes = append(excludedVolumes, volume.Name)
		}
	}
	snapshot.Status.SnapshotVolumes = &hitoseacomv1.SnapshotVolumesLists{
		IncludedVolumes: includedVolumes,
		ExcludedVolumes: excludedVolumes,
	}
}

func updateSnapshotCondition(ss *hitoseacomv1.VirtualMachineSnapshot, c hitoseacomv1.Condition) {
	ss.Status.Conditions = updateCondition(ss.Status.Conditions, c, false)
}

func updateCondition(conditions []hitoseacomv1.Condition, c hitoseacomv1.Condition, includeReason bool) []hitoseacomv1.Condition {
	found := false
	for i := range conditions {
		if conditions[i].Type == c.Type {
			if conditions[i].Status != c.Status || (includeReason && conditions[i].Reason != c.Reason) {
				conditions[i] = c
			}
			found = true
			break
		}
	}
	if !found {
		conditions = append(conditions, c)
	}
	return conditions
}
