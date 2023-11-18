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

package controller

import (
	"context"
	"fmt"
	hitoseacomv1 "github.com/am6737/histore/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	vmSnapshotFinalizer        = "snapshot.hitosea.com/vmsnapshot-protection"
	vmSnapshotContentFinalizer = "snapshot.hitosea.com/vmsnapshotcontent-protection"
)

func RemoveFinalizer(object metav1.Object, finalizer string) {
	filtered := []string{}
	for _, f := range object.GetFinalizers() {
		if f != finalizer {
			filtered = append(filtered, f)
		}
	}
	object.SetFinalizers(filtered)
}

func AddFinalizer(object metav1.Object, finalizer string) {
	if HasFinalizer(object, finalizer) {
		return
	}
	object.SetFinalizers(append(object.GetFinalizers(), finalizer))
}

func HasFinalizer(object metav1.Object, finalizer string) bool {
	for _, f := range object.GetFinalizers() {
		if f == finalizer {
			return true
		}
	}
	return false
}

// addFinalizerToVR adds the VR finalizer on the VolumeReplication instance.
func (r *VirtualMachineSnapshotReconciler) addFinalizerToVms(vms *hitoseacomv1.VirtualMachineSnapshot,
) error {
	if !contains(vms.ObjectMeta.Finalizers, vmSnapshotFinalizer) {
		r.Log.Info("adding finalizer to VirtualMachineSnapshot object", "Finalizer", vmSnapshotFinalizer)
		vms.ObjectMeta.Finalizers = append(vms.ObjectMeta.Finalizers, vmSnapshotFinalizer)
		if err := r.Client.Update(context.TODO(), vms); err != nil {
			return fmt.Errorf("failed to add finalizer (%s) to VirtualMachineSnapshot resource"+
				" (%s/%s) %w",
				vmSnapshotFinalizer, vms.Namespace, vms.Name, err)
		}
	}

	return nil
}

// removeFinalizerFromVR removes the VR finalizer from the VolumeReplication instance.
func (r *VirtualMachineSnapshotReconciler) removeFinalizerFromVms(vms *hitoseacomv1.VirtualMachineSnapshot) error {
	if contains(vms.ObjectMeta.Finalizers, vmSnapshotFinalizer) {
		r.Log.Info("removing finalizer from VirtualMachineSnapshot object", "Finalizer", vmSnapshotFinalizer)
		vms.ObjectMeta.Finalizers = remove(vms.ObjectMeta.Finalizers, vmSnapshotFinalizer)
		if err := r.Client.Update(context.TODO(), vms); err != nil {
			return fmt.Errorf("failed to remove finalizer (%s) from VirtualMachineSnapshot resource"+
				" (%s/%s), %w",
				vmSnapshotFinalizer, vms.Namespace, vms.Name, err)
		}
	}

	return nil
}

// addFinalizerToPVC adds the VR finalizer on the PersistentVolumeClaim.
func (r *VirtualMachineSnapshotContentReconciler) addFinalizerToVmsc(vmsc *hitoseacomv1.VirtualMachineSnapshotContent) error {
	if !contains(vmsc.ObjectMeta.Finalizers, vmSnapshotContentFinalizer) {
		r.Log.Info("adding finalizer to VirtualMachineSnapshotContent object", "Finalizer", vmSnapshotContentFinalizer)
		vmsc.ObjectMeta.Finalizers = append(vmsc.ObjectMeta.Finalizers, vmSnapshotContentFinalizer)
		if err := r.Client.Update(context.TODO(), vmsc); err != nil {
			return fmt.Errorf("failed to add finalizer (%s) to VirtualMachineSnapshotContent resource"+
				" (%s/%s) %w",
				vmSnapshotContentFinalizer, vmsc.Namespace, vmsc.Name, err)
		}
	}

	return nil
}

// removeFinalizerFromPVC removes the VR finalizer on PersistentVolumeClaim.
func (r *VirtualMachineSnapshotContentReconciler) removeFinalizerFromVmsc(vmsc *hitoseacomv1.VirtualMachineSnapshotContent,
) error {
	if contains(vmsc.ObjectMeta.Finalizers, vmSnapshotContentFinalizer) {
		r.Log.Info("removing finalizer from VirtualMachineSnapshotContent object", "Finalizer", vmSnapshotContentFinalizer)
		vmsc.ObjectMeta.Finalizers = remove(vmsc.ObjectMeta.Finalizers, vmSnapshotContentFinalizer)
		if err := r.Client.Update(context.TODO(), vmsc); err != nil {
			return fmt.Errorf("failed to remove finalizer (%s) from VirtualMachineSnapshotContent resource"+
				" (%s/%s), %w",
				vmSnapshotContentFinalizer, vmsc.Namespace, vmsc.Name, err)
		}
	}

	return nil
}

// Checks whether a string is contained within a slice.
func contains(slice []string, s string) bool {
	for _, item := range slice {
		if item == s {
			return true
		}
	}

	return false
}

// Removes a given string from a slice and returns the new slice.
func remove(slice []string, s string) (result []string) {
	for _, item := range slice {
		if item == s {
			continue
		}
		result = append(result, item)
	}

	return
}
