package controller

import (
	"context"
	"fmt"
	hitoseacomv1 "github.com/am6737/histore/api/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	kubevirtv1 "kubevirt.io/api/core/v1"
)

const (
	restoreNameAnnotation = "restore.kubevirt.io/name"

	populatedForPVCAnnotation = "cdi.kubevirt.io/storage.populatedFor"

	lastRestoreAnnotation = "restore.kubevirt.io/lastRestoreUID"

	restoreSourceNameLabel = "restore.kubevirt.io/source-vm-name"

	restoreSourceNamespaceLabel = "restore.kubevirt.io/source-vm-namespace"

	restoreCompleteEvent = "VirtualMachineRestoreComplete"

	restoreErrorEvent = "VirtualMachineRestoreError"

	restoreDataVolumeCreateErrorEvent = "RestoreDataVolumeCreateError"
)

type restoreTarget interface {
	Ready() (bool, error)
	Reconcile() (bool, error)
	Cleanup() error
	Own(obj metav1.Object)
	UpdateDoneRestore() (bool, error)
	UpdateRestoreInProgress() error
	UpdateTarget(obj metav1.Object)
}

type vmRestoreTarget struct {
	vmRestore *hitoseacomv1.VirtualMachineRestore
	vm        *kubevirtv1.VirtualMachine
}

func (r *VirtualMachineRestoreReconciler) getTarget(vmRestore *hitoseacomv1.VirtualMachineRestore) (restoreTarget, error) {
	vmRestore.Spec.Target.DeepCopy()
	switch vmRestore.Spec.Target.Kind {
	case "VirtualMachine":
		vm, err := r.getVM(vmRestore.Namespace, vmRestore.Spec.Target.Name)
		if err != nil {
			return nil, err
		}

		return &vmRestoreTarget{
			vmRestore: vmRestore,
			vm:        vm,
		}, nil
	}

	return nil, fmt.Errorf("unknown source %+v", vmRestore.Spec.Target)
}

func (r *VirtualMachineRestoreReconciler) getVM(namespace, name string) (*kubevirtv1.VirtualMachine, error) {
	vm := &kubevirtv1.VirtualMachine{}
	if err := r.Client.Get(context.TODO(), types.NamespacedName{
		Namespace: namespace,
		Name:      name,
	}, vm); err != nil {
		if apierrors.IsNotFound(err) {
			return vm, nil
		}
		return vm, err
	}

	return vm.DeepCopy(), nil
}

func (v *vmRestoreTarget) Ready() (bool, error) {
	if !v.doesTargetVMExist() {
		return true, nil
	}

	//log.Log.Object(v.vmRestore).V(3).Info("Checking VM ready")
	//
	//rs, err := v.vm.RunStrategy()
	//if err != nil {
	//	return false, err
	//}
	//
	//if rs != kubevirtv1.RunStrategyHalted {
	//	return false, fmt.Errorf("invalid RunStrategy %q", rs)
	//}
	//
	//vmiKey, err := controller.KeyFunc(t.vm)
	//if err != nil {
	//	return false, err
	//}
	//
	//_, exists, err := t.controller.VMIInformer.GetStore().GetByKey(vmiKey)
	//if err != nil {
	//	return false, err
	//}

	return false, nil
}

func (v *vmRestoreTarget) Reconcile() (bool, error) {
	//TODO implement me
	panic("implement me")
}

func (v *vmRestoreTarget) Cleanup() error {
	//TODO implement me
	panic("implement me")
}

func (v *vmRestoreTarget) Own(obj metav1.Object) {
	if !v.doesTargetVMExist() {
		return
	}
	b := true
	obj.SetOwnerReferences([]metav1.OwnerReference{
		{
			APIVersion:         kubevirtv1.GroupVersion.String(),
			Kind:               "VirtualMachine",
			Name:               v.vm.Name,
			UID:                v.vm.UID,
			Controller:         &b,
			BlockOwnerDeletion: &b,
		},
	})
}

func (v *vmRestoreTarget) doesTargetVMExist() bool {
	return v.vm != nil
}

func (v *vmRestoreTarget) UpdateDoneRestore() (bool, error) {
	//TODO implement me
	panic("implement me")
}

func (v *vmRestoreTarget) UpdateRestoreInProgress() error {
	if !v.doesTargetVMExist() || hasLastRestoreAnnotation(v.vmRestore, v.vm) {
		return nil
	}

	if v.vm.Status.RestoreInProgress != nil && *v.vm.Status.RestoreInProgress != v.vmRestore.Name {
		return fmt.Errorf("vm restore %s in progress", *v.vm.Status.RestoreInProgress)
	}

	vmCopy := v.vm.DeepCopy()

	if vmCopy.Status.RestoreInProgress == nil {
		vmCopy.Status.RestoreInProgress = &v.vmRestore.Name

		// unfortunately, status Updater does not return the updated resource
		// but the controller is watching VMs so will get notified
		//return v.controller.vmStatusUpdater.UpdateStatus(vmCopy)
	}

	return nil
}

func hasLastRestoreAnnotation(restore *hitoseacomv1.VirtualMachineRestore, obj metav1.Object) bool {
	return obj.GetAnnotations()[lastRestoreAnnotation] == getRestoreAnnotationValue(restore)
}

func getRestoreAnnotationValue(restore *hitoseacomv1.VirtualMachineRestore) string {
	return fmt.Sprintf("%s-%s", restore.Name, restore.UID)
}

func (v *vmRestoreTarget) UpdateTarget(obj metav1.Object) {
	//TODO implement me
	panic("implement me")
}
