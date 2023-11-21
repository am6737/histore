package controller

import (
	"context"
	"fmt"
	hitoseacomv1 "github.com/am6737/histore/api/v1"
	"github.com/am6737/histore/pkg/config"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	kubevirtv1 "kubevirt.io/api/core/v1"
	cdiv1 "kubevirt.io/containerized-data-importer-api/pkg/apis/core/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"
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
	controller *VirtualMachineRestoreReconciler
	vmRestore  *hitoseacomv1.VirtualMachineRestore
	vm         *kubevirtv1.VirtualMachine
	oldPvc     *corev1.PersistentVolumeClaim
	oldPv      *corev1.PersistentVolume
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
			controller: r,
			vmRestore:  vmRestore,
			vm:         vm,
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

func (t *vmRestoreTarget) Ready() (bool, error) {
	if !t.doesTargetVMExist() {
		fmt.Println("Ready 1")
		return true, nil
	}

	//rs, err := t.vm.RunStrategy()
	//if err != nil {
	//	return false, err
	//}

	//if rs != kubevirtv1.RunStrategyHalted {
	//	return false, fmt.Errorf("invalid RunStrategy %q", rs)
	//}

	vmi := &kubevirtv1.VirtualMachineInstance{}
	if err := t.controller.Get(context.Background(), client.ObjectKey{Namespace: t.vm.Namespace, Name: t.vm.Name}, vmi); err != nil {
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, err
	}

	return false, nil
}

func (t *vmRestoreTarget) Reconcile() (bool, error) {
	if updated, err := t.reconcileSpec(); updated || err != nil {
		return updated, err
	}
	return t.reconcileDataVolumes()
}

func (t *vmRestoreTarget) reconcileDataVolumes() (bool, error) {
	createdDV := false
	waitingDV := false
	for _, dvt := range t.vm.Spec.DataVolumeTemplates {
		dv, err := t.controller.getDV(t.vm.Namespace, dvt.Name)
		if err != nil {
			return false, err
		}
		if dv != nil {
			waitingDV = dv.Status.Phase != cdiv1.Succeeded && dv.Status.Phase != cdiv1.WaitForFirstConsumer
			continue
		}
		if createdDV, err = t.createDataVolume(dvt); err != nil {
			return false, err
		}
		if !createdDV {
			continue
		}
	}
	return createdDV || waitingDV, nil
}

func (t *vmRestoreTarget) Cleanup() error {
	var err error
	for _, dvName := range t.vmRestore.Status.DeletedDataVolumes {
		dv := &cdiv1.DataVolume{}
		if err = t.controller.Client.Get(context.Background(), types.NamespacedName{Namespace: t.vmRestore.Namespace, Name: dvName}, dv); err != nil {
			if apierrors.IsNotFound(err) {
				fmt.Println("dv 不存在 => ", dvName)
				return nil
			}
			return err
		}
		fmt.Println("要删除的dv => ", dvName)
		if err = t.controller.Client.Delete(context.Background(), dv); err != nil {
			return err
		}
	}

	return nil
}

func (t *vmRestoreTarget) Own(obj metav1.Object) {
	if !t.doesTargetVMExist() {
		return
	}
	b := true
	obj.SetOwnerReferences([]metav1.OwnerReference{
		{
			APIVersion:         kubevirtv1.GroupVersion.String(),
			Kind:               "VirtualMachine",
			Name:               t.vm.Name,
			UID:                t.vm.UID,
			Controller:         &b,
			BlockOwnerDeletion: &b,
		},
	})
}

func (t *vmRestoreTarget) doesTargetVMExist() bool {
	return t.vm != nil
}

func (t *vmRestoreTarget) UpdateDoneRestore() (bool, error) {
	if t.vm.Status.RestoreInProgress == nil || *t.vm.Status.RestoreInProgress != t.vmRestore.Name {
		return false, nil
	}

	vmCopy := t.vm.DeepCopy()

	vmCopy.Status.RestoreInProgress = nil
	vmCopy.Status.MemoryDumpRequest = nil

	return true, t.controller.Client.Status().Update(context.TODO(), vmCopy)
}

func (t *vmRestoreTarget) UpdateRestoreInProgress() error {
	if !t.doesTargetVMExist() || hasLastRestoreAnnotation(t.vmRestore, t.vm) {
		return nil
	}

	if t.vm.Status.RestoreInProgress != nil && *t.vm.Status.RestoreInProgress != t.vmRestore.Name {
		return fmt.Errorf("vm restore %s in progress", *t.vm.Status.RestoreInProgress)
	}

	vmCopy := t.vm.DeepCopy()

	if vmCopy.Status.RestoreInProgress == nil {
		vmCopy.Status.RestoreInProgress = &t.vmRestore.Name

		// unfortunately, status Updater does not return the updated resource
		// but the controller is watching VMs so will get notified
		//return t.controller.vmStatusUpdater.UpdateStatus(vmCopy)
	}

	return nil
}

func hasLastRestoreAnnotation(restore *hitoseacomv1.VirtualMachineRestore, obj metav1.Object) bool {
	return obj.GetAnnotations()[lastRestoreAnnotation] == getRestoreAnnotationValue(restore)
}

func getRestoreAnnotationValue(restore *hitoseacomv1.VirtualMachineRestore) string {
	return fmt.Sprintf("%s-%s", restore.Name, restore.UID)
}

func (t *vmRestoreTarget) UpdateTarget(obj metav1.Object) {
	t.vm = obj.(*kubevirtv1.VirtualMachine)
}

func (t *vmRestoreTarget) reconcileSpec() (bool, error) {
	if t.doesTargetVMExist() && hasLastRestoreAnnotation(t.vmRestore, t.vm) {
		fmt.Println("hasLastRestoreAnnotation => ", t.vm.Annotations[lastRestoreAnnotation])
		return false, nil
	}

	content, err := t.controller.getSnapshotContent(t.vmRestore)
	if err != nil {
		return false, err
	}
	//
	//if content == nil {
	//	return false, fmt.Errorf("content does not exist")
	//}

	snapshotVM := content.Spec.Source.VirtualMachine
	if snapshotVM == nil {
		return false, fmt.Errorf("unexpected snapshot source")
	}

	var newTemplates = make([]kubevirtv1.DataVolumeTemplateSpec, len(snapshotVM.Spec.DataVolumeTemplates))
	var newVolumes []kubevirtv1.Volume
	var deletedDataVolumes []string
	updatedStatus := false

	for i, t := range snapshotVM.Spec.DataVolumeTemplates {
		t.DeepCopyInto(&newTemplates[i])
	}

	for _, v := range snapshotVM.Spec.Template.Spec.Volumes {
		nv := v.DeepCopy()
		//dump.Println("nv.DataVolume => ", nv.DataVolume)
		//dump.Println("nv.PersistentVolumeClaim => ", nv.PersistentVolumeClaim)
		//dump.Println("t.vmRestore.Status.Restores ", t.vmRestore.Status.Restores)
		if nv.DataVolume != nil || nv.PersistentVolumeClaim != nil {
			for k := range t.vmRestore.Status.Restores {
				vr := &t.vmRestore.Status.Restores[k]
				//fmt.Println("vmRestore status => ", vr)
				if vr.VolumeName != nv.Name {
					continue
				}
				//fmt.Println("reconcileSpec 2")
				pvc, err := t.controller.getPVC(t.vmRestore.Namespace, vr.PersistentVolumeClaimName)
				if err != nil {
					return false, err
				}

				if pvc == nil {
					return false, fmt.Errorf("pvc %s/%s does not exist and should", t.vmRestore.Namespace, vr.PersistentVolumeClaimName)
				}

				if nv.DataVolume != nil {
					templateIndex := -1
					for i, dvt := range snapshotVM.Spec.DataVolumeTemplates {
						//fmt.Println("v.DataVolume.Name => ", v.DataVolume.Name)
						//fmt.Println(" dvt.Name => ", dvt.Name)
						if v.DataVolume.Name == dvt.Name {
							templateIndex = i
							break
						}
					}

					if templateIndex >= 0 {
						if vr.DataVolumeName == nil {
							updatePVC := pvc.DeepCopy()
							dvName := restoreDVName(t.vmRestore, vr.VolumeName)
							//fmt.Println("pvc name => ", pvc.Name)
							//fmt.Println("restoreDVName -------1 ")
							if updatePVC.Annotations[populatedForPVCAnnotation] != dvName {
								if updatePVC.Annotations == nil {
									updatePVC.Annotations = make(map[string]string)
								}
								updatePVC.Annotations[populatedForPVCAnnotation] = dvName
								updatePVC.OwnerReferences = nil
								if err = t.controller.Client.Update(context.Background(), updatePVC); err != nil {
									return false, err
								}
							}
							vr.DataVolumeName = &dvName
							updatedStatus = true
						}

						dv := snapshotVM.Spec.DataVolumeTemplates[templateIndex].DeepCopy()
						dv.Name = *vr.DataVolumeName
						newTemplates[templateIndex] = *dv
						nv.DataVolume.Name = *vr.DataVolumeName
						//fmt.Println("vr.DataVolumeName => ", *vr.DataVolumeName)
					} else {
						// convert to PersistentVolumeClaim volume
						nv = &kubevirtv1.Volume{
							Name: nv.Name,
							VolumeSource: kubevirtv1.VolumeSource{
								PersistentVolumeClaim: &kubevirtv1.PersistentVolumeClaimVolumeSource{
									PersistentVolumeClaimVolumeSource: corev1.PersistentVolumeClaimVolumeSource{
										ClaimName: vr.PersistentVolumeClaimName,
									},
								},
							},
						}
					}
				} else {
					nv.PersistentVolumeClaim.ClaimName = vr.PersistentVolumeClaimName
				}
			}
		} else if nv.MemoryDump != nil {
			// don't restore memory dump volume in the new spec
			continue
		}
		newVolumes = append(newVolumes, *nv)
	}
	//fmt.Println("updatedStatus => ", updatedStatus)
	if t.doesTargetVMExist() && updatedStatus {
		//fmt.Println("t.doesTargetVMExist() && updatedStatus")
		// find DataVolumes that will no longer exist
		for _, cdv := range t.vm.Spec.DataVolumeTemplates {
			found := false
			for _, ndv := range newTemplates {
				//fmt.Println("cdv.Name => ", cdv.Name)
				//fmt.Println("ndv.Name => ", ndv.Name)
				if cdv.Name == ndv.Name {
					found = true
					break
				}
			}
			if !found {
				deletedDataVolumes = append(deletedDataVolumes, cdv.Name)
			}
		}
		t.vmRestore.Status.DeletedDataVolumes = deletedDataVolumes

		return true, nil
	}
	//fmt.Println("reconcileSpec 5")
	var newVM *kubevirtv1.VirtualMachine
	if !t.doesTargetVMExist() {
		newVM = &kubevirtv1.VirtualMachine{
			ObjectMeta: metav1.ObjectMeta{
				Name:        t.vmRestore.Spec.Target.Name,
				Namespace:   t.vmRestore.Namespace,
				Labels:      snapshotVM.Labels,
				Annotations: snapshotVM.Annotations,
			},
			Spec:   *snapshotVM.Spec.DeepCopy(),
			Status: kubevirtv1.VirtualMachineStatus{},
		}

	} else {
		newVM = t.vm.DeepCopy()
		newVM.Spec = *snapshotVM.Spec.DeepCopy()
	}

	// update Running state in case snapshot was on online VM
	if newVM.Spec.RunStrategy != nil {
		runStrategyHalted := kubevirtv1.RunStrategyHalted
		newVM.Spec.RunStrategy = &runStrategyHalted
	} else if newVM.Spec.Running != nil {
		running := false
		newVM.Spec.Running = &running
	}
	newVM.Spec.DataVolumeTemplates = newTemplates
	newVM.Spec.Template.Spec.Volumes = newVolumes

	fmt.Println("setLastRestoreAnnotation => ", getRestoreAnnotationValue(t.vmRestore))

	setLastRestoreAnnotation(t.vmRestore, newVM)

	//dump.Println("newVM.Spec.DataVolumeTemplates => ", newVM.Spec.DataVolumeTemplates)
	//dump.Println("newVM.Spec.Template.Spec.Volumes => ", newVM.Spec.Template.Spec.Volumes)

	//newVM, err = patchVM(newVM, t.vmRestore.Spec.Patches)
	//if err != nil {
	//	return false, fmt.Errorf("error patching VM %s: %v", newVM.Name, err)
	//}

	if !t.doesTargetVMExist() {
		fmt.Println("create newVM => ", newVM.Name)
		if err = t.controller.Client.Create(context.TODO(), newVM); err != nil {
			return false, err
		}
	} else {
		if err = t.controller.Client.Update(context.TODO(), newVM); err != nil {
			return false, err
		}
	}
	if err != nil {
		return false, err
	}
	t.UpdateTarget(newVM)

	//if err = t.claimInstancetypeControllerRevisionsOwnership(t.vm); err != nil {
	//	return false, err
	//}
	return true, nil
}

func (t *vmRestoreTarget) createDataVolume(dvt kubevirtv1.DataVolumeTemplateSpec) (bool, error) {
	pvc, err := t.controller.getPVC(t.vm.Namespace, dvt.Name)
	if err != nil {
		return false, err
	}
	if pvc.Annotations[populatedForPVCAnnotation] != dvt.Name || len(pvc.OwnerReferences) > 0 {
		return false, nil
	}

	fmt.Println("createDataVolume 1")
	newDataVolume, err := CreateDataVolumeManifest(t.controller.Client, dvt, t.vm)
	if err != nil {
		return false, fmt.Errorf("Unable to create restore DataVolume manifest: %v", err)
	}
	fmt.Println("createDataVolume 2")
	if newDataVolume.Annotations == nil {
		newDataVolume.Annotations = make(map[string]string)
	}
	newDataVolume.Annotations[restoreNameAnnotation] = t.vmRestore.Name
	newDataVolume.Namespace = corev1.NamespaceDefault
	newDataVolume.Spec.Storage.StorageClassName = &config.DC.SlaveStorageClass

	//dump.Println("newDataVolume => ", newDataVolume)

	fmt.Println("t.createDataVolume(dvt)")

	if err = t.controller.Client.Create(context.Background(), newDataVolume); err != nil {
		return false, fmt.Errorf("failed to create restore DataVolume: %v", err)
	}

	//if _, err = t.controller.Client.CdiClient().CdiV1beta1().DataVolumes(t.vm.Namespace).Create(context.Background(), newDataVolume, v1.CreateOptions{}); err != nil {
	//	t.controller.Recorder.Eventf(t.vm, corev1.EventTypeWarning, restoreDataVolumeCreateErrorEvent, "Error creating restore DataVolume %s: %v", newDataVolume.Name, err)
	//	return false, fmt.Errorf("Failed to create restore DataVolume: %v", err)
	//}
	// Update restore DataVolumeName
	for _, v := range t.vm.Spec.Template.Spec.Volumes {
		if v.DataVolume == nil || v.DataVolume.Name != dvt.Name {
			continue
		}
		for k := range t.vmRestore.Status.Restores {
			vr := &t.vmRestore.Status.Restores[k]
			if vr.VolumeName == v.Name {
				vr.DataVolumeName = &dvt.Name
				break
			}
		}
	}

	return true, nil
}

func CreateDataVolumeManifest(client client.Client, dataVolumeTemplate kubevirtv1.DataVolumeTemplateSpec, vm *kubevirtv1.VirtualMachine) (*cdiv1.DataVolume, error) {
	newDataVolume, err := GenerateDataVolumeFromTemplate(client, dataVolumeTemplate, vm.Namespace, vm.Spec.Template.Spec.PriorityClassName)
	if err != nil {
		return nil, err
	}

	newDataVolume.ObjectMeta.Labels[kubevirtv1.CreatedByLabel] = string(vm.UID)
	newDataVolume.ObjectMeta.OwnerReferences = []metav1.OwnerReference{
		*metav1.NewControllerRef(vm, kubevirtv1.VirtualMachineGroupVersionKind),
	}

	return newDataVolume, nil
}

func GenerateDataVolumeFromTemplate(client client.Client, dataVolumeTemplate kubevirtv1.DataVolumeTemplateSpec, namespace, priorityClassName string) (*cdiv1.DataVolume, error) {
	newDataVolume := &cdiv1.DataVolume{}
	newDataVolume.Spec = *dataVolumeTemplate.Spec.DeepCopy()
	newDataVolume.ObjectMeta = *dataVolumeTemplate.ObjectMeta.DeepCopy()

	labels := map[string]string{}
	for k, v := range dataVolumeTemplate.Labels {
		labels[k] = v
	}
	newDataVolume.ObjectMeta.Labels = labels

	annotations := map[string]string{}
	for k, v := range dataVolumeTemplate.Annotations {
		annotations[k] = v
	}
	newDataVolume.ObjectMeta.Annotations = annotations

	if newDataVolume.Spec.PriorityClassName == "" && priorityClassName != "" {
		newDataVolume.Spec.PriorityClassName = priorityClassName
	}

	cloneSource, err := GetCloneSource(context.TODO(), client, namespace, &newDataVolume.Spec)
	if err != nil {
		return nil, err
	}

	if cloneSource != nil {
		// If SourceRef is set, populate spec.Source with data from the DataSource
		// If not, update the field anyway to account for possible namespace changes
		if newDataVolume.Spec.SourceRef != nil {
			newDataVolume.Spec.SourceRef = nil
		}
		newDataVolume.Spec.Source = &cdiv1.DataVolumeSource{
			PVC: &cdiv1.DataVolumeSourcePVC{
				Namespace: cloneSource.Namespace,
				Name:      cloneSource.Name,
			},
		}
	}

	return newDataVolume, nil
}

type CloneSource struct {
	Namespace string
	Name      string
}

func GetCloneSource(ctx context.Context, client client.Client, namespace string, dvSpec *cdiv1.DataVolumeSpec) (*CloneSource, error) {
	var cloneSource *CloneSource
	if dvSpec.Source != nil && dvSpec.Source.PVC != nil {
		cloneSource = &CloneSource{
			Namespace: dvSpec.Source.PVC.Namespace,
			Name:      dvSpec.Source.PVC.Name,
		}

		if cloneSource.Namespace == "" {
			cloneSource.Namespace = namespace
		}
	} else if dvSpec.SourceRef != nil && dvSpec.SourceRef.Kind == "DataSource" {
		ns := namespace
		if dvSpec.SourceRef.Namespace != nil {
			ns = *dvSpec.SourceRef.Namespace
		}

		ds := &cdiv1.DataSource{}
		if err := client.Get(ctx, types.NamespacedName{
			Namespace: ns,
			Name:      dvSpec.SourceRef.Name,
		}, ds); err != nil {
			return nil, err
		}

		//ds, err := client.CdiClient().CdiV1beta1().DataSources(ns).Get(ctx, dvSpec.SourceRef.Name, metav1.GetOptions{})
		//if err != nil {
		//	return nil, err
		//}

		if ds.Spec.Source.PVC != nil {
			cloneSource = &CloneSource{
				Namespace: ds.Spec.Source.PVC.Namespace,
				Name:      ds.Spec.Source.PVC.Name,
			}

			if cloneSource.Namespace == "" {
				cloneSource.Namespace = ns
			}
		}
	}

	return cloneSource, nil
}

func restoreDVName(vmRestore *hitoseacomv1.VirtualMachineRestore, name string) string {
	return restorePVCName(vmRestore, name)
}

func restorePVCName(vmRestore *hitoseacomv1.VirtualMachineRestore, name string) string {
	return fmt.Sprintf("restore-%s-%s", vmRestore.UID, name)
}

func setLastRestoreAnnotation(restore *hitoseacomv1.VirtualMachineRestore, obj metav1.Object) {
	if obj.GetAnnotations() == nil {
		obj.SetAnnotations(make(map[string]string))
	}
	obj.GetAnnotations()[lastRestoreAnnotation] = getRestoreAnnotationValue(restore)
}
