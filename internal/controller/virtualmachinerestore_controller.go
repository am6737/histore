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
	"errors"
	"fmt"
	"github.com/go-logr/logr"
	vsv1 "github.com/kubernetes-csi/external-snapshotter/client/v6/apis/volumesnapshot/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"strings"
	"time"

	hitoseacomv1 "github.com/am6737/histore/api/v1"
)

var restoreAnnotationsToDelete = []string{
	"pv.kubernetes.io",
	"volume.beta.kubernetes.io",
	"cdi.kubevirt.io",
	"volume.kubernetes.io",
	"k8s.io/CloneRequest",
	"k8s.io/CloneOf",
}

// VirtualMachineRestoreReconciler reconciles a VirtualMachineRestore object
type VirtualMachineRestoreReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Log    logr.Logger
}

//+kubebuilder:rbac:groups=hitosea.com,resources=virtualmachinerestores,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=hitosea.com,resources=virtualmachinerestores/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=hitosea.com,resources=virtualmachinerestores/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the VirtualMachineRestore object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.14.4/pkg/reconcile
func (r *VirtualMachineRestoreReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	_ = log.FromContext(ctx)

	// TODO(user): your logic here
	vmRestoreIn := &hitoseacomv1.VirtualMachineRestore{}
	if err := r.Client.Get(ctx, req.NamespacedName, vmRestoreIn, &client.GetOptions{}); err != nil {
		return ctrl.Result{}, err
	}

	if !VmRestoreProgressing(vmRestoreIn) {
		return ctrl.Result{}, nil
	}

	vmRestoreOut := vmRestoreIn.DeepCopy()
	if vmRestoreOut.Status == nil {
		f := false
		vmRestoreOut.Status = &hitoseacomv1.VirtualMachineRestoreStatus{
			Complete: &f,
		}
	}

	target, err := r.getTarget(vmRestoreOut)
	if err != nil {
		r.Log.Error(err, "Error getting restore target")
		return ctrl.Result{}, err
	}

	if len(vmRestoreOut.OwnerReferences) == 0 {
		target.Own(vmRestoreOut)
		//updateRestoreCondition(vmRestoreOut, newProgressingCondition(corev1.ConditionTrue, "Initializing VirtualMachineRestore"))
		//updateRestoreCondition(vmRestoreOut, newReadyCondition(corev1.ConditionFalse, "Initializing VirtualMachineRestore"))
	}

	if err = target.UpdateRestoreInProgress(); err != nil {
		return ctrl.Result{}, err
	}

	// let's make sure everything is initialized properly before continuing
	if !equality.Semantic.DeepEqual(vmRestoreIn, vmRestoreOut) {
		return ctrl.Result{}, r.doUpdate(vmRestoreIn, vmRestoreOut)
	}

	updated, err := r.reconcileVolumeRestores(vmRestoreOut, target)
	if err != nil {
		r.Log.Error(errors.New(""), "Error reconciling VolumeRestores")
		return ctrl.Result{}, err
	}
	if updated {
		r.Log.Info("reconcileVolumeRestores updated")
		//updateRestoreCondition(vmRestoreOut, newProgressingCondition(corev1.ConditionTrue, "Creating new PVCs"))
		//updateRestoreCondition(vmRestoreOut, newReadyCondition(corev1.ConditionFalse, "Waiting for new PVCs"))
		//return 0, ctrl.doUpdate(vmRestoreIn, vmRestoreOut)
	}

	ready, err := target.Ready()
	if err != nil {
		r.Log.Error(err, "Error checking target ready")
		return ctrl.Result{}, err
	}
	if !ready {
		r.Log.Info("Waiting for target to be ready")
		//reason := "Waiting for target to be ready"
		//updateRestoreCondition(vmRestoreOut, newProgressingCondition(corev1.ConditionFalse, reason))
		//updateRestoreCondition(vmRestoreOut, newReadyCondition(corev1.ConditionFalse, reason))
		// try again in 5 secs
		return ctrl.Result{
			Requeue:      true,
			RequeueAfter: 5 * time.Second,
		}, err
	}

	return ctrl.Result{}, nil
}

func (r *VirtualMachineRestoreReconciler) reconcileVolumeRestores(vmRestore *hitoseacomv1.VirtualMachineRestore, target restoreTarget) (bool, error) {
	content, err := r.getSnapshotContent(vmRestore)
	if err != nil {
		return false, err
	}

	noRestore := volumesNotForRestore(content)

	var restores []hitoseacomv1.VolumeRestore
	for _, vb := range content.Spec.VolumeBackups {
		if noRestore.Has(vb.VolumeName) {
			continue
		}

		found := false
		for _, vr := range vmRestore.Status.Restores {
			if vb.VolumeName == vr.VolumeName {
				restores = append(restores, vr)
				found = true
				break
			}
		}

		if !found {
			if vb.VolumeSnapshotName == nil {
				return false, fmt.Errorf("VolumeSnapshotName missing %+v", vb)
			}

			vr := hitoseacomv1.VolumeRestore{
				VolumeName:                vb.VolumeName,
				PersistentVolumeClaimName: restorePVCName(vmRestore, vb.VolumeName),
				VolumeSnapshotName:        *vb.VolumeSnapshotName,
			}
			restores = append(restores, vr)
		}
	}

	if !equality.Semantic.DeepEqual(vmRestore.Status.Restores, restores) {
		if len(vmRestore.Status.Restores) > 0 {
			r.Log.Info("VMRestore in strange state")
		}

		vmRestore.Status.Restores = restores
		return true, nil
	}

	createdPVC := false
	waitingPVC := false
	for _, restore := range restores {
		pvc, err := r.getPVC(vmRestore.Namespace, restore.PersistentVolumeClaimName)
		if err != nil {
			return false, err
		}

		if pvc == nil {
			backup, err := getRestoreVolumeBackup(restore.VolumeName, content)
			if err != nil {
				return false, err
			}
			if err = r.createRestorePVC(pvc, vmRestore, target, backup, &restore, content.Spec.Source.VirtualMachine.Name, content.Spec.Source.VirtualMachine.Namespace); err != nil {
				return false, err
			}
			createdPVC = true
		} else if pvc.Status.Phase == corev1.ClaimPending {
			bindingMode, err := r.getBindingMode(pvc)
			if err != nil {
				return false, err
			}

			if bindingMode == nil || *bindingMode == storagev1.VolumeBindingImmediate {
				waitingPVC = true
			}
		} else if pvc.Status.Phase != corev1.ClaimBound {
			return false, fmt.Errorf("PVC %s/%s in status %q", pvc.Namespace, pvc.Name, pvc.Status.Phase)
		}
	}
	return createdPVC || waitingPVC, nil
}

func getRestoreVolumeBackup(volName string, content *hitoseacomv1.VirtualMachineSnapshotContent) (*hitoseacomv1.VolumeBackup, error) {
	for _, vb := range content.Spec.VolumeBackups {
		if vb.VolumeName == volName {
			return &vb, nil
		}
	}
	return &hitoseacomv1.VolumeBackup{}, fmt.Errorf("volume backup for volume %s not found", volName)
}

func restorePVCName(vmRestore *hitoseacomv1.VirtualMachineRestore, name string) string {
	return fmt.Sprintf("restore-%s-%s", vmRestore.UID, name)
}

// Returns a set of volumes not for restore
// Currently only memory dump volumes should not be restored
func volumesNotForRestore(content *hitoseacomv1.VirtualMachineSnapshotContent) sets.String {
	volumes := content.Spec.Source.VirtualMachine.Spec.Template.Spec.Volumes
	noRestore := sets.NewString()

	for _, volume := range volumes {
		if volume.MemoryDump != nil {
			noRestore.Insert(volume.Name)
		}
	}

	return noRestore
}

func (r *VirtualMachineRestoreReconciler) getSnapshotContent(vmRestore *hitoseacomv1.VirtualMachineRestore) (*hitoseacomv1.VirtualMachineSnapshotContent, error) {

	objKey := &hitoseacomv1.VirtualMachineSnapshot{}
	err := r.Client.Get(context.TODO(), client.ObjectKey{
		Namespace: vmRestore.Namespace,
		Name:      vmRestore.Spec.VirtualMachineSnapshotName,
	}, objKey)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("VMSnapshot %s does not exist", objKey)
		}
		return nil, err
	}

	vms := objKey.DeepCopy()
	if !VmSnapshotReady(vms) {
		return nil, fmt.Errorf("VirtualMachineSnapshot %s not ready", objKey)
	}

	if vms.Status.VirtualMachineSnapshotContentName == nil {
		return nil, fmt.Errorf("no snapshot content name in %s", objKey)
	}

	obj := &hitoseacomv1.VirtualMachineSnapshotContent{}
	if err := r.Client.Get(context.TODO(), client.ObjectKey{
		Namespace: vmRestore.Namespace,
		Name:      *vms.Status.VirtualMachineSnapshotContentName,
	}, obj, &client.GetOptions{}); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("VirtualMachineSnapshotContent %s does not exist", objKey)
		}
		return nil, err
	}

	vmss := obj.DeepCopy()
	if !vmSnapshotContentReady(vmss) {
		return nil, fmt.Errorf("VirtualMachineSnapshotContent %s not ready", objKey)
	}

	return vmss, nil
}

func (r *VirtualMachineRestoreReconciler) doUpdate(original, updated *hitoseacomv1.VirtualMachineRestore) error {
	if !equality.Semantic.DeepEqual(original, updated) {
		if err := r.Client.Update(context.Background(), updated, &client.UpdateOptions{}); err != nil {
			return err
		}
	}

	return nil
}

func VmRestoreProgressing(vmRestore *hitoseacomv1.VirtualMachineRestore) bool {
	return vmRestore.Status == nil || vmRestore.Status.Complete == nil || !*vmRestore.Status.Complete
}

// SetupWithManager sets up the controller with the Manager.
func (r *VirtualMachineRestoreReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&hitoseacomv1.VirtualMachineRestore{}).
		Complete(r)
}

func (r *VirtualMachineRestoreReconciler) getPVC(namespace, volumeName string) (*corev1.PersistentVolumeClaim, error) {
	obj := &corev1.PersistentVolumeClaim{}
	if err := r.Client.Get(context.TODO(), client.ObjectKey{Namespace: namespace, Name: volumeName}, obj); err != nil {
		return nil, err
	}
	return obj.DeepCopy(), nil
}

func (r *VirtualMachineRestoreReconciler) createRestorePVC(oldPvc *corev1.PersistentVolumeClaim, vmRestore *hitoseacomv1.VirtualMachineRestore, target restoreTarget, volumeBackup *hitoseacomv1.VolumeBackup, volumeRestore *hitoseacomv1.VolumeRestore, sourceVmName, sourceVmNamespace string) error {
	if volumeBackup == nil || volumeBackup.VolumeSnapshotName == nil {
		r.Log.Error(errors.New(""), fmt.Sprintf("VolumeSnapshot name missing %+v", volumeBackup))
		return fmt.Errorf("missing VolumeSnapshot name")
	}

	if vmRestore == nil {
		return fmt.Errorf("missing vmRestore")
	}

	//volumeSnapshot := &vsv1.VolumeSnapshot{}
	//if err := r.Client.Get(context.Background(), client.ObjectKey{Namespace: vmRestore.Namespace, Name: *volumeBackup.VolumeSnapshotName}, volumeSnapshot); err != nil {
	//	return err
	//}

	if volumeRestore == nil {
		return fmt.Errorf("missing volumeRestore")
	}

	oldPv := &corev1.PersistentVolume{}
	if err := r.Client.Get(context.Background(), client.ObjectKey{Namespace: oldPvc.Namespace, Name: oldPvc.Spec.VolumeName}, oldPv); err != nil {
		return err
	}

	pvc := CreateRestoreStaticPVCDefFromVMRestore(vmRestore.Name, volumeRestore.PersistentVolumeClaimName, volumeBackup, sourceVmName, sourceVmNamespace)
	//pvc := CreateRestorePVCDefFromVMRestore(vmRestore.Name, volumeRestore.PersistentVolumeClaimName, volumeSnapshot, volumeBackup, sourceVmName, sourceVmNamespace)
	target.Own(pvc)
	if err := r.Client.Create(context.Background(), pvc, &client.CreateOptions{}); err != nil {
		return err
	}

	pv := r.CreateRestoreStaticPVDefFromVMRestore(oldPv)
	pv.Name = "pvc-" + string(pvc.UID)
	if err := r.Client.Create(context.Background(), pv, &client.CreateOptions{}); err != nil {
		return err
	}

	pvc.Spec.VolumeName = pv.Name
	// pvc Binding pv
	if err := r.Client.Status().Update(context.Background(), pvc); err != nil {
		return fmt.Errorf("failed to update PVC status: %w", err)
	}

	return nil
}

func (r *VirtualMachineRestoreReconciler) CreateRestoreStaticPVDefFromVMRestore(oldPv *corev1.PersistentVolume) *corev1.PersistentVolume {
	return CreateRestoreStaticPVDef(oldPv)
}

func (r *VirtualMachineRestoreReconciler) getBindingMode(pvc *corev1.PersistentVolumeClaim) (*storagev1.VolumeBindingMode, error) {
	if pvc.Spec.StorageClassName == nil {
		return nil, nil
	}

	obj := &storagev1.StorageClass{}
	if err := r.Client.Get(context.Background(), client.ObjectKey{Name: *pvc.Spec.StorageClassName}, obj); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("StorageClass %s does not exist", *pvc.Spec.StorageClassName)
		}
		return nil, err
	}

	sc := obj.DeepCopy()
	return sc.VolumeBindingMode, nil
}

func CreateRestoreStaticPVCDefFromVMRestore(vmRestoreName, restorePVCName string, volumeBackup *hitoseacomv1.VolumeBackup, sourceVmName, sourceVmNamespace string) *corev1.PersistentVolumeClaim {
	pvc := CreateRestoreStaticPVCDef(restorePVCName, volumeBackup)
	if pvc.Labels == nil {
		pvc.Labels = make(map[string]string)
	}

	if pvc.Annotations == nil {
		pvc.Annotations = make(map[string]string)
	}
	pvc.Labels[restoreSourceNameLabel] = sourceVmName
	pvc.Labels[restoreSourceNamespaceLabel] = sourceVmNamespace
	pvc.Labels["hitosea.com/histore"] = "true"
	pvc.Annotations[restoreNameAnnotation] = vmRestoreName
	return pvc
}

func CreateRestorePVCDefFromVMRestore(vmRestoreName, restorePVCName string, volumeSnapshot *vsv1.VolumeSnapshot, volumeBackup *hitoseacomv1.VolumeBackup, sourceVmName, sourceVmNamespace string) *corev1.PersistentVolumeClaim {
	pvc := CreateRestorePVCDef(restorePVCName, volumeSnapshot, volumeBackup)
	if pvc.Labels == nil {
		pvc.Labels = make(map[string]string)
	}

	if pvc.Annotations == nil {
		pvc.Annotations = make(map[string]string)
	}
	pvc.Labels[restoreSourceNameLabel] = sourceVmName
	pvc.Labels[restoreSourceNamespaceLabel] = sourceVmNamespace
	pvc.Annotations[restoreNameAnnotation] = vmRestoreName
	return pvc
}

func CreateRestoreStaticPVDef(pv *corev1.PersistentVolume) *corev1.PersistentVolume {
	return &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: pv.Namespace,
		},
		Spec: corev1.PersistentVolumeSpec{
			Capacity: pv.Spec.Capacity,
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: pv.Spec.CSI,
			},
			StorageClassName:              "",
			AccessModes:                   pv.Spec.AccessModes,
			PersistentVolumeReclaimPolicy: pv.Spec.PersistentVolumeReclaimPolicy,
			MountOptions:                  pv.Spec.MountOptions,
			VolumeMode:                    pv.Spec.VolumeMode,
		},
	}
}

func CreateRestoreStaticPVCDef(restorePVCName string, volumeBackup *hitoseacomv1.VolumeBackup) *corev1.PersistentVolumeClaim {
	sourcePVC := volumeBackup.PersistentVolumeClaim.DeepCopy()
	newPvc := &corev1.PersistentVolumeClaim{
		TypeMeta: metav1.TypeMeta{},
		ObjectMeta: metav1.ObjectMeta{
			Name:        restorePVCName,
			Labels:      sourcePVC.Labels,
			Annotations: sourcePVC.Annotations,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: sourcePVC.Spec.AccessModes,
			Resources:   sourcePVC.Spec.Resources,
			VolumeMode:  sourcePVC.Spec.VolumeMode,
			//StorageClassName: "",
		},
	}

	for _, prefix := range restoreAnnotationsToDelete {
		for anno := range newPvc.Annotations {
			if strings.HasPrefix(anno, prefix) {
				delete(newPvc.Annotations, anno)
			}
		}
	}

	return newPvc
}

func CreateRestorePVCDef(restorePVCName string, volumeSnapshot *vsv1.VolumeSnapshot, volumeBackup *hitoseacomv1.VolumeBackup) *corev1.PersistentVolumeClaim {
	if volumeBackup == nil || volumeBackup.VolumeSnapshotName == nil {
		log.Log.Error(errors.New(""), fmt.Sprintf("VolumeSnapshot name missing %+v", volumeBackup))
		return nil
	}
	sourcePVC := volumeBackup.PersistentVolumeClaim.DeepCopy()
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        restorePVCName,
			Labels:      sourcePVC.Labels,
			Annotations: sourcePVC.Annotations,
		},
		Spec: sourcePVC.Spec,
	}

	if volumeSnapshot == nil {
		log.Log.Error(errors.New(""), fmt.Sprintf("VolumeSnapshot missing %+v", volumeSnapshot))
		return nil
	}
	if volumeSnapshot.Status != nil && volumeSnapshot.Status.RestoreSize != nil {
		restorePVCSize, ok := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
		// Update restore pvc size to be the maximum between the source PVC and the restore size
		if !ok || restorePVCSize.Cmp(*volumeSnapshot.Status.RestoreSize) < 0 {
			pvc.Spec.Resources.Requests[corev1.ResourceStorage] = *volumeSnapshot.Status.RestoreSize
		}
	}

	for _, prefix := range restoreAnnotationsToDelete {
		for anno := range pvc.Annotations {
			if strings.HasPrefix(anno, prefix) {
				delete(pvc.Annotations, anno)
			}
		}
	}

	apiGroup := vsv1.GroupName
	// We need to overwrite both dataSource and dataSourceRef to avoid incompatibilities between the two
	pvc.Spec.DataSource = &corev1.TypedLocalObjectReference{
		APIGroup: &apiGroup,
		Kind:     "VolumeSnapshot",
		Name:     *volumeBackup.VolumeSnapshotName,
	}
	pvc.Spec.DataSourceRef = &corev1.TypedObjectReference{
		APIGroup: &apiGroup,
		Kind:     "VolumeSnapshot",
		Name:     *volumeBackup.VolumeSnapshotName,
	}

	pvc.Spec.VolumeName = ""
	return pvc
}
