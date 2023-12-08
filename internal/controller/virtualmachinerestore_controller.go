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
	hitoseacomv1 "github.com/am6737/histore/api/v1"
	"github.com/am6737/histore/pkg/ceph/rbd"
	"github.com/am6737/histore/pkg/ceph/util"
	"github.com/am6737/histore/pkg/config"
	librbd "github.com/ceph/go-ceph/rbd"
	"github.com/go-logr/logr"
	vsv1 "github.com/kubernetes-csi/external-snapshotter/client/v6/apis/volumesnapshot/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/tools/record"
	cdi "kubevirt.io/containerized-data-importer-api/pkg/apis/core/v1beta1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"strings"
	"time"
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
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
	Log      logr.Logger
}

// SetupWithManager sets up the controller with the Manager.
func (r *VirtualMachineRestoreReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&hitoseacomv1.VirtualMachineRestore{}).
		Complete(r)
}

//+kubebuilder:rbac:groups=snapshot.hitosea.com,resources=virtualmachinerestores,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=snapshot.hitosea.com,resources=virtualmachinerestores/Status,verbs=get;update;patch
//+kubebuilder:rbac:groups=snapshot.hitosea.com,resources=virtualmachinerestores/finalizers,verbs=update

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
		if apierrors.IsNotFound(err) {
			// 对象不存在，可能已被删除，可以返回一个无需处理的结果
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if !vmRestoreIn.GetDeletionTimestamp().IsZero() {
		fmt.Println("VirtualMachineRestore已删除 => ", req.NamespacedName)
		return ctrl.Result{}, nil
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
		updateRestoreCondition(vmRestoreOut, newProgressingCondition(corev1.ConditionTrue, "Initializing VirtualMachineRestore"))
		updateRestoreCondition(vmRestoreOut, newReadyCondition(corev1.ConditionFalse, "Initializing VirtualMachineRestore"))
	}

	if err = target.UpdateRestoreInProgress(); err != nil {
		return ctrl.Result{}, err
	}

	//// let's make sure everything is initialized properly before continuing
	//if !equality.Semantic.DeepEqual(vmRestoreIn, vmRestoreOut) {
	//	return reconcile.Result{
	//		RequeueAfter: 5 * time.Second,
	//	}, r.doUpdate(vmRestoreIn, vmRestoreOut)
	//}

	updated, err := r.reconcileVolumeRestores(vmRestoreOut, target)
	if err != nil {
		r.Log.Error(err, "reconciling VolumeRestores")
		return reconcile.Result{RequeueAfter: 5 * time.Second}, nil
	}
	if updated {
		//r.Log.Info("reconcileVolumeRestores updated")
		updateRestoreCondition(vmRestoreOut, newProgressingCondition(corev1.ConditionTrue, "Creating new PVCs"))
		updateRestoreCondition(vmRestoreOut, newReadyCondition(corev1.ConditionFalse, "Waiting for new PVCs"))
		return reconcile.Result{
			RequeueAfter: 5 * time.Second,
		}, r.doStatusUpdate(vmRestoreIn, vmRestoreOut)
	}

	ready, err := target.Ready()
	if err != nil {
		r.Log.Error(err, "Error checking target ready")
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
	if !ready {
		r.Log.Info("Waiting for target to be ready")
		reason := "Waiting for target to be ready"
		updateRestoreCondition(vmRestoreOut, newProgressingCondition(corev1.ConditionFalse, reason))
		updateRestoreCondition(vmRestoreOut, newReadyCondition(corev1.ConditionFalse, reason))
		return reconcile.Result{
			RequeueAfter: 5 * time.Second,
		}, r.doStatusUpdate(vmRestoreIn, vmRestoreOut)
	}

	updated, err = target.Reconcile()
	if err != nil {
		r.Log.Error(err, "Error reconciling target")
		return ctrl.Result{}, err
	}
	if updated {
		updateRestoreCondition(vmRestoreOut, newProgressingCondition(corev1.ConditionTrue, "Updating target spec"))
		updateRestoreCondition(vmRestoreOut, newReadyCondition(corev1.ConditionFalse, "Waiting for target update"))
		//if err = r.doStatusUpdate(vmRestoreIn, vmRestoreOut); err != nil {
		//	return reconcile.Result{
		//		RequeueAfter: 5 * time.Second,
		//	}, nil
		//}
		//if err = r.Status().Update(ctx, vmRestoreOut); err != nil {
		//	return reconcile.Result{
		//		RequeueAfter: 15 * time.Second,
		//	}, nil
		//}
		return ctrl.Result{RequeueAfter: 5 * time.Second}, r.doStatusUpdate(vmRestoreIn, vmRestoreOut)
	}

	if err = target.Cleanup(); err != nil {
		r.Log.Error(err, "Error cleaning up")
		return ctrl.Result{}, err
	}

	updated, err = target.UpdateDoneRestore()
	if err != nil {
		r.Log.Error(err, "Error updating done restore")
		return ctrl.Result{}, err
	}
	if updated {
		updateRestoreCondition(vmRestoreOut, newProgressingCondition(corev1.ConditionTrue, "Updating target Status"))
		updateRestoreCondition(vmRestoreOut, newReadyCondition(corev1.ConditionFalse, "Waiting for target update"))
		//if err = r.doStatusUpdate(vmRestoreIn, vmRestoreOut); err != nil {
		//	return reconcile.Result{
		//		RequeueAfter: 5 * time.Second,
		//	}, nil
		//}
		return reconcile.Result{
			RequeueAfter: 5 * time.Second,
		}, r.doStatusUpdate(vmRestoreIn, vmRestoreOut)
		//if err = r.Status().Update(ctx, vmRestoreOut); err != nil {
		//	return reconcile.Result{
		//		RequeueAfter: 15 * time.Second,
		//	}, nil
		//}
		//return ctrl.Result{}, nil
	}

	r.Recorder.Eventf(
		vmRestoreOut,
		corev1.EventTypeNormal,
		restoreCompleteEvent,
		"Successfully completed VirtualMachineRestore %s",
		vmRestoreOut.Name,
	)

	t := true
	vmRestoreOut.Status.Complete = &t
	vmRestoreOut.Status.RestoreTime = currentTime()
	updateRestoreCondition(vmRestoreOut, newProgressingCondition(corev1.ConditionFalse, "Operation complete"))
	updateRestoreCondition(vmRestoreOut, newReadyCondition(corev1.ConditionTrue, "Operation complete"))

	return ctrl.Result{}, r.Status().Update(ctx, vmRestoreOut)
}

func updateRestoreCondition(r *hitoseacomv1.VirtualMachineRestore, c hitoseacomv1.Condition) {
	r.Status.Conditions = updateCondition(r.Status.Conditions, c, true)
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
				return false, fmt.Errorf("MasterVolumeHandle missing %+v", vb)
			}

			vr := hitoseacomv1.VolumeRestore{
				VolumeName:                vb.VolumeName,
				PersistentVolumeClaimName: restorePVCName(vmRestore, vb.VolumeName),
				VolumeSnapshotName:        *vb.VolumeSnapshotName,
			}
			restores = append(restores, vr)
		}
	}

	if vmRestore.Status == nil {
		vmRestore.Status.Restores = restores
		return true, nil
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
	var slaveVolumeHandle, snapshotVolumeHandle string
	for _, restore := range restores {
		pvc, err := r.getPVC(vmRestore.Namespace, restore.PersistentVolumeClaimName)
		if err != nil {
			return false, err
		}

		if pvc == nil {
			backup, err := getRestoreVolumeBackup(restore.VolumeName, content)
			if err != nil {
				r.Log.Error(err, "getRestoreVolumeBackup")
				return false, err
			}

			for _, status := range content.Status.VolumeStatus {
				if backup.VolumeName == status.VolumeName {
					slaveVolumeHandle = status.SlaveVolumeHandle
					snapshotVolumeHandle = status.SnapshotVolumeHandle
					break
				}
			}
			if slaveVolumeHandle == "" {
				return false, fmt.Errorf("SlaveVolumeHandle missing %+v", backup)
			}
			if snapshotVolumeHandle == "" {
				return false, fmt.Errorf("SnapshotVolumeHandle missing %+v", backup)
			}

			pv, err := r.getPv(GetVolUUId(snapshotVolumeHandle))
			if err != nil {
				return false, err
			}

			if pv == nil {
				pv, err = r.createRestorePv(backup, vmRestore.Namespace, snapshotVolumeHandle)
				if err != nil {
					r.Log.Error(err, "createRestorePv")
					return false, err
				}
			}

			if err = r.createRestoreSnapshot(snapshotVolumeHandle, slaveVolumeHandle); err != nil {
				return false, err
			}

			if err = r.createRestorePVC(vmRestore, target, backup, &restore, content.Spec.Source.VirtualMachine.Name, content.Spec.Source.VirtualMachine.Namespace, pv.Name); err != nil {
				r.Log.Error(err, "createRestorePVC")
				return false, err
			}
			createdPVC = true
		} else if pvc.Status.Phase == corev1.ClaimPending {
			bindingMode, err := r.getBindingMode(pvc)
			if err != nil {
				r.Log.Error(err, "getBindingMode")
				return false, err
			}
			if bindingMode == nil || *bindingMode == storagev1.VolumeBindingImmediate {
				waitingPVC = true
			}
		} else if pvc.Status.Phase != corev1.ClaimBound {
			return false, fmt.Errorf("PVC %s/%s in Status %q", pvc.Namespace, pvc.Name, pvc.Status.Phase)
		}
	}

	return createdPVC || waitingPVC, nil
}

func (r *VirtualMachineRestoreReconciler) createRestorePv(volumeBackup *hitoseacomv1.VolumeBackup, vmSnapshotNamespace, snapshotVolumeHandle string) (*corev1.PersistentVolume, error) {

	ssc, err := config.GetCephCsiConfigForSC(r.Client, config.DC.SlaveStorageClass)
	if err != nil {
		return nil, err
	}

	vmSnapshot := &hitoseacomv1.VirtualMachineSnapshot{}
	if err = r.Client.Get(context.Background(), client.ObjectKey{
		Namespace: vmSnapshotNamespace,
		Name:      *volumeBackup.VolumeSnapshotName,
	}, vmSnapshot); err != nil {
		return nil, err
	}

	var deletionPolicy corev1.PersistentVolumeReclaimPolicy
	if vmSnapshot.Spec.DeletionPolicy == nil {
		deletionPolicy = corev1.PersistentVolumeReclaimDelete
	} else {
		deletionPolicy = corev1.PersistentVolumeReclaimPolicy(*vmSnapshot.Spec.DeletionPolicy)
	}

	pv := r.CreateRestoreStaticPVDefFromVMRestore(volumeBackup, ssc, snapshotVolumeHandle, deletionPolicy)
	pv.Name = "pvc-" + GetVolUUId(snapshotVolumeHandle)
	if err = r.Client.Create(context.TODO(), pv); err != nil {
		return nil, err
	}

	r.Log.Info("restore pv created successfully", "name", pv.Name)

	return pv.DeepCopy(), nil
}

func (r *VirtualMachineRestoreReconciler) createRestoreSnapshot(volumeID, sourceVolumeID string) error {
	ssc, err := config.GetCephCsiConfigForSC(r.Client, config.DC.SlaveStorageClass)
	if err != nil {
		return err
	}

	secret, err := r.getSecret(ssc.NodeStageSecretNamespace, ssc.NodeStageSecretName)
	if err != nil {
		return err
	}

	cr, err := util.NewUserCredentials(secret)
	if err != nil {
		return err
	}
	defer cr.DeleteCredentials()

	rbdVol, err := rbd.GenVolFromVolID(context.Background(), volumeID, cr, secret)
	if err != nil {
		if !errors.Is(err, librbd.ErrNotFound) {
			return err
		}
	} else {
		defer rbdVol.Destroy()
		return nil
	}

	_, err = (&VolumeService{Client: r.Client, Log: r.Log}).RestoreSnapshot(context.Background(), &RestoreSnapshotRequest{
		VolumeId:       volumeID,
		SourceVolumeId: sourceVolumeID,
		Secrets:        secret,
		Parameters:     map[string]string{"clusterID": ssc.ClusterID, "pool": ssc.Pool},
	})
	if err != nil {
		return err
	}

	r.Log.Info("restore volume created successfully", "volumeId", volumeID)

	return nil
}

func (r *VirtualMachineRestoreReconciler) getPv(name string) (*corev1.PersistentVolume, error) {
	pv := &corev1.PersistentVolume{}
	if err := r.Client.Get(context.TODO(), client.ObjectKey{
		Name: name,
	}, pv); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}

	return pv.DeepCopy(), nil
}

func getRestoreVolumeBackup(volName string, content *hitoseacomv1.VirtualMachineSnapshotContent) (*hitoseacomv1.VolumeBackup, error) {
	for _, vb := range content.Spec.VolumeBackups {
		if vb.VolumeName == volName {
			return &vb, nil
		}
	}
	return &hitoseacomv1.VolumeBackup{}, fmt.Errorf("volume backup for volume %s not found", volName)
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
	if err := r.Client.Get(context.TODO(), client.ObjectKey{
		Namespace: vmRestore.Namespace,
		Name:      vmRestore.Spec.VirtualMachineSnapshotName,
	}, objKey); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("VMSnapshot %v does not exist", objKey)
		}
		return nil, err
	}

	vms := objKey.DeepCopy()
	if !VmSnapshotReady(vms) {
		return nil, fmt.Errorf("VirtualMachineSnapshot %v not ready", objKey)
	}

	if vms.Status.VirtualMachineSnapshotContentName == nil {
		return nil, fmt.Errorf("no snapshot Content name in %v", objKey)
	}

	obj := &hitoseacomv1.VirtualMachineSnapshotContent{}
	if err := r.Client.Get(context.TODO(), client.ObjectKey{
		Namespace: vmRestore.Namespace,
		Name:      *vms.Status.VirtualMachineSnapshotContentName,
	}, obj, &client.GetOptions{}); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("VirtualMachineSnapshotContent %v does not exist", objKey)
		}
		return nil, err
	}

	vmss := obj.DeepCopy()
	if !vmSnapshotContentReady(vmss) {
		return nil, fmt.Errorf("VirtualMachineSnapshotContent %v not ready", objKey)
	}

	return vmss, nil
}

func (r *VirtualMachineRestoreReconciler) doStatusUpdate(original, updated *hitoseacomv1.VirtualMachineRestore) error {
	if !equality.Semantic.DeepEqual(original, updated) {
		if err := r.Client.Status().Update(context.Background(), updated); err != nil {
			return err
		}
	}
	return nil
}

func (r *VirtualMachineRestoreReconciler) doUpdate(original, updated *hitoseacomv1.VirtualMachineRestore) error {
	if !equality.Semantic.DeepEqual(original, updated) {
		if err := r.Client.Update(context.Background(), updated); err != nil {
			return err
		}
	}
	return nil
}

func VmRestoreProgressing(vmRestore *hitoseacomv1.VirtualMachineRestore) bool {
	return vmRestore.Status == nil || vmRestore.Status.Complete == nil || !*vmRestore.Status.Complete
}

func (r *VirtualMachineRestoreReconciler) getPVC(namespace, volumeName string) (*corev1.PersistentVolumeClaim, error) {
	obj := &corev1.PersistentVolumeClaim{}
	if err := r.Client.Get(context.TODO(), client.ObjectKey{Namespace: namespace, Name: volumeName}, obj); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return obj.DeepCopy(), nil
}

func (r *VirtualMachineRestoreReconciler) createRestorePVC(
	vmRestore *hitoseacomv1.VirtualMachineRestore,
	target restoreTarget,
	volumeBackup *hitoseacomv1.VolumeBackup,
	volumeRestore *hitoseacomv1.VolumeRestore,
	sourceVmName, sourceVmNamespace, pvName string) error {
	if volumeBackup == nil || volumeBackup.VolumeSnapshotName == nil {
		r.Log.Error(errors.New(""), fmt.Sprintf("VolumeSnapshot name missing %+v", volumeBackup))
		return fmt.Errorf("missing VolumeSnapshot name")
	}

	if vmRestore == nil {
		return fmt.Errorf("missing vmRestore")
	}

	if volumeRestore == nil {
		return fmt.Errorf("missing volumeRestore")
	}

	pvc := CreateRestoreStaticPVCDefFromVMRestore(vmRestore.Name, config.DC.SlaveStorageClass, volumeRestore.PersistentVolumeClaimName, volumeBackup, sourceVmName, sourceVmNamespace)
	pvc.Namespace = sourceVmNamespace
	target.Own(pvc)
	if err := r.Client.Create(context.TODO(), pvc); err != nil {
		log.Log.Error(err, "create pvc")
		return err
	}

	r.Log.Info("restore pvc created successfully", "namespace", pvc.Namespace, "name", pvc.Name)

	pvc.Spec.VolumeName = pvName
	// pvc Binding pv
	if err := r.Client.Update(context.TODO(), pvc); err != nil {
		return fmt.Errorf("failed to update PVC Status: %w", err)
	}

	return nil
}

func (r *VirtualMachineRestoreReconciler) CreateRestoreStaticPVDefFromVMRestore(volumeBackup *hitoseacomv1.VolumeBackup, ssc *config.CephCsiConfig, slaveVolumeHandle string, deletionPolicy corev1.PersistentVolumeReclaimPolicy) *corev1.PersistentVolume {
	pv := CreateRestoreStaticPVDef(volumeBackup, ssc, slaveVolumeHandle, deletionPolicy)
	if pv.Annotations == nil {
		pv.Annotations = make(map[string]string)
	}
	pv.Annotations["pv.kubernetes.io/provisioned-by"] = ssc.Driver
	pv.Annotations[deletionSecretName] = ssc.NodeStageSecretName
	pv.Annotations[deletionSecretNamespace] = ssc.NodeStageSecretNamespace
	return pv
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

func (r *VirtualMachineRestoreReconciler) getDV(namespace string, name string) (*cdi.DataVolume, error) {
	dv := &cdi.DataVolume{}
	if err := r.Client.Get(context.TODO(), client.ObjectKey{Namespace: namespace, Name: name}, dv); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return dv.DeepCopy(), nil
}

func (r *VirtualMachineRestoreReconciler) getSnapshot(content *hitoseacomv1.VirtualMachineSnapshotContent) (*hitoseacomv1.VirtualMachineSnapshot, error) {
	for _, ref := range content.OwnerReferences {
		if ref.Kind == "VirtualMachineSnapshot" {
			obj := &hitoseacomv1.VirtualMachineSnapshot{}
			if err := r.Client.Get(context.TODO(), client.ObjectKey{Namespace: content.Namespace, Name: ref.Name}, obj); err != nil {
				if apierrors.IsNotFound(err) {
					return nil, nil
				}
			}
			return obj.DeepCopy(), nil
		}
	}
	return nil, nil
}

func CreateRestoreStaticPVCDefFromVMRestore(vmRestoreName, storageClassName, restorePVCName string, volumeBackup *hitoseacomv1.VolumeBackup, sourceVmName, sourceVmNamespace string) *corev1.PersistentVolumeClaim {
	pvc := CreateRestoreStaticPVCDef(restorePVCName, storageClassName, volumeBackup)
	if pvc.Labels == nil {
		pvc.Labels = make(map[string]string)
	}

	if pvc.Annotations == nil {
		pvc.Annotations = make(map[string]string)
	}
	pvc.Labels[restoreSourceNameLabel] = sourceVmName
	pvc.Labels[restoreSourceNamespaceLabel] = sourceVmNamespace
	pvc.Labels["snapshot.hitosea.com/histore"] = "true"
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

func CreateRestoreStaticPVDef(volumeBackup *hitoseacomv1.VolumeBackup, ssc *config.CephCsiConfig, slaveVolumeHandle string, deletionPolicy corev1.PersistentVolumeReclaimPolicy) *corev1.PersistentVolume {
	options := map[string]string{
		"clusterID":     ssc.ClusterID,
		"imageFeatures": "layering",
		"imageName":     "csi-vol-" + GetVolUUId(slaveVolumeHandle),
		"journalPool":   ssc.Pool,
		"pool":          ssc.Pool,
	}
	sourcePVC := volumeBackup.PersistentVolumeClaim.DeepCopy()
	newPv := &corev1.PersistentVolume{
		Spec: corev1.PersistentVolumeSpec{
			Capacity: sourcePVC.Spec.Resources.Requests,
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					Driver:           ssc.Driver,
					VolumeHandle:     slaveVolumeHandle,
					ReadOnly:         false,
					VolumeAttributes: options,
					NodeStageSecretRef: &corev1.SecretReference{
						Name:      ssc.NodeStageSecretName,
						Namespace: ssc.NodeStageSecretNamespace,
					},
					ControllerExpandSecretRef: &corev1.SecretReference{
						Name:      ssc.ControllerExpandSecretName,
						Namespace: ssc.ControllerExpandSecretNamespace,
					},
				},
			},
			StorageClassName:              config.DC.SlaveStorageClass,
			AccessModes:                   sourcePVC.Spec.AccessModes,
			PersistentVolumeReclaimPolicy: deletionPolicy,
			//MountOptions:                  pvc.Spec.MountOptions,
			VolumeMode: sourcePVC.Spec.VolumeMode,
		},
	}
	return newPv
}

func CreateRestoreStaticPVCDef(restorePVCName, storageClassName string, volumeBackup *hitoseacomv1.VolumeBackup) *corev1.PersistentVolumeClaim {
	sourcePVC := volumeBackup.PersistentVolumeClaim.DeepCopy()
	newPvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        restorePVCName,
			Labels:      sourcePVC.Labels,
			Annotations: sourcePVC.Annotations,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      sourcePVC.Spec.AccessModes,
			Resources:        sourcePVC.Spec.Resources,
			VolumeMode:       sourcePVC.Spec.VolumeMode,
			StorageClassName: &storageClassName,
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

func (r *VirtualMachineRestoreReconciler) getSecret(namespace, name string) (map[string]string, error) {
	// 通过 secret 名称和命名空间获取 secret 对象
	secret := &corev1.Secret{}
	if err := r.Client.Get(context.Background(), client.ObjectKey{Name: name, Namespace: namespace}, secret); err != nil {
		r.Log.Error(err, "get secret")
		return nil, err
	}
	secretToMap := func() map[string]string {
		dataMap := make(map[string]string)
		for key, value := range secret.Data {
			dataMap[key] = string(value)
		}
		return dataMap
	}()
	return secretToMap, nil
}
