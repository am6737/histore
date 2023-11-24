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
	"github.com/am6737/histore/pkg/ceph/rbd"
	"github.com/am6737/histore/pkg/ceph/util"
	"github.com/am6737/histore/pkg/config"
	librbd "github.com/ceph/go-ceph/rbd"
	"github.com/go-logr/logr"
	vsv1 "github.com/kubernetes-csi/external-snapshotter/client/v6/apis/volumesnapshot/v1"
	"google.golang.org/grpc/codes"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/record"
	"regexp"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"strings"
	"time"

	hitoseacomv1 "github.com/am6737/histore/api/v1"
)

const (
	snapshotSourceNameLabel      = "snapshot.hitosea.com/source-vm-name"
	snapshotSourceNamespaceLabel = "snapshot.hitosea.com/source-vm-namespace"
	snapshotSecretName           = "csi.storage.k8s.io/snapshotter-secret-name"
	snapshotSecretNamespace      = "csi.storage.k8s.io/snapshotter-secret-namespace"

	SnapshotAnnotationParameterPrefix = "snapshot.storage.kubernetes.io/"

	prefixedSnapshotDeleteSecretNameKey      = SnapshotAnnotationParameterPrefix + "deletion-secret-name"      // name key for secret
	prefixedSnapshotDeleteSecretNamespaceKey = SnapshotAnnotationParameterPrefix + "deletion-secret-namespace" // name key for secret

	volumeSnapshotMissingEvent = "VolumeSnapshotMissing"
	volumeCloneCreateEvent     = "SuccessfulVolumeCloneCreate"
)

var (
	volumePromotionKnownErrors    = []codes.Code{codes.FailedPrecondition}
	disableReplicationKnownErrors = []codes.Code{codes.NotFound}
)

// VirtualMachineSnapshotContentReconciler reconciles a VirtualMachineSnapshotContent object
type VirtualMachineSnapshotContentReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
	Log      logr.Logger

	Vc VolumeController
}

//+kubebuilder:rbac:groups=hitosea.com,resources=virtualmachinesnapshotcontents,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=hitosea.com,resources=virtualmachinesnapshotcontents/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=hitosea.com,resources=virtualmachinesnapshotcontents/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the VirtualMachineSnapshotContent object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.14.4/pkg/reconcile
func (r *VirtualMachineSnapshotContentReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	content := &hitoseacomv1.VirtualMachineSnapshotContent{}
	if err := r.Get(ctx, req.NamespacedName, content); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		logger.Error(err, "Unable to obtain VirtualMachineSnapshot object")
		return ctrl.Result{}, err
	}

	if vmSnapshotContentDeleting(content) {
		//SecretName := content.Annotations[prefixedSnapshotDeleteSecretNameKey]
		//SecretNamespace := content.Annotations[prefixedSnapshotDeleteSecretNamespaceKey]
		//secret, err := r.getSecret(SecretNamespace, SecretName)
		//if err != nil {
		//	logger.Error(err, "getSecret")
		//	return reconcile.Result{RequeueAfter: 15 * time.Second}, err
		//}
		if err := r.volumeDeleteHandler(content); err != nil {
			logger.Error(err, "volume deleteHandle")
			return reconcile.Result{RequeueAfter: 5 * time.Second}, nil
		}
		if err := r.removeFinalizerFromVmsc(content); err != nil {
			logger.Error(err, "Failed to remove VirtualMachineSnapshotContent finalizer")
			return reconcile.Result{RequeueAfter: 5 * time.Second}, nil
		}
		return reconcile.Result{}, nil
	} else {
		if !contains(content.Finalizers, vmSnapshotContentFinalizer) {
			if err := r.addFinalizerToVmsc(content); err != nil {
				logger.Error(err, "Failed to add VirtualMachineSnapshotContent finalizer")
				return reconcile.Result{}, err
			}
		}
	}

	if content.Status == nil {
		f := false
		content.Status = &hitoseacomv1.VirtualMachineSnapshotContentStatus{
			ReadyToUse:   &f,
			CreationTime: currentTime(),
			VolumeStatus: nil,
			Error:        nil,
		}
		for _, v := range content.Spec.VolumeBackups {
			content.Status.VolumeStatus = append(content.Status.VolumeStatus, hitoseacomv1.VolumeStatus{
				VolumeName: v.VolumeName,
				Phase:      hitoseacomv1.WaitForCreation,
				ReadyToUse: false,
			})
		}
		if err := r.Status().Update(ctx, content); err != nil {
			return reconcile.Result{
				RequeueAfter: 15 * time.Second,
			}, nil
		}
	}

	//currentlyCreated := vmSnapshotContentCreated(content)
	currentlyError := content.Status.Error != nil

	//if content.Status.ReadyToUse {
	//	r.Log.Info("VirtualMachineSnapshotContent已完成", "namespace", content.Namespace, "name", content.Name)
	//	return ctrl.Result{}, nil
	//}

	//var deletedSnapshots, skippedSnapshots []string
	var completionList []string
	for k, volumeBackup := range content.Spec.VolumeBackups {
		if volumeBackup.VolumeSnapshotName == nil {
			continue
		}
		if volumeBackup.VolumeName == content.Status.VolumeStatus[k].VolumeName && content.Status.VolumeStatus[k].ReadyToUse {
			continue
		}

		vsName := *volumeBackup.VolumeSnapshotName

		pv, err := r.getPVFromPVCName(ctx, volumeBackup.PersistentVolumeClaim.Namespace, volumeBackup.PersistentVolumeClaim.Name)
		if err != nil {
			r.Log.Error(err, "getPVFromPVCName")
			return ctrl.Result{}, err
		}

		// check if snapshot was deleted
		//if currentlyCreated {
		//	logger.Info(fmt.Sprintf("VolumeSnapshot %s no longer exists", vsName))
		//	r.Recorder.Eventf(
		//		content,
		//		corev1.EventTypeWarning,
		//		volumeSnapshotMissingEvent,
		//		"VolumeSnapshot %s no longer exists",
		//		vsName,
		//	)
		//	deletedSnapshots = append(deletedSnapshots, vsName)
		//}

		if currentlyError {
			log.Log.V(1).Info(fmt.Sprintf("Not creating snapshot %s because in error state", vsName))
			//skippedSnapshots = append(skippedSnapshots, vsName)
			continue
		}

		success, err := r.CreateVolume(ctx, pv.Spec.CSI.VolumeHandle, content, &volumeBackup)
		if err != nil {
			r.Log.Error(err, "CreateVolume")
			continue
		}

		if success {
			completionList = append(completionList, volumeBackup.VolumeName)
		}
	}

	//// 更新 ReadyToUse 状态
	updateReadyToUseStatus := func() error {
		newContent := &hitoseacomv1.VirtualMachineSnapshotContent{}
		if err := r.Get(ctx, req.NamespacedName, newContent); err != nil {
			return err
		}
		if content.Status == nil {
			return nil
		}

		complete := 0
		for _, vStatus := range newContent.Status.VolumeStatus {
			if vStatus.ReadyToUse {
				complete++
			}
		}
		if complete == len(newContent.Status.VolumeStatus) {
			// 如果所有卷都匹配，设置 ReadyToUse 为 true
			t := true
			newContent.Status.ReadyToUse = &t
			if err := r.Status().Update(ctx, newContent); err != nil {
				return err
			}
			for _, ownerRef := range content.OwnerReferences {
				if ownerRef.Kind == "VirtualMachineSnapshot" {
					vmSnapshot := &hitoseacomv1.VirtualMachineSnapshot{}
					if err := r.Get(ctx, types.NamespacedName{Name: ownerRef.Name, Namespace: content.Namespace}, vmSnapshot); err != nil {
						return err
					}
					if !*vmSnapshot.Status.ReadyToUse {
						vmSnapshot.Status.ReadyToUse = &t
						if err := r.Status().Update(ctx, vmSnapshot); err != nil {
							return err
						}
					}
				}
			}
		}
		return nil
	}

	if err := updateReadyToUseStatus(); err != nil {
		return reconcile.Result{
			RequeueAfter: 15 * time.Second,
		}, nil
	}

	return ctrl.Result{}, nil
}

func (r *VirtualMachineSnapshotContentReconciler) getPVFromPVCName(ctx context.Context, namespace, name string) (*corev1.PersistentVolume, error) {
	pvc := &corev1.PersistentVolumeClaim{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, pvc); err != nil {
		return nil, fmt.Errorf("failed to get PVC %s: %w", name, err)
	}
	pvName := ""
	if pvc.Spec.VolumeName != "" {
		pvName = pvc.Spec.VolumeName
	} else {
		// If neither VolumeName nor annotations are present, return an error
		return nil, fmt.Errorf("PVC %s is not yet bound to a PV", name)

	}
	pv := &corev1.PersistentVolume{}
	if err := r.Get(ctx, types.NamespacedName{Name: pvName}, pv); err != nil {
		return nil, fmt.Errorf("failed to get PV %s: %w", pvName, err)
	}
	return pv.DeepCopy(), nil
}

func translateError(e *hitoseacomv1.Error) *hitoseacomv1.Error {
	if e == nil {
		return nil
	}
	return &hitoseacomv1.Error{
		Message: e.Message,
		Time:    e.Time,
	}
}

func vmSnapshotContentCreated(vmSnapshotContent *hitoseacomv1.VirtualMachineSnapshotContent) bool {
	return vmSnapshotContent.Status.CreationTime != nil
}

func vmSnapshotContentDeleting(content *hitoseacomv1.VirtualMachineSnapshotContent) bool {
	return content != nil && content.DeletionTimestamp != nil
}

// variable so can be overridden in tests
var currentTime = func() *metav1.Time {
	t := metav1.Now()
	return &t
}

// SetupWithManager sets up the controller with the Manager.
func (r *VirtualMachineSnapshotContentReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		//WithEventFilter(predicate.Funcs{
		//	DeleteFunc: r.deleteVolumeHandler,
		//}).
		For(&hitoseacomv1.VirtualMachineSnapshotContent{}).
		//Owns(&hitoseacomv1.VirtualMachineSnapshot{}).
		Complete(r)
}

//func (r *VirtualMachineSnapshotContentReconciler) deleteVolumeHandler(e event.DeleteEvent) bool {
//	vmSnapshot, ok := e.Object.(*hitoseacomv1.VirtualMachineSnapshot)
//	if !ok {
//		return false
//	}
//	content := &hitoseacomv1.VirtualMachineSnapshotContent{}
//	if err := r.Client.Get(context.TODO(), client.ObjectKey{Namespace: vmSnapshot.Namespace, Name: *vmSnapshot.Status.VirtualMachineSnapshotContentName}, content); err != nil {
//		r.Log.Error(err, "get VirtualMachineSnapshotContent")
//		return false
//	}
//	//dump.Println("deleteVolumeHandler vmSnapshot => ", vmSnapshot.Status)
//	SecretName := content.Annotations[prefixedSnapshotDeleteSecretNameKey]
//	SecretNamespace := content.Annotations[prefixedSnapshotDeleteSecretNamespaceKey]
//	// check if the object is being deleted
//
//	secret, err := r.getSecret(SecretNamespace, SecretName)
//	if err != nil {
//		r.Log.Error(err, "getSecret")
//		return false
//	}
//	if r.volumeDeleteHandler(content, secret) != nil {
//		r.Log.Error(err, "volumeDeleteHandler")
//		return false
//	}
//
//	return true
//}

func (r *VirtualMachineSnapshotContentReconciler) volumeDeleteHandler(content *hitoseacomv1.VirtualMachineSnapshotContent) error {

	msc, err := config.GetCephCsiConfigForSC(r.Client, config.DC.MasterStorageClass)
	if err != nil {
		r.Log.Error(err, "getCephCsiConfigForSC")
		return err
	}

	masterSecret, err := r.getSecret(msc.NodeStageSecretNamespace, msc.NodeStageSecretName)
	if err != nil {
		r.Log.Error(err, "getSecret")
		return err
	}

	slaveSecret, err := r.getSecretMapForSC(config.DC.SlaveStorageClass)
	if err != nil {
		return err
	}

	for _, v := range content.Status.VolumeStatus {
		r.Log.Info("volume deleting", "volumeHandle", v.SlaveVolumeHandle)
		if !v.ReadyToUse {
			masterVolumeHandle := r.getSlaveVolumeHandle(v.SlaveVolumeHandle, msc.ClusterID)
			if err = r.DeleteVolumeSnapshot(context.Background(), masterVolumeHandle, masterSecret, map[string]string{}); err != nil {
				if errors.Is(err, librbd.ErrNotFound) {
					r.Log.Info(fmt.Sprintf("master source Volume ID %s not found", masterVolumeHandle))
					return nil
				}
				return fmt.Errorf(fmt.Sprintf("Failed to remove volume %s", v.SlaveVolumeHandle))
			}
		} else {
			if err = r.DeleteVolumeSnapshot(context.Background(), v.SlaveVolumeHandle, slaveSecret, map[string]string{}); err != nil {
				if errors.Is(err, librbd.ErrNotFound) {
					r.Log.Info(fmt.Sprintf("master source Volume ID %s not found", v.SlaveVolumeHandle))
					return nil
				}
				return fmt.Errorf(fmt.Sprintf("Failed to remove volume %s", v.SlaveVolumeHandle))
			}
		}

	}
	return nil
}

func (r *VirtualMachineSnapshotContentReconciler) DeleteVolumeSnapshot(ctx context.Context, volumeHandle string, secrets, parameters map[string]string) error {

	_, err := r.Vc.DeleteVolume(ctx, &DeleteVolumeRequest{
		VolumeId: volumeHandle,
		Secrets:  secrets,
	})
	if err != nil {
		return err
	}

	return err
}

func (r *VirtualMachineSnapshotContentReconciler) updateVolumeStatus(content *hitoseacomv1.VirtualMachineSnapshotContent, newVolumeStatus hitoseacomv1.VolumeStatus) error {
	// Find the index of the target volumeName in volumeStatus slice
	var targetIndex int
	for i, vStatus := range content.Status.VolumeStatus {
		if vStatus.VolumeName == newVolumeStatus.VolumeName {
			targetIndex = i
			break
		}
	}
	//if !equality.Semantic.DeepEqual(content.Status.VolumeStatus[targetIndex], newVolumeStatus) {
	//	fmt.Println("updateVolumeStatus !equality.Semantic.DeepEqual")
	//	dump.Println("old Status => ", content.Status.VolumeStatus[targetIndex])
	//	dump.Println("new Status => ", newVolumeStatus)
	//}
	if newVolumeStatus.CreationTime != nil {
		content.Status.VolumeStatus[targetIndex].CreationTime = newVolumeStatus.CreationTime
	}
	if newVolumeStatus.MasterVolumeHandle != "" {
		content.Status.VolumeStatus[targetIndex].MasterVolumeHandle = newVolumeStatus.MasterVolumeHandle
	}
	if newVolumeStatus.SlaveVolumeHandle != "" {
		content.Status.VolumeStatus[targetIndex].SlaveVolumeHandle = newVolumeStatus.SlaveVolumeHandle
	}
	if newVolumeStatus.VolumeName != "" {
		content.Status.VolumeStatus[targetIndex].VolumeName = newVolumeStatus.VolumeName
	}
	if newVolumeStatus.Phase != 0 {
		content.Status.VolumeStatus[targetIndex].Phase = newVolumeStatus.Phase
	}
	content.Status.VolumeStatus[targetIndex].ReadyToUse = newVolumeStatus.ReadyToUse
	return r.Status().Update(context.Background(), content)
}

func (r *VirtualMachineSnapshotContentReconciler) getSecretMapForSC(storageClass string) (map[string]string, error) {
	msc, err := config.GetCephCsiConfigForSC(r.Client, storageClass)
	if err != nil {
		r.Log.Error(err, "getCephCsiConfigForSC")
		return nil, err
	}

	secret, err := r.getSecret(msc.NodeStageSecretNamespace, msc.NodeStageSecretName)
	if err != nil {
		r.Log.Error(err, "getSecret")
		return nil, err
	}

	return secret, nil
}

func (r *VirtualMachineSnapshotContentReconciler) WaitFlattenCompleted(ctx context.Context, volumeHandle string, cr *util.Credentials, secrets map[string]string, maxWait time.Duration) error {
	rbdSnap := &rbd.RbdSnapshot{}
	if err := rbd.GenSnapFromSnapID(ctx, rbdSnap, volumeHandle, cr, secrets); err != nil {
		return err
	}

	rbdVol := rbd.GenerateVolFromSnap(rbdSnap)

	if err := rbdVol.Connect(cr); err != nil {
		return err
	}
	defer rbdVol.Destroy()

	if _, err := rbdVol.IsFlattenCompleted(maxWait); err != nil {
		return err
	}

	return nil
}

func (r *VirtualMachineSnapshotContentReconciler) CreateVolume(ctx context.Context, masterVolumeHandle string,
	content *hitoseacomv1.VirtualMachineSnapshotContent,
	volumeBackup *hitoseacomv1.VolumeBackup,
) (bool, error) {

	var phase hitoseacomv1.VirtualMachineSnapshotContentPhase
	const (
		scheduleSyncPeriod = 5 * time.Second
		TTL                = 3 * time.Minute
	)

	ct := &hitoseacomv1.VirtualMachineSnapshotContent{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: content.Namespace, Name: content.Name}, ct); err != nil {
		return false, err
	}
	contentCpy := ct.DeepCopy()
	index := 0
	is := false
	for k, v := range contentCpy.Status.VolumeStatus {
		if v.VolumeName == volumeBackup.VolumeName {
			is = true
			index = k
			break
		}
	}
	if !is {
		r.Log.Info("status异常")
		return false, nil
	}
	if contentCpy.Status.VolumeStatus[index].ReadyToUse == true {
		return false, nil
	}

	msc, err := config.GetCephCsiConfigForSC(r.Client, config.DC.MasterStorageClass)
	if err != nil {
		r.Log.Error(err, "getCephCsiConfigForSC")
		return false, err
	}

	masterSecret, err := r.getSecret(msc.NodeStageSecretNamespace, msc.NodeStageSecretName)
	if err != nil {
		r.Log.Error(err, "getSecret")
		return false, err
	}

	masterCr, err := util.NewUserCredentials(masterSecret)
	if err != nil {
		r.Log.Error(err, "NewUserCredentials")
		return false, err
	}
	defer masterCr.DeleteCredentials()

	ssc, err := config.GetCephCsiConfigForSC(r.Client, config.DC.SlaveStorageClass)
	if err != nil {
		r.Log.Error(err, "getCephCsiConfigForSC")
		return false, err
	}

	slaveSecret, err := r.getSecret(ssc.NodeStageSecretNamespace, ssc.NodeStageSecretName)
	if err != nil {
		r.Log.Error(err, "getSecret")
		return false, err
	}

	slaveCr, err := util.NewUserCredentials(slaveSecret)
	if err != nil {
		r.Log.Error(err, "NewUserCredentials")
		return false, err
	}
	defer slaveCr.DeleteCredentials()

	EnabledImageHandler := func(rbdVol *rbd.RbdVolume) error {
		if err = wait.PollImmediate(scheduleSyncPeriod, TTL, func() (done bool, err error) {
			if err = rbdVol.EnableImageMirroring(librbd.ImageMirrorModeSnapshot); err != nil {
				if strings.Contains(err.Error(), "Device or resource busy") {
					return false, nil
				}
				r.Log.Error(err, "Demote master rbd failed")
				return false, err
			}
			r.Log.Info("Master RBD image enabled successfully", "image", fmt.Sprintf("%s/%s", rbdVol.Pool, rbdVol.RbdImageName))
			return true, nil
		}); err != nil {
			if err == wait.ErrWaitTimeout {
				// todo 超时处理
				fmt.Println("Operation timed out")
				return err
			}
			return err
		}
		return r.updateVolumeStatus(contentCpy, hitoseacomv1.VolumeStatus{
			VolumeName: volumeBackup.VolumeName,
			Phase:      hitoseacomv1.VolumeDemote,
		})
	}

	DemoteImageHandler := func(rbdVol *rbd.RbdVolume) error {
		state, err := rbdVol.GetImageMirroringInfo()
		if err != nil {
			return err
		}
		if !state.Primary {
			return r.updateVolumeStatus(contentCpy, hitoseacomv1.VolumeStatus{
				VolumeName: volumeBackup.VolumeName,
				Phase:      hitoseacomv1.VolumePromote,
			})
		}
		//r.Log.Info("Wait master RBD image demote  ", "image", fmt.Sprintf("%s/%s", rbdVol.Pool, rbdVol.RbdImageName))
		if err = wait.PollImmediate(scheduleSyncPeriod, TTL, func() (done bool, err error) {
			if err = rbdVol.DemoteImage(); err != nil {
				if strings.Contains(err.Error(), "Device or resource busy") {
					return false, nil
				}
				r.Log.Error(err, "demote master rbd failed")
				return false, err
			}
			r.Log.Info("Master RBD image demote successfully", "image", fmt.Sprintf("%s/%s", rbdVol.Pool, rbdVol.RbdImageName))
			return true, nil
		}); err != nil {
			if err == wait.ErrWaitTimeout {
				// todo 超时处理
				fmt.Println("Operation timed out")
				return err
			}
			return err
		}
		return r.updateVolumeStatus(contentCpy, hitoseacomv1.VolumeStatus{
			VolumeName: volumeBackup.VolumeName,
			Phase:      hitoseacomv1.VolumePromote,
		})
	}

	PromoteImageHandler := func(rbdVol *rbd.RbdVolume) error {
		state, err := rbdVol.GetImageMirroringInfo()
		if err != nil {
			return err
		}
		if state.Primary {
			return r.updateVolumeStatus(contentCpy, hitoseacomv1.VolumeStatus{
				VolumeName: volumeBackup.VolumeName,
				Phase:      hitoseacomv1.DisableReplication,
			})
		}
		//r.Log.Info("Wait slave RBD image promote  ", "image", fmt.Sprintf("%s/%s", rbdVol.Pool, rbdVol.RbdImageName))
		if err = wait.PollImmediate(scheduleSyncPeriod, TTL, func() (done bool, err error) {
			if err = rbdVol.PromoteImage(false); err != nil {
				if strings.Contains(err.Error(), "Device or resource busy") {
					return false, nil
				}
				r.Log.Error(err, "Promote slave rbd failed")
				return false, err
			}
			r.Log.Info("Slave RBD image promote successfully", "image", fmt.Sprintf("%s/%s", rbdVol.Pool, rbdVol.RbdImageName))
			return true, nil
		}); err != nil {
			return err
		}
		return r.updateVolumeStatus(contentCpy, hitoseacomv1.VolumeStatus{
			VolumeName: volumeBackup.VolumeName,
			Phase:      hitoseacomv1.DisableReplication,
		})
	}

	DisableImageHandler := func(rbdVol *rbd.RbdVolume) error {
		if err = wait.PollImmediate(scheduleSyncPeriod, TTL, func() (done bool, err error) {
			if err = rbdVol.DisableImageMirroring(false); err != nil {
				if strings.Contains(err.Error(), "Device or resource busy") {
					return false, nil
				}
				r.Log.Error(err, "Disable slave rbd image failed")
			}
			r.Log.Info("Slave RBD image disable successfully", "image", fmt.Sprintf("%s/%s", rbdVol.Pool, rbdVol.RbdImageName))
			return true, nil
		}); err != nil {
			return err
		}
		if err = r.updateVolumeStatus(contentCpy, hitoseacomv1.VolumeStatus{
			VolumeName: volumeBackup.VolumeName,
			Phase:      hitoseacomv1.Complete,
		}); err != nil {
			return err
		}
		return nil
	}

	var (
		cloneRbd          *rbd.RbdVolume
		slaveRbd          *rbd.RbdVolume
		slaveVolumeHandle string
	)

	phase = contentCpy.Status.VolumeStatus[index].Phase

	waitForCreation := func() (string, error) {
		clusterID, err := GetClusterIDFromVolumeHandle(masterVolumeHandle)
		if err != nil {
			r.Log.Error(err, "GetClusterIDFromVolumeHandle")
			return "", err
		}

		vol, err := r.Vc.CreateVolume(ctx, &CreateVolumeRequest{
			Name:         string(content.UID),
			VolumeId:     masterVolumeHandle,
			Parameters:   map[string]string{"clusterID": clusterID},
			MaterSecrets: masterSecret,
			SlaveSecrets: slaveSecret,
		})
		if err != nil {
			return "", err
		}
		deleteClone := false
		defer func() {
			if deleteClone {
				if _, err = r.Vc.DeleteVolume(ctx, &DeleteVolumeRequest{
					VolumeId: vol.VolumeId,
					Secrets:  map[string]string{"clusterID": clusterID},
				}); err != nil {
					r.Log.Error(err, "delete volume failed")
				}
			}
		}()

		slaveVolumeHandle = r.getSlaveVolumeHandle(vol.VolumeId, ssc.ClusterID)

		r.Log.Info(fmt.Sprintf("Waiting for volume flatten"), "VolumeId", vol.VolumeId)

		if err = r.WaitFlattenCompleted(ctx, vol.VolumeId, masterCr, masterSecret, 5*time.Minute); err != nil {
			r.Log.Error(err, "Wait volume flatten timeout")
			deleteClone = true
			return "", err
		}

		r.Log.Info(fmt.Sprintf("Volume flatten successfully"), "VolumeId", vol.VolumeId)

		if err = r.updateVolumeStatus(content, hitoseacomv1.VolumeStatus{
			CreationTime:       currentTime(),
			VolumeName:         volumeBackup.VolumeName,
			Phase:              hitoseacomv1.EnableReplication,
			SlaveVolumeHandle:  slaveVolumeHandle,
			MasterVolumeHandle: masterVolumeHandle,
		}); err != nil {
			deleteClone = true
			r.Log.Error(err, "WaitForCreation updateVolumeStatus")
			return "", err
		}

		return vol.VolumeId, nil
	}

	if phase == hitoseacomv1.WaitForCreation {
		_, err = waitForCreation()
		if err != nil {
			r.Log.Error(err, "waitForCreation")
			return false, err
		}
		//cloneRbd, err = rbd.GenVolFromVolID(ctx, VolumeId, masterCr, masterSecret)
		//if err != nil {
		//	r.Log.Error(err, "GenVolFromVolID")
		//	return false, err
		//}
		//defer cloneRbd.Destroy()
		phase++
	}

	if phase == hitoseacomv1.EnableReplication {
		slaveVolumeHandle = contentCpy.Status.VolumeStatus[index].SlaveVolumeHandle
		if cloneRbd == nil {
			cloneRbd, err = rbd.GenVolFromVolID(ctx, r.getSlaveVolumeHandle(slaveVolumeHandle, msc.ClusterID), masterCr, masterSecret)
			if err != nil {
				r.Log.Error(err, "GenVolFromVolID")
				return false, err
			}
			defer cloneRbd.Destroy()
		}

		if err = EnabledImageHandler(cloneRbd); err != nil {
			r.Log.Error(err, "EnabledImageHandler")
			return false, err
		}
		phase++
	}

	if phase != hitoseacomv1.Complete {
		slaveVolumeHandle = contentCpy.Status.VolumeStatus[index].SlaveVolumeHandle
		if cloneRbd == nil {
			cloneRbd, err = rbd.GenVolFromVolID(ctx, r.getSlaveVolumeHandle(slaveVolumeHandle, msc.ClusterID), masterCr, masterSecret)
			if err != nil {
				r.Log.Error(err, "GenVolFromVolID")
				return false, err
			}
			defer cloneRbd.Destroy()
		}

		if err = wait.PollImmediate(scheduleSyncPeriod, TTL, func() (done bool, err error) {
			if config.SlavePoolID != 0 {
				slaveVolumeHandle = replacePool(slaveVolumeHandle, config.SlavePoolID)
			}
			slaveRbd, err = rbd.GenVolFromVolID(ctx, slaveVolumeHandle, slaveCr, slaveSecret)
			if err != nil {
				if errors.Is(err, librbd.ErrNotFound) {
					return false, nil
				}
				r.Log.Error(err, "failed to get slave rbd")
				return false, nil
			}
			return true, nil
		}); err != nil {
			if err == wait.ErrWaitTimeout {
				return false, fmt.Errorf("slave source Volume ID %s not found", slaveVolumeHandle)
			}
			return false, err
		}
	}

	for {
		switch phase {
		case hitoseacomv1.DisableReplication:
			if err = DisableImageHandler(slaveRbd); err != nil {
				r.Log.Error(err, "DisableImageHandler")
				return false, err
			}
			phase++
		case hitoseacomv1.Complete:
			slaveVolumeHandle = content.Status.VolumeStatus[index].SlaveVolumeHandle
			t := true
			if err = r.updateVolumeStatus(contentCpy, hitoseacomv1.VolumeStatus{
				VolumeName: volumeBackup.VolumeName,
				Phase:      hitoseacomv1.Complete,
				ReadyToUse: t,
			}); err != nil {
				r.Log.Error(err, "updateVolumeStatus Complete")
				return false, err
			}
			r.Recorder.Eventf(contentCpy, corev1.EventTypeNormal, volumeCloneCreateEvent, fmt.Sprintf("Successfully created VolumeHandle %s", slaveVolumeHandle))
			return true, nil
		default:
			status := contentCpy.Status.VolumeStatus[index]
			if err = r.waitForSlaveImageSync(status, cloneRbd, slaveRbd, DemoteImageHandler, PromoteImageHandler); err != nil {
				r.Log.Error(err, "waitForSlaveImageSync")
				return false, err
			}
			if err = r.waitForMasterImageSync(cloneRbd, slaveRbd, DisableImageHandler); err != nil {
				r.Log.Error(err, "waitForMasterImageSync")
				return false, err
			}
			phase++
		}
	}
}

func (r *VirtualMachineSnapshotContentReconciler) waitForMasterImageSync(masterRbd, slaveRbd *rbd.RbdVolume, DisableImageHandler func(slaveRBD *rbd.RbdVolume) error) error {
	const (
		scheduleSyncPeriod = 5 * time.Second
		TTL                = 3 * time.Minute
	)
	if err := wait.PollImmediate(scheduleSyncPeriod, TTL, func() (done bool, err error) {
		masterRbdImageStatus, err := masterRbd.GetImageMirroringStatus()
		if err != nil {
			fmt.Println("masterRbd.GetImageMirroringStatus err ", err)
			return false, nil
		}

		for _, mrs := range masterRbdImageStatus.SiteStatuses {
			if mrs.MirrorUUID == "" {
				//dump.P("master rbd image status", mrs)
				rs1, err := mrs.DescriptionReplayStatus()
				if err != nil {
					if strings.Contains(err.Error(), "No such file or directory") {
						return false, nil
					}
					return false, err
				}
				if rs1.ReplayState == "idle" {
					if err := DisableImageHandler(slaveRbd); err != nil {
						return false, err
					}
					return true, nil
				}
			}
		}
		return false, nil
	}); err != nil {
		return err
	}

	return nil
}

func (r *VirtualMachineSnapshotContentReconciler) waitForSlaveImageSync(status hitoseacomv1.VolumeStatus, cloneRbd, slaveRbd *rbd.RbdVolume, DemoteImageHandler, PromoteImageHandler func(volRbd *rbd.RbdVolume) (err error)) error {
	const (
		scheduleSyncPeriod = 5 * time.Second
		TTL                = 3 * time.Minute
	)

	slaveRbdHandle := func() error {
		var err error
		// 定义一个变量来跟踪当前步骤
		currentStep := status.Phase
		// 定义条件函数，检查对象状态是否已更新
		rbdConditionFunc := func() (bool, error) {
			time.Sleep(scheduleSyncPeriod)
			// 根据当前步骤执行相应的操作
			switch currentStep {
			case hitoseacomv1.VolumeDemote:
				err = DemoteImageHandler(cloneRbd)
			case hitoseacomv1.VolumePromote:
				err = PromoteImageHandler(slaveRbd)
			}
			if err != nil {
				// 如果发生错误，记录当前步骤，并返回错误
				r.Log.Error(err, fmt.Sprintf("rbdConditionFunc index %v", currentStep))
				return false, err
			}
			currentStep++
			// 如果成功执行当前步骤，将 currentStep 增加 1，准备执行下一步骤
			// 如果所有步骤都执行完毕，返回 true，等待循环将结束
			return currentStep >= 4, nil
		}
		if err = wait.PollImmediate(scheduleSyncPeriod, TTL, rbdConditionFunc); err != nil {
			return err
		}
		return nil
	}

	if err := wait.PollImmediate(scheduleSyncPeriod, TTL, func() (done bool, err error) {
		sRbdStatus, err := slaveRbd.GetImageMirroringStatus()
		if err != nil {
			r.Log.Error(err, "GetImageMirroringStatus err")
			return false, nil
		}
		//dump.Println("sRbdStatus => ", sRbdStatus)
		for _, srs := range sRbdStatus.SiteStatuses {
			if srs.MirrorUUID == "" {
				if strings.Contains(srs.Description, "remote image is not primary") || strings.Contains(srs.Description, "local image is primary") {
					if err := slaveRbdHandle(); err != nil {
						r.Log.Error(err, "slaveRbdHandle")
						return false, nil
					}
					return true, nil
				}
				replayStatus, err := srs.DescriptionReplayStatus()
				if err != nil {
					// 错误包含 "No such file or directory"，忽略此错误
					if strings.Contains(err.Error(), "No such file or directory") {
						return false, nil
					}
					r.Log.Error(err, "replayStatus err")
					return false, nil
				}
				//dump.Println("replayStatus => ", replayStatus)
				if replayStatus.ReplayState == "idle" {
					if err := slaveRbdHandle(); err != nil {
						r.Log.Error(err, "slaveRbdHandle")
						return false, nil
					}
					return true, nil
				}
			}
		}
		return false, nil
	}); err != nil {
		return err
	}

	return nil
}

func (r *VirtualMachineSnapshotContentReconciler) getVolumeSnapshotClass(storageClassName string) (string, error) {

	obj := &storagev1.StorageClass{}
	if err := r.Client.Get(context.TODO(), client.ObjectKey{Name: storageClassName}, obj); err != nil {
		return "", err
	}

	storageClass := obj.DeepCopy()

	var matches []vsv1.VolumeSnapshotClass
	volumeSnapshotClasses := r.getVolumeSnapshotClasses()
	for _, volumeSnapshotClass := range volumeSnapshotClasses {
		if volumeSnapshotClass.Driver == storageClass.Provisioner {
			matches = append(matches, volumeSnapshotClass)
		}
	}

	if len(matches) == 0 {
		//log.Log.Warningf("No VolumeSnapshotClass for %s", storageClassName)
		return "", nil
	}

	if len(matches) == 1 {
		return matches[0].Name, nil
	}

	for _, volumeSnapshotClass := range matches {
		for annotation := range volumeSnapshotClass.Annotations {
			if annotation == defaultVolumeSnapshotClassAnnotation {
				return volumeSnapshotClass.Name, nil
			}
		}
	}

	return "", fmt.Errorf("%d matching VolumeSnapshotClasses for %s", len(matches), storageClassName)
}

func (r *VirtualMachineSnapshotContentReconciler) getVolumeSnapshotClasses() []vsv1.VolumeSnapshotClass {

	objs := &vsv1.VolumeSnapshotClassList{}
	var vscs []vsv1.VolumeSnapshotClass

	if err := r.Client.List(context.TODO(), objs); err != nil {
		return nil
	}

	for _, obj := range objs.Items {
		vsc := obj.DeepCopy()
		vscs = append(vscs, *vsc)
	}

	return vscs
}

func (r *VirtualMachineSnapshotContentReconciler) getVMSnapshot(vmsc *hitoseacomv1.VirtualMachineSnapshotContent) (*hitoseacomv1.VirtualMachineSnapshot, error) {
	vmSnapshot := &hitoseacomv1.VirtualMachineSnapshot{}
	if err := r.Get(context.TODO(), client.ObjectKey{
		Namespace: vmsc.Namespace,
		Name:      *vmsc.Spec.VirtualMachineSnapshotName,
	}, vmSnapshot); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return vmSnapshot, nil
}

func (r *VirtualMachineSnapshotContentReconciler) GetVolumeSnapshot(namespace string, name string) (*vsv1.VolumeSnapshot, error) {
	volumeSnapshot := &vsv1.VolumeSnapshot{}
	if err := r.Client.Get(context.TODO(), client.ObjectKey{
		Namespace: namespace,
		Name:      name,
	}, volumeSnapshot); err != nil {
		if apierrors.IsNotFound(err) {
			// 如果资源不存在，则返回一个空的对象
			return nil, nil
		}
		return nil, err
	}
	return volumeSnapshot.DeepCopy(), nil
}

func (r *VirtualMachineSnapshotContentReconciler) getVolumeSnapshotContent(ctx context.Context, content *hitoseacomv1.VirtualMachineSnapshotContent) (*vsv1.VolumeSnapshotContent, error) {
	volumeContent := &vsv1.VolumeSnapshotContent{}
	volumeSnapshot, err := r.GetVolumeSnapshot(content.Namespace, *content.Spec.VolumeBackups[0].VolumeSnapshotName)
	if err != nil {
		return nil, err
	}
	if volumeSnapshot == nil {
		r.Log.Info("volumeSnapshot is Empty")
		return nil, nil
	}

	if err = r.Client.Get(ctx, client.ObjectKey{Namespace: volumeSnapshot.Namespace, Name: *volumeSnapshot.Status.BoundVolumeSnapshotContentName}, volumeContent); err != nil {
		return nil, err
	}
	return volumeContent.DeepCopy(), nil
}

func (r *VirtualMachineSnapshotContentReconciler) vmSnapshotDeleting(snapshot *hitoseacomv1.VirtualMachineSnapshot) bool {
	return !snapshot.ObjectMeta.DeletionTimestamp.IsZero()
}

func (r *VirtualMachineSnapshotContentReconciler) getSecret(namespace, name string) (map[string]string, error) {
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

func GetVolUUId(originalString string) string {
	re := regexp.MustCompile(`[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`)
	matchIndexes := re.FindAllStringIndex(originalString, -1)
	// 确保至少有两个匹配
	if len(matchIndexes) >= 2 {
		// 获取第二个匹配的位置的内容
		return originalString[matchIndexes[1][0]:matchIndexes[1][1]]
	}
	// 如果没有找到足够的匹配，返回原始字符串
	return originalString
}

func (r *VirtualMachineSnapshotContentReconciler) getSlaveVolumeHandle(volumeHandle, clusterID string) string {
	replaceString := func(input, oldSubstring, newSubstring string) string {
		return strings.Replace(input, oldSubstring, newSubstring, -1)
	}
	re := regexp.MustCompile(`\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b`)
	//slaveVolumeHandle := replaceString(volumeHandle, re.FindString(volumeHandle), "5e709abc-419e-11ee-a132-af7f7bf3bfc0")
	return replaceString(volumeHandle, re.FindString(volumeHandle), clusterID)
}

func replacePool(vh string, newValue int64) string {
	parts := strings.Split(vh, "-")
	newValueString := fmt.Sprintf("%016d", newValue)
	parts[7] = newValueString
	return strings.Join(parts, "-")
}

func GenerateSlaveVolumeHandle(masterVolumeHandle, clusterID, sRbdName string) string {
	re := regexp.MustCompile(`[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`)
	// 查找所有匹配的位置
	matchIndexes := re.FindAllStringIndex(masterVolumeHandle, -1)
	// 确保至少有两个匹配
	if len(matchIndexes) >= 2 {
		// 替换第一个匹配的位置的内容
		result := masterVolumeHandle[:matchIndexes[0][0]] + clusterID + masterVolumeHandle[matchIndexes[0][1]:]
		// 替换第二个匹配的位置的内容
		result = result[:matchIndexes[1][0]] + sRbdName + result[matchIndexes[1][1]:]
		return result
	}
	// 如果没有找到足够的匹配，返回原始字符串
	return masterVolumeHandle
}

func GetCloneVolumeHandleFromVolumeHandle(originalString, replacement string) string {
	re := regexp.MustCompile(`[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`)
	matchIndexes := re.FindAllStringIndex(originalString, -1)
	if len(matchIndexes) >= 2 {
		// 替换第二个匹配的位置的内容
		result := originalString[:matchIndexes[1][0]] + replacement + originalString[matchIndexes[1][1]:]
		return result
	}
	return originalString
}

func GetClusterIDFromVolumeHandle(volumeHandle string) (string, error) {
	re := regexp.MustCompile(`[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`)
	matches := re.FindStringSubmatch(volumeHandle)
	if len(matches) < 1 {
		return "", fmt.Errorf("pattern not found in the volume handle")
	}
	return matches[0], nil
}

func (r *VirtualMachineSnapshotContentReconciler) getSnapshotClassInfo(name string) (*vsv1.VolumeSnapshotClass, error) {
	snapshotClass := &vsv1.VolumeSnapshotClass{}
	if err := r.Client.Get(context.Background(), types.NamespacedName{Name: name}, snapshotClass); err != nil {
		fmt.Println("get snapshotClass ", err)
		return nil, err
	}
	return snapshotClass.DeepCopy(), nil
}
