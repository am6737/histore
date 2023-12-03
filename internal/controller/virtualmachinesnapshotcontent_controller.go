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
	"github.com/google/uuid"
	vsv1 "github.com/kubernetes-csi/external-snapshotter/client/v6/apis/volumesnapshot/v1"
	"google.golang.org/grpc/codes"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	"math/rand"
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
	Vc       VolumeController
	Vs       VolumeStage
}

//+kubebuilder:rbac:groups=snapshot.hitosea.com,resources=virtualmachinesnapshotcontents,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=snapshot.hitosea.com,resources=virtualmachinesnapshotcontents/Status,verbs=get;update;patch
//+kubebuilder:rbac:groups=snapshot.hitosea.com,resources=virtualmachinesnapshotcontents/finalizers,verbs=update

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

	//r.Log.Info(fmt.Sprintf("Req 0 %v", req))

	content := &hitoseacomv1.VirtualMachineSnapshotContent{}
	if err := r.Get(ctx, req.NamespacedName, content); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		logger.Error(err, "unable to obtain VirtualMachineSnapshot object")
		return ctrl.Result{}, err
	}

	if vmSnapshotContentDeleting(content) {
		if err := r.volumeDeleteHandler(ctx, content); err != nil {
			logger.Error(err, "volume deleteHandle")
			return reconcile.Result{RequeueAfter: 5 * time.Second}, nil
		}
		if err := r.removeFinalizerFromVmsc(content); err != nil {
			logger.Error(err, "failed to remove VirtualMachineSnapshotContent finalizer")
			return reconcile.Result{RequeueAfter: 5 * time.Second}, nil
		}
		return ctrl.Result{}, nil
	} else {
		if err := r.addFinalizerToVmsc(content); err != nil {
			logger.Error(err, "failed to add VirtualMachineSnapshotContent finalizer")
			return ctrl.Result{Requeue: true}, nil
		}
	}

	if content.Status == nil {
		f := false
		content.Status = &hitoseacomv1.VirtualMachineSnapshotContentStatus{
			ReadyToUse:   &f,
			CreationTime: currentTime(),
			VolumeStatus: []hitoseacomv1.VolumeStatus{},
			Error:        nil,
		}
		for _, v := range content.Spec.VolumeBackups {
			pv, err := r.getPVFromPVCName(ctx, v.PersistentVolumeClaim.Namespace, v.PersistentVolumeClaim.Name)
			if err != nil {
				r.Log.Error(err, "getPVFromPVCName")
				return ctrl.Result{}, err
			}
			if pv == nil {
				return ctrl.Result{}, fmt.Errorf(fmt.Sprintf("pv %s/%s not found", v.PersistentVolumeClaim.Namespace, v.PersistentVolumeClaim.Name))
			}

			slaveVolumeHandle := GetCloneVolumeHandleFromVolumeHandle(pv.Spec.CSI.VolumeHandle, uuid.New().String())
			content.Status.VolumeStatus = append(content.Status.VolumeStatus, hitoseacomv1.VolumeStatus{
				ReadyToUse:         &f,
				VolumeName:         v.VolumeName,
				MasterVolumeHandle: pv.Spec.CSI.VolumeHandle,
				SlaveVolumeHandle:  slaveVolumeHandle,
			})
		}
		if err := r.Status().Update(ctx, content); err != nil {
			r.Log.Error(err, "r.Status().Update")
			return ctrl.Result{
				RequeueAfter: time.Second,
			}, nil
		}
	}

	//r.Log.Info(fmt.Sprintf("Req 1 %v", req))

	//currentlyCreated := vmSnapshotContentCreated(content)
	currentlyError := content.Status.Error != nil

	//if Content.Status.ReadyToUse {
	//	r.Log.Info("VirtualMachineSnapshotContent已完成", "namespace", Content.Namespace, "name", Content.Name)
	//	return ctrl.Result{}, nil
	//}
	//var deletedSnapshots, skippedSnapshots []string
	//var completionList []string
	for k, volumeBackup := range content.Spec.VolumeBackups {
		if volumeBackup.VolumeSnapshotName == nil {
			continue
		}
		if volumeBackup.VolumeName == content.Status.VolumeStatus[k].VolumeName && *content.Status.VolumeStatus[k].ReadyToUse {
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
		//	continue
		//	//deletedSnapshots = append(deletedSnapshots, vsName)
		//}

		if currentlyError {
			log.Log.V(1).Info(fmt.Sprintf("Not creating snapshot %s because in error state", vsName))
			//skippedSnapshots = append(skippedSnapshots, vsName)
			continue
		}
		//r.Log.Info(fmt.Sprintf("Req 2 %v", req))
		requeue, err := r.createVolume(ctx, pv.Spec.CSI.VolumeHandle, content, &volumeBackup)
		if err != nil {
			r.Log.Error(err, "createVolume")
			return ctrl.Result{}, err
		}
		if requeue {
			//fmt.Println("requeue => ", requeue)
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
	}

	//// 更新 ReadyToUse 状态
	updateReadyToUseStatus := func() error {

		newContent := &hitoseacomv1.VirtualMachineSnapshotContent{}
		if err := r.Get(ctx, req.NamespacedName, newContent); err != nil {
			return err
		}
		if newContent.Status == nil {
			return nil
		}

		complete := 0
		for _, vStatus := range newContent.Status.VolumeStatus {
			if *vStatus.ReadyToUse {
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
			for _, ownerRef := range newContent.OwnerReferences {
				if ownerRef.Kind == "VirtualMachineSnapshot" {
					vmSnapshot, err := r.getVMSnapshot(content)
					if err != nil {
						return err
					}
					if vmSnapshot != nil && !*vmSnapshot.Status.ReadyToUse {
						vmSnapshot.Status.ReadyToUse = &t
						if err = r.Status().Update(ctx, vmSnapshot); err != nil {
							return err
						}
					}
				}
			}
		}

		return nil
	}

	if err := updateReadyToUseStatus(); err != nil {
		r.Log.Error(err, "updateReadyToUseStatus")
		return reconcile.Result{
			Requeue: true,
		}, nil
	}

	return ctrl.Result{}, nil
}

// randomSleep 随机暂停函数，区间为0-10秒
func randomSleep() {
	rand.Seed(time.Now().UnixNano())                       // 使用当前时间作为随机数种子
	duration := time.Duration(rand.Intn(21)) * time.Second // 生成0到10秒的随机时间段
	time.Sleep(duration)
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
	return vmSnapshotContent.Status != nil && vmSnapshotContent.Status.CreationTime != nil
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
//	Content := &hitoseacomv1.VirtualMachineSnapshotContent{}
//	if err := r.Client.Get(context.TODO(), client.ObjectKey{Namespace: vmSnapshot.Namespace, Name: *vmSnapshot.Status.VirtualMachineSnapshotContentName}, Content); err != nil {
//		r.Log.Error(err, "get VirtualMachineSnapshotContent")
//		return false
//	}
//	//dump.Println("deleteVolumeHandler vmSnapshot => ", vmSnapshot.Status)
//	SecretName := Content.Annotations[prefixedSnapshotDeleteSecretNameKey]
//	SecretNamespace := Content.Annotations[prefixedSnapshotDeleteSecretNamespaceKey]
//	// check if the object is being deleted
//
//	secret, err := r.getSecret(SecretNamespace, SecretName)
//	if err != nil {
//		r.Log.Error(err, "getSecret")
//		return false
//	}
//	if r.volumeDeleteHandler(Content, secret) != nil {
//		r.Log.Error(err, "volumeDeleteHandler")
//		return false
//	}
//
//	return true
//}

func (r *VirtualMachineSnapshotContentReconciler) volumeDeleteHandler(ctx context.Context, content *hitoseacomv1.VirtualMachineSnapshotContent) error {

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

	masterCr, err := util.NewUserCredentials(masterSecret)
	if err != nil {
		r.Log.Error(err, "NewUserCredentials")
		return err
	}
	defer masterCr.DeleteCredentials()

	ssc, err := config.GetCephCsiConfigForSC(r.Client, config.DC.SlaveStorageClass)
	if err != nil {
		r.Log.Error(err, "getCephCsiConfigForSC")
		return err
	}

	slaveSecret, err := r.getSecretMapForSC(config.DC.SlaveStorageClass)
	if err != nil {
		return err
	}

	slaveCr, err := util.NewUserCredentials(slaveSecret)
	if err != nil {
		r.Log.Error(err, "NewUserCredentials")
		return err
	}
	defer slaveCr.DeleteCredentials()

	var success bool

	for _, v := range content.Status.VolumeStatus {
		r.Log.Info("volume deleting", "volumeHandle", v.SlaveVolumeHandle)
		if v.SlaveVolumeHandle == "" {
			continue
		}

		switch {
		case v.Phase == hitoseacomv1.Create:
			cloneRbd, err := rbd.GenVolFromVolID(ctx, v.SlaveVolumeHandle, masterCr, masterSecret)
			if err != nil {
				if errors.Is(err, librbd.ErrNotFound) {
					continue
				}
			}
			defer cloneRbd.Destroy()

			if cloneRbd.CreatedAt != nil {
				if _, err = r.Vc.DeleteVolume(ctx, &DeleteVolumeRequest{
					VolumeId: v.SlaveVolumeHandle,
					Secrets:  masterSecret,
				}); err != nil {
					return err
				}
			}

		case v.Phase <= hitoseacomv1.Demote:
			cloneRbd, err := rbd.GenVolFromVolID(ctx, v.SlaveVolumeHandle, masterCr, masterSecret)
			if err != nil {
				if !errors.Is(err, librbd.ErrNotFound) {
					return err
				}
			}
			defer cloneRbd.Destroy()

			if cloneRbd.CreatedAt != nil {
				state, err := cloneRbd.GetImageMirroringInfo()
				if err != nil {
					return err
				}
				if state.State != librbd.MirrorImageDisabled {
					if !state.Primary {
						success, err = r.Vs.Promote(ctx, cloneRbd, true)
						if err != nil {
							r.Log.Error(err, "failed Promote")
							return err
						}
					} else {
						success = true
					}
					if state.State == librbd.MirrorImageEnabled {
						if success {
							success, err = r.Vs.Disable(ctx, cloneRbd, true)
							if err != nil {
								return err
							}
						}
					} else {
						success = true
					}
				} else {
					success = true
				}
				if success {
					if _, err = r.Vc.DeleteVolume(ctx, &DeleteVolumeRequest{
						VolumeId: v.SlaveVolumeHandle,
						Secrets:  masterSecret,
					}); err != nil {
						return err
					}
				}
			}

			slaveRbd, err := rbd.GenVolFromVolID(ctx, getSlaveVolumeHandle(v.SlaveVolumeHandle, ssc.ClusterID), slaveCr, slaveSecret)
			if err != nil {
				if errors.Is(err, librbd.ErrNotFound) {
					continue
				}
				r.Log.Error(err, "failed to get slave rbd")
				return err
			}
			defer slaveRbd.Destroy()

			if slaveRbd.CreatedAt != nil {
				state, err := slaveRbd.GetImageMirroringInfo()
				if err != nil {
					return err
				}
				if state.State != librbd.MirrorImageDisabled {
					if !state.Primary {
						success, err = r.Vs.Promote(ctx, slaveRbd, true)
						if err != nil {
							r.Log.Error(err, "failed Promote")
							return err
						}
					} else {
						success = true
					}
					if state.State == librbd.MirrorImageEnabled {
						if success {
							success, err = r.Vs.Disable(ctx, slaveRbd, true)
							if err != nil {
								return err
							}
						}
					} else {
						success = true
					}
				} else {
					success = true
				}
				if success {
					if _, err = r.Vc.DeleteVolume(ctx, &DeleteVolumeRequest{
						VolumeId: v.SlaveVolumeHandle,
						Secrets:  masterSecret,
					}); err != nil {
						return err
					}
				}
			}

		case v.Phase >= hitoseacomv1.Promote:
			volumeHandle := getSlaveVolumeHandle(v.SlaveVolumeHandle, msc.ClusterID)
			cloneRbd, err := rbd.GenVolFromVolID(ctx, volumeHandle, masterCr, masterSecret)
			if err != nil {
				if !errors.Is(err, librbd.ErrNotFound) {
					return err
				}
			}
			defer cloneRbd.Destroy()

			if cloneRbd.CreatedAt != nil {
				mstate, err := cloneRbd.GetImageMirroringInfo()
				if err != nil {
					return err
				}
				if mstate.State != librbd.MirrorImageDisabled {
					if !mstate.Primary {
						success, err = r.Vs.Promote(ctx, cloneRbd, true)
						if err != nil {
							return err
						}
					} else {
						success = true
					}
					if mstate.State == librbd.MirrorImageEnabled {
						if success {
							success, err = r.Vs.Disable(ctx, cloneRbd, true)
							if err != nil {
								return err
							}
						}
					} else {
						success = true
					}
				} else {
					success = true
				}
				if success {
					if _, err = r.Vc.DeleteVolume(ctx, &DeleteVolumeRequest{
						VolumeId: volumeHandle,
						Secrets:  masterSecret,
					}); err != nil {
						return err
					}
				}
			}

			slaveRbd, err := rbd.GenVolFromVolID(ctx, v.SlaveVolumeHandle, slaveCr, slaveSecret)
			if err != nil {
				if errors.Is(err, librbd.ErrNotFound) {
					continue
				}
				r.Log.Error(err, "failed to get slave rbd")
				return err
			}
			defer slaveRbd.Destroy()

			if slaveRbd.CreatedAt != nil {
				state, err := slaveRbd.GetImageMirroringInfo()
				if err != nil {
					return err
				}
				if state.State != librbd.MirrorImageDisabled {
					if !state.Primary {
						success, err = r.Vs.Promote(ctx, slaveRbd, true)
						if err != nil {
							return err
						}
					} else {
						success = true
					}
					if state.State == librbd.MirrorImageEnabled && !state.Primary {
						if success {
							success, err = r.Vs.Disable(ctx, slaveRbd, true)
							if err != nil {
								return err
							}
						}
					} else {
						success = true
					}
				} else {
					success = true
				}
				if success {
					if _, err = r.Vc.DeleteVolume(ctx, &DeleteVolumeRequest{
						VolumeId: v.SlaveVolumeHandle,
						Secrets:  slaveSecret,
					}); err != nil {
						return err
					}
				}
			}

		default:
			return fmt.Errorf("unknown phase %v", v.Phase)
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

func (r *VirtualMachineSnapshotContentReconciler) updateVolumeStatus(ctx context.Context, content *hitoseacomv1.VirtualMachineSnapshotContent, newVolumeStatus hitoseacomv1.VolumeStatus) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var newContent hitoseacomv1.VirtualMachineSnapshotContent
		if err := r.Client.Get(ctx, types.NamespacedName{
			Namespace: content.Namespace,
			Name:      content.Name,
		}, &newContent); err != nil {
			return err
		}

		var targetIndex int
		found := false
		for i, v := range content.Status.VolumeStatus {
			if v.VolumeName == newVolumeStatus.VolumeName {
				targetIndex = i
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("volume %s not found", newVolumeStatus.VolumeName)
		}
		copyNonEmptyFields(&newContent.Status.VolumeStatus[targetIndex], &newVolumeStatus)
		if err := r.Client.Status().Update(ctx, &newContent); err != nil {
			return fmt.Errorf("failed to update resource Status: %w", err)
		}

		return nil
	})
}

func copyNonEmptyFields(dest, src *hitoseacomv1.VolumeStatus) {
	if src.CreationTime != nil {
		dest.CreationTime = src.CreationTime
	}
	if src.MasterVolumeHandle != "" {
		dest.MasterVolumeHandle = src.MasterVolumeHandle
	}
	if src.SlaveVolumeHandle != "" {
		dest.SlaveVolumeHandle = src.SlaveVolumeHandle
	}
	if src.SnapshotVolumeHandle != "" {
		dest.SnapshotVolumeHandle = src.SnapshotVolumeHandle
	}
	if src.VolumeName != "" {
		dest.VolumeName = src.VolumeName
	}
	if src.Phase != 0 {
		dest.Phase = src.Phase
	}
	if src.ReadyToUse != nil {
		dest.ReadyToUse = src.ReadyToUse
	}
	//destVal := reflect.ValueOf(dest).Elem()
	//srcVal := reflect.ValueOf(src).Elem()
	//for i := 0; i < destVal.NumField(); i++ {
	//	destField := destVal.Field(i)
	//	srcField := srcVal.Field(i)
	//	if reflect.DeepEqual(reflect.Zero(destField.Type()).Interface(), destField.Interface()) {
	//		if !reflect.DeepEqual(reflect.Zero(srcField.Type()).Interface(), srcField.Interface()) {
	//			destVal.Field(i).Set(srcField)
	//		}
	//	}
	//}
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

func (r *VirtualMachineSnapshotContentReconciler) createVolume(ctx context.Context, masterVolumeHandle string, content *hitoseacomv1.VirtualMachineSnapshotContent, volumeBackup *hitoseacomv1.VolumeBackup) (bool, error) {

	var phase hitoseacomv1.VirtualMachineSnapshotContentPhase
	var status hitoseacomv1.VolumeStatus

	index := 0
	is := false
	for k, v := range content.Status.VolumeStatus {
		if v.VolumeName == volumeBackup.VolumeName {
			is = true
			index = k
			break
		}
	}
	if !is {
		f := false
		status = hitoseacomv1.VolumeStatus{
			VolumeName:   volumeBackup.VolumeName,
			CreationTime: currentTime(),
			ReadyToUse:   &f,
			Error:        nil,
		}
		content.Status.VolumeStatus = append(content.Status.VolumeStatus, status)
	} else {
		status = content.Status.VolumeStatus[index]
	}

	msc, err := config.GetCephCsiConfigForSC(r.Client, config.DC.MasterStorageClass)
	if err != nil {
		r.Log.Error(err, "getCephCsiConfigForSC")
		return true, err
	}

	masterSecret, err := r.getSecret(msc.NodeStageSecretNamespace, msc.NodeStageSecretName)
	if err != nil {
		r.Log.Error(err, "getSecret")
		return true, err
	}

	masterCr, err := util.NewUserCredentials(masterSecret)
	if err != nil {
		r.Log.Error(err, "NewUserCredentials")
		return true, err
	}
	defer masterCr.DeleteCredentials()

	ssc, err := config.GetCephCsiConfigForSC(r.Client, config.DC.SlaveStorageClass)
	if err != nil {
		r.Log.Error(err, "getCephCsiConfigForSC")
		return true, err
	}

	slaveSecret, err := r.getSecret(ssc.NodeStageSecretNamespace, ssc.NodeStageSecretName)
	if err != nil {
		r.Log.Error(err, "getSecret")
		return true, err
	}

	slaveCr, err := util.NewUserCredentials(slaveSecret)
	if err != nil {
		r.Log.Error(err, "NewUserCredentials")
		return true, err
	}
	defer slaveCr.DeleteCredentials()

	var (
		cloneRbd          *rbd.RbdVolume
		slaveRbd          *rbd.RbdVolume
		slaveVolumeHandle string
		success           bool
	)

	phase = status.Phase
	slaveVolumeHandle = status.SlaveVolumeHandle

	if phase == hitoseacomv1.Create {
		//var vid string
		//var deleteClone bool
		//dump.P("Create phase status VolumeName => ", status.VolumeName, " masterVolumeHandle => ", masterVolumeHandle, " SlaveVolumeHandle", slaveVolumeHandle)
		success, err = r.Vs.Create(ctx, GetVolUUId(status.SlaveVolumeHandle), slaveVolumeHandle, masterVolumeHandle, map[string]string{"clusterID": msc.ClusterID, "pool": msc.Pool}, masterSecret, slaveSecret)
		if err != nil {
			return true, err
		}
		if success {
			status.CreationTime = currentTime()
			status.Phase = hitoseacomv1.Flatten
			if err = r.updateVolumeStatus(ctx, content, status); err != nil {
				//deleteClone = true
				r.Log.Error(err, "waitForCreation updateVolumeStatus")
				return true, err
			}
			//defer func() {
			//	if deleteClone {
			//		if _, err = r.Vc.DeleteVolume(ctx, &DeleteVolumeRequest{
			//			VolumeId: vid,
			//			Secrets:  masterSecret,
			//		}); err != nil {
			//			r.Log.Error(err, "delete volume failed", "volumeHandle", vid)
			//		}
			//	}
			//}()
		}
		return true, nil
	}

	time.Sleep(10 * time.Second)

	if phase != 0 && phase <= hitoseacomv1.Demote && cloneRbd == nil {
		cloneRbd, err = rbd.GenVolFromVolID(ctx, slaveVolumeHandle, masterCr, masterSecret)
		if err != nil {
			r.Log.Error(err, "GenVolFromVolID")
			return true, err
		}
		defer cloneRbd.Destroy()
	}

	if phase != hitoseacomv1.Complete && phase > hitoseacomv1.Demote && phase != hitoseacomv1.Snapshot {
		slaveVolumeHandle = getSlaveVolumeHandle(slaveVolumeHandle, ssc.ClusterID)
		if config.SlavePoolID != 0 {
			slaveVolumeHandle = replacePool(slaveVolumeHandle, config.SlavePoolID)
		}
		slaveRbd, err = rbd.GenVolFromVolID(ctx, slaveVolumeHandle, slaveCr, slaveSecret)
		if err != nil {
			if errors.Is(err, librbd.ErrNotFound) {
				return true, err
			}
			r.Log.Error(err, "failed to get slave rbd")
			return true, err
		}
		defer slaveRbd.Destroy()

	}

	switch phase {
	case hitoseacomv1.Flatten:
		success, err = r.Vs.Flatten(ctx, slaveVolumeHandle, masterSecret)
		if err != nil {
			return true, err
		}
		if success {
			status.Phase = hitoseacomv1.Enable
			if err = r.updateVolumeStatus(ctx, content, status); err != nil {
				r.Log.Error(err, "waitForCreation updateVolumeStatus")
				return true, err
			}
		}
	case hitoseacomv1.Enable:
		_, err = r.Vs.Enable(ctx, cloneRbd)
		if err != nil {
			return true, err
		}
		status.Phase = hitoseacomv1.Demote
		if err = r.updateVolumeStatus(ctx, content, status); err != nil {
			r.Log.Error(err, "waitForCreation updateVolumeStatus")
			return true, err
		}
	case hitoseacomv1.Demote:
		success, err = r.Vs.SyncSlaveImage(ctx, cloneRbd)
		if err != nil {
			return true, err
		}
		if success {
			success, err = r.Vs.Demote(ctx, cloneRbd)
			if err != nil {
				return true, err
			}
			if success {
				status.Phase = hitoseacomv1.Promote
				status.SlaveVolumeHandle = getSlaveVolumeHandle(slaveVolumeHandle, ssc.ClusterID)
				if err = r.updateVolumeStatus(ctx, content, status); err != nil {
					r.Log.Error(err, "SyncSlaveImage updateVolumeStatus")
					return true, err
				}
			}
		}
	case hitoseacomv1.Promote:
		success, err = r.Vs.Promote(ctx, slaveRbd, false)
		if err != nil {
			return true, err
		}
		if success {
			status.Phase = hitoseacomv1.Disable
			if err = r.updateVolumeStatus(ctx, content, status); err != nil {
				r.Log.Error(err, "waitForCreation updateVolumeStatus")
				return true, err
			}
		}
	case hitoseacomv1.Disable:
		success, err = r.Vs.SyncMasterImage(ctx, slaveRbd)
		if err != nil {
			return true, err
		}
		if success {
			success, err = r.Vs.Disable(ctx, slaveRbd, false)
			if err != nil {
				return true, err
			}
			if success {
				status.Phase = hitoseacomv1.Snapshot
				if err = r.updateVolumeStatus(ctx, content, status); err != nil {
					r.Log.Error(err, "waitForCreation updateVolumeStatus")
					return true, err
				}
			}
		}
	case hitoseacomv1.Snapshot:
		slaveVolumeHandle = getSlaveVolumeHandle(slaveVolumeHandle, ssc.ClusterID)
		if config.SlavePoolID != 0 {
			slaveVolumeHandle = replacePool(slaveVolumeHandle, config.SlavePoolID)
		}
		var snapshotId string
		snapshotId, err = r.Vs.Snapshot(ctx, slaveVolumeHandle, slaveSecret)
		if err != nil {
			return true, err
		}
		status.SnapshotVolumeHandle = snapshotId
		status.Phase = hitoseacomv1.Complete
		if err = r.updateVolumeStatus(ctx, content, status); err != nil {
			r.Log.Error(err, "updateVolumeStatus SnapshotCreation")
			return true, err
		}
	case hitoseacomv1.Complete:
		if slaveVolumeHandle == "" {
			slaveVolumeHandle = status.SlaveVolumeHandle
		}
		slaveVolumeHandle = getSlaveVolumeHandle(slaveVolumeHandle, ssc.ClusterID)
		if config.SlavePoolID != 0 {
			slaveVolumeHandle = replacePool(slaveVolumeHandle, config.SlavePoolID)
		}
		t := true
		status.Phase = hitoseacomv1.Complete
		status.ReadyToUse = &t
		status.SlaveVolumeHandle = slaveVolumeHandle
		if err = r.updateVolumeStatus(ctx, content, status); err != nil {
			r.Log.Error(err, "updateVolumeStatus Complete")
			return true, err
		}
		r.Log.Info(fmt.Sprintf("successfully created volumeHandle %s", slaveVolumeHandle))
		r.Recorder.Eventf(content, corev1.EventTypeNormal, volumeCloneCreateEvent, fmt.Sprintf("Successfully created VolumeHandle %s", content.Name))
		return false, nil
	default:
		return true, fmt.Errorf("unknown phase %v", phase)
	}

	return true, nil
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

func getSlaveVolumeHandle(volumeHandle, clusterID string) string {
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
