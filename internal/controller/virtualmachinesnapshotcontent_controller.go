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
	librbd "github.com/ceph/go-ceph/rbd"
	"github.com/go-logr/logr"
	"github.com/google/uuid"
	"github.com/gookit/goutil/dump"
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
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
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
	Scheme       *runtime.Scheme
	Recorder     record.EventRecorder
	Log          logr.Logger
	MasterScName string
	SlaveScName  string
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
		logger.Error(err, "无法获取 VirtualMachineSnapshot 对象")
		return ctrl.Result{}, err
	}

	log.Log.V(1).Info("测试日志----------------------------1")

	if vmSnapshotContentDeleting(content) {
		logger.Info("Content deleting %s/%s", "namespace", content.Namespace, "name", content.Name)
		return reconcile.Result{Requeue: true, RequeueAfter: 5 * time.Second}, nil
	}

	currentlyCreated := vmSnapshotContentCreated(content)
	currentlyError := content.Status.Error != nil

	if len(content.Status.VolumeStatus) == 0 {
		fmt.Println("------------------- create  content.Status.VolumeStatus -----------")
		content.Status.ReadyToUse = false
		content.Status.CreationTime = currentTime()
		for _, v := range content.Spec.VolumeBackups {
			content.Status.VolumeStatus = append(content.Status.VolumeStatus, hitoseacomv1.VolumeStatus{
				VolumeName: v.VolumeName,
				Phase:      0,
				ReadyToUse: false,
			})
		}
		if err := r.Status().Update(ctx, content); err != nil {
			return reconcile.Result{
				Requeue:      true,
				RequeueAfter: 5 * time.Second,
			}, err
		}
	}

	//if content.Status.ReadyToUse {
	//	r.Log.Info("VirtualMachineSnapshotContent已完成", "namespace", content.Namespace, "name", content.Name)
	//	return ctrl.Result{}, nil
	//}

	var deletedSnapshots, skippedSnapshots []string
	var completionList []string
	for _, volumeBackup := range content.Spec.VolumeBackups {
		if volumeBackup.VolumeSnapshotName == nil {
			continue
		}
		for _, v := range content.Status.VolumeStatus {
			if v.VolumeName == volumeBackup.VolumeName && v.ReadyToUse {
				continue
			}
		}

		vsName := *volumeBackup.VolumeSnapshotName

		pv, err := r.getPVFromPVCName(ctx, volumeBackup.PersistentVolumeClaim.Namespace, volumeBackup.PersistentVolumeClaim.Name)
		if err != nil {
			r.Log.Error(err, "getPVFromPVCName")
			return ctrl.Result{}, err
		}

		// check if snapshot was deleted
		if currentlyCreated {
			logger.Info(fmt.Sprintf("VolumeSnapshot %s no longer exists", vsName))
			r.Recorder.Eventf(
				content,
				corev1.EventTypeWarning,
				volumeSnapshotMissingEvent,
				"VolumeSnapshot %s no longer exists",
				vsName,
			)
			deletedSnapshots = append(deletedSnapshots, vsName)
		}

		if currentlyError {
			log.Log.V(3).Info("Not creating snapshot %s because in error state", vsName)
			skippedSnapshots = append(skippedSnapshots, vsName)
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
		fmt.Println(" newContent.Status.VolumeStatus => ", newContent.Status.VolumeStatus)
		//f := false
		complete := 0
		for _, vStatus := range newContent.Status.VolumeStatus {
			if vStatus.ReadyToUse {
				complete++
			}
		}
		if complete == len(newContent.Status.VolumeStatus) {
			// 如果所有卷都匹配，设置 ReadyToUse 为 true
			newContent.Status.ReadyToUse = true
			if err := r.Status().Update(ctx, newContent); err != nil {
				return err
			}
			// 3. 找到关联的 VirtualMachineSnapshot 对象
			for _, ownerRef := range content.OwnerReferences {
				if ownerRef.Kind == "VirtualMachineSnapshot" {
					vmSnapshot := &hitoseacomv1.VirtualMachineSnapshot{}
					if err := r.Get(ctx, types.NamespacedName{Name: ownerRef.Name, Namespace: content.Namespace}, vmSnapshot); err != nil {
						return err
					}
					if !*vmSnapshot.Status.ReadyToUse {
						t := true
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

	// 执行 ReadyToUse 更新
	if err := updateReadyToUseStatus(); err != nil {
		return reconcile.Result{
			Requeue:      true,
			RequeueAfter: 5 * time.Second,
		}, err
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
		WithEventFilter(predicate.Funcs{
			DeleteFunc: r.deleteVolumeHandler,
		}).
		For(&hitoseacomv1.VirtualMachineSnapshotContent{}).
		Owns(&hitoseacomv1.VirtualMachineSnapshot{}).
		Complete(r)
}

func (r *VirtualMachineSnapshotContentReconciler) deleteVolumeHandler(e event.DeleteEvent) bool {
	vmSnapshot, ok := e.Object.(*hitoseacomv1.VirtualMachineSnapshot)
	if !ok {
		return false
	}
	content := &hitoseacomv1.VirtualMachineSnapshotContent{}
	err := r.Client.Get(context.TODO(), client.ObjectKey{Namespace: vmSnapshot.Namespace, Name: *vmSnapshot.Status.VirtualMachineSnapshotContentName}, content)
	if err != nil {
		r.Log.Error(err, "get VirtualMachineSnapshotContent")
		return false
	}
	fmt.Println("deleteVolumeHandler ----------------------------- 1")
	SecretName := content.Annotations[prefixedSnapshotDeleteSecretNameKey]
	SecretNamespace := content.Annotations[prefixedSnapshotDeleteSecretNamespaceKey]
	// check if the object is being deleted
	if !vmSnapshot.GetDeletionTimestamp().IsZero() {
		secret, err := r.getSecret(SecretNamespace, SecretName)
		if err != nil {
			r.Log.Error(err, "getSecret")
		}
		for _, v := range content.Status.VolumeStatus {
			if err = r.DeleteVolumeSnapshot(context.Background(), v.SlaveVolumeHandle, secret, map[string]string{}); err != nil {
				r.Log.Error(err, "Failed to add PersistentVolumeClaim finalizer")
			}
		}
	}
	return true
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

func (r *VirtualMachineSnapshotContentReconciler) DeleteVolumeSnapshot(ctx context.Context, volumeHandle string, secrets, parameters map[string]string) error {
	cr, err := util.NewUserCredentials(secrets)
	if err != nil {
		return err
	}
	defer cr.DeleteCredentials()

	vol, err := rbd.GenVolFromVolID(ctx, volumeHandle, cr, secrets)
	defer vol.Destroy()
	if err != nil {
		if errors.Is(err, librbd.ErrNotFound) {
			//log.DebugLog(ctx, "image %s encrypted state not set", ri)
			r.Log.Info(fmt.Sprintf("source Volume ID %s not found", volumeHandle))
			return err
		}
		return err
	}

	return vol.DeleteImage(ctx)
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

	// Compare the old and new VolumeStatus
	//oldVolumeStatus := content.Status.VolumeStatus[targetIndex]
	//if !reflect.DeepEqual(oldVolumeStatus, newVolumeStatus) {
	fmt.Println("updateVolumeStatus => ", newVolumeStatus)
	content.Status.VolumeStatus[targetIndex] = newVolumeStatus
	// Update the VirtualMachineSnapshotContent status using patch
	if err := r.Status().Update(context.Background(), content); err != nil {
		log.Log.V(0).Error(err, "Failed to update VirtualMachineSnapshotContent status")
		return err
	}
	//}
	return nil
}

func (r *VirtualMachineSnapshotContentReconciler) CreateVolume(
	ctx context.Context,
	masterVolumeHandle string,
	content *hitoseacomv1.VirtualMachineSnapshotContent,
	volumeBackup *hitoseacomv1.VolumeBackup,
) (bool, error) {

	contentCpy := &hitoseacomv1.VirtualMachineSnapshotContent{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: content.Namespace, Name: content.Name}, contentCpy); err != nil {
		return false, err
	}
	vindex := 0
	is := false
	for k, v := range contentCpy.Status.VolumeStatus {
		if v.VolumeName == volumeBackup.VolumeName {
			is = true
			vindex = k
			break
		}
	}
	if !is {
		r.Log.Info("status异常")
		return false, nil
	}
	if contentCpy.Status.VolumeStatus[vindex].ReadyToUse == true {
		return false, nil
	}

	msc, err := getCephCsiConfigForSC(r.Client, r.MasterScName)
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

	ssc, err := getCephCsiConfigForSC(r.Client, r.SlaveScName)
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

	masterRbd, err := rbd.GenVolFromVolID(ctx, masterVolumeHandle, masterCr, masterSecret)
	defer masterRbd.Destroy()
	if err != nil {
		if errors.Is(err, librbd.ErrNotFound) {
			//log.DebugLog(ctx, "image %s encrypted state not set", ri)
			r.Log.Info(fmt.Sprintf("master source Volume ID %s not found", masterVolumeHandle))
			return false, err
		}
	}

	clusterID, err := GetClusterIDFromVolumeHandle(masterVolumeHandle)
	if err != nil {
		r.Log.Error(err, "GetClusterIDFromVolumeHandle")
		return false, err
	}

	rbdSnap, err := rbd.GenSnapFromOptions(ctx, masterRbd, map[string]string{"clusterID": clusterID})
	if err != nil {
		r.Log.Error(err, "GenSnapFromOptions")
		return false, err
	}
	sRbdName := uuid.New().String()
	rbdSnap.RbdImageName = masterRbd.RbdImageName
	rbdSnap.VolSize = masterRbd.VolSize
	rbdSnap.SourceVolumeID = masterVolumeHandle
	rbdSnap.RbdSnapName = "csi-vol-" + sRbdName

	log.Log.Info(fmt.Sprintf("Attempting to create VolumeSnapshot %s", *volumeBackup.VolumeSnapshotName))

	_, err = rbd.CreateRBDVolumeFromSnapshot(ctx, masterRbd, rbdSnap, masterCr)
	if err != nil {
		r.Log.Error(err, "CreateRBDVolumeFromSnapshot")
		return false, err
	}

	cloneRbd, err := rbd.GenVolFromVolID(ctx, GetCloneVolumeHandleFromVolumeHandle(masterVolumeHandle, sRbdName), masterCr, masterSecret)
	defer cloneRbd.Destroy()
	if err != nil {
		r.Log.Error(err, "GenVolFromVolID")
		return false, err
	}

	slaveVolumeHandle := GenerateSlaveVolumeHandle(masterVolumeHandle, ssc.ClusterID, sRbdName)

	const (
		scheduleSyncPeriod = 5 * time.Second
		TTL                = 3 * time.Minute
	)

	dump.P("contentCpy.Status.VolumeStatus => ", contentCpy.Status.VolumeStatus)

	DemoteImageHandler := func(rbdVol *rbd.RbdVolume) error {
		if err = r.updateVolumeStatus(contentCpy, hitoseacomv1.VolumeStatus{
			VolumeName: volumeBackup.VolumeName,
			Phase:      hitoseacomv1.VolumeDemote,
		}); err != nil {
			return err
		}
		if err = wait.PollImmediate(scheduleSyncPeriod, TTL, func() (done bool, err error) {
			if err = rbdVol.DemoteImage(); err != nil {
				if strings.Contains(err.Error(), "Device or resource busy") {
					return false, nil
				}
				r.Log.Error(err, "demote master rbd failed")
				return false, err
			}
			r.Log.Info("demote master rbd success")
			return true, nil
		}); err != nil {
			return err
		}
		return nil
	}

	PromoteImageHandler := func(rbdVol *rbd.RbdVolume) error {
		if err = r.updateVolumeStatus(contentCpy, hitoseacomv1.VolumeStatus{
			VolumeName: volumeBackup.VolumeName,
			Phase:      hitoseacomv1.VolumePromote,
		}); err != nil {
			return err
		}
		if err = wait.PollImmediate(scheduleSyncPeriod, TTL, func() (done bool, err error) {
			if err = rbdVol.PromoteImage(false); err != nil {
				if strings.Contains(err.Error(), "Device or resource busy") {
					r.Log.Info(err.Error())
					return false, nil
				}
				r.Log.Error(err, "promote slave rbd failed")
				return false, err
			}
			r.Log.Info("promote slave rbd success")
			return true, nil
		}); err != nil {
			return err
		}
		return nil
	}

	DisableImageHandler := func(rbdVol *rbd.RbdVolume) error {
		if err = r.updateVolumeStatus(contentCpy, hitoseacomv1.VolumeStatus{
			VolumeName: volumeBackup.VolumeName,
			Phase:      hitoseacomv1.DisableReplication,
		}); err != nil {
			return err
		}
		if err = wait.PollImmediate(scheduleSyncPeriod, TTL, func() (done bool, err error) {
			if err = rbdVol.DisableImageMirroring(false); err != nil {
				if strings.Contains(err.Error(), "Device or resource busy") {
					r.Log.Info(err.Error())
					return false, nil
				}
				r.Log.Error(err, "disable slave rbd image failed")
			}
			r.Log.Info("disable slave rbd image success")
			return true, nil
		}); err != nil {
			return err
		}

		return err
	}

	fmt.Println("contentCpy.Status.VolumeStatus[vindex].Phase => ", contentCpy.Status.VolumeStatus[vindex].Phase)

	if contentCpy.Status.VolumeStatus[vindex].Phase < hitoseacomv1.VolumePromote {
		fmt.Println("start masterRbd EnableImageMirroring")
		if err = wait.PollImmediate(scheduleSyncPeriod, TTL, func() (done bool, err error) {
			if err = cloneRbd.EnableImageMirroring(librbd.ImageMirrorModeSnapshot); err != nil {
				r.Log.Error(err, "master rbd enable image mirror failed")
				log.Log.V(0).Error(err, "master rbd enable image mirror failed")
				return false, nil
			}
			return true, nil
		},
		); err != nil {
			return false, err
		}
	}

	slaveRbd := &rbd.RbdVolume{}
	if err = wait.PollImmediate(scheduleSyncPeriod, TTL, func() (done bool, err error) {
		slaveRbd, err = rbd.GenVolFromVolID(ctx, slaveVolumeHandle, slaveCr, slaveSecret)
		if err != nil {
			if errors.Is(err, librbd.ErrNotFound) {
				r.Log.Info(fmt.Sprintf("slave source Volume ID %s not found", slaveVolumeHandle))
				return false, nil
			}
			r.Log.Error(err, "failed to get slave rbd")
			return false, nil
		}
		r.Log.Info(fmt.Sprintf("slave source Volume ID %s success", slaveVolumeHandle))
		return true, nil
	}); err != nil {
		return false, err
	}

	slaveRbdHandle := func() error {
		currentStep := hitoseacomv1.EnableReplication
		// 定义一个变量来跟踪当前步骤
		if len(contentCpy.Status.VolumeStatus) != 0 {
			currentStep = contentCpy.Status.VolumeStatus[vindex].Phase
		}
		// 定义条件函数，检查对象状态是否已更新
		rbdConditionFunc := func() (bool, error) {
			time.Sleep(3 * time.Second)
			// 根据当前步骤执行相应的操作
			switch currentStep {
			case 0:
				//err = EnableImageHandler(masterRbd)
			case 1:
				err = DemoteImageHandler(masterRbd)
			case 2:
				err = PromoteImageHandler(nil)
			case 3:
				//err = DisableImageHandler(nil)
			}
			if err != nil {
				// 如果发生错误，记录当前步骤，并返回错误
				r.Log.Error(err, fmt.Sprintf("rbdConditionFunc index %v", currentStep))
				return false, err
			}
			currentStep++
			// 如果成功执行当前步骤，将 currentStep 增加 1，准备执行下一步骤
			// 如果所有步骤都执行完毕，返回 true，等待循环将结束
			return currentStep == 4, nil
		}
		// 使用 wait.PollImmediate 等待条件满足
		if err = wait.PollImmediate(scheduleSyncPeriod, TTL, rbdConditionFunc); err != nil {
			return err
		}
		return nil
	}

	//masterRbdImageStatus := &librbd.GlobalMirrorImageStatus{}
	Disable := false
	if err = wait.PollImmediate(scheduleSyncPeriod, TTL, func() (done bool, err error) {
		////// 要等待image同步完毕在下降提升
		//if err = slaveRbdHandle(); err != nil {
		//	r.Log.Info(err.Error())
		//	return false, nil
		//}
		//return true, nil
		//lcoalSt, err := slaveRbd.GetLocalState()
		//if err != nil {
		//	return false, nil
		//}
		//if !lcoalSt.Up {
		//	r.Log.Info(fmt.Sprintf("image已禁用%v", lcoalSt))
		//	Disable = true
		//	return true, nil
		//}

		sRbdStatus, err := slaveRbd.GetImageMirroringStatus()
		if err != nil {
			r.Log.Error(err, "GetImageMirroringStatus err")
			return false, nil
		}
		for _, srs := range sRbdStatus.SiteStatuses {
			if srs.MirrorUUID == "" {
				dump.P("slave image status", srs)
				replayStatus, err := srs.DescriptionReplayStatus()
				if err != nil {
					// 错误包含 "No such file or directory"，忽略此错误
					if strings.Contains(err.Error(), "No such file or directory") {
						return false, nil
					}
					r.Log.Error(err, "replayStatus err")
					return false, nil
				}
				if replayStatus.ReplayState == "idle" {
					// 要等待image同步完毕在下降提升
					if err = slaveRbdHandle(); err != nil {
						r.Log.Info(err.Error())
						return false, nil
					}
					return true, nil
				}
			}
		}
		return false, nil
	}); err != nil {
		return false, err
	}

	if err = wait.PollImmediate(scheduleSyncPeriod, TTL, func() (done bool, err error) {
		if Disable {
			return true, nil
		}
		masterRbdImageStatus, err := cloneRbd.GetImageMirroringStatus()
		if err != nil {
			fmt.Println("masterRBD.GetImageMirroringStatus err ", err)
			return false, nil
		}
		for _, mrs := range masterRbdImageStatus.SiteStatuses {
			if mrs.MirrorUUID == "" {
				dump.P("master image status", mrs)
				rs1, err := mrs.DescriptionReplayStatus()
				if err != nil {
					// 错误包含 "No such file or directory"，忽略此错误
					if strings.Contains(err.Error(), "No such file or directory") {
						return false, nil
					}
					r.Log.Error(err, "replayStatus err")
					return false, nil
				}
				if rs1.ReplayState == "idle" {
					if err = DisableImageHandler(slaveRbd); err != nil {
						return false, err
					}
					return true, nil
				}
				//return false, nil
			}
		}
		return false, nil
		//if err = DisableImageHandler(slaveRbd); err != nil {
		//	return false, err
		//}
		//return true, nil
	}); err != nil {
		return false, err
	}

	if err = r.updateVolumeStatus(contentCpy, hitoseacomv1.VolumeStatus{
		VolumeName:         volumeBackup.VolumeName,
		Phase:              hitoseacomv1.Complete,
		MasterVolumeHandle: masterVolumeHandle,
		SlaveVolumeHandle:  slaveVolumeHandle,
		ReadyToUse:         true,
	}); err != nil {
		if apierrors.IsConflict(err) {
			log.Log.V(0).Info("Retrying with patch due to conflict error")
			patch := client.MergeFrom(contentCpy.DeepCopy())
			if err = r.Client.Status().Patch(context.Background(), content, patch); err != nil {
				r.Log.Error(err, "Failed to update VirtualMachineSnapshotContent status")
				return false, err
			}
		}
		return false, err
	}

	r.Recorder.Eventf(contentCpy, corev1.EventTypeNormal, volumeCloneCreateEvent, "Successfully created VolumeHandle %s", slaveVolumeHandle)

	r.Log.Info("sync rbd complete")

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

type CommonParameters struct {
	content            *hitoseacomv1.VirtualMachineSnapshotContent
	masterVolumeHandle string
	slaveVolumeHandle  string
	parame             map[string]string
	masterSecret       map[string]string
	slaveSecret        map[string]string
}

func (r *VirtualMachineSnapshotContentReconciler) getCommonParameters(content *hitoseacomv1.VirtualMachineSnapshotContent, volumeSnapshotClassName, masterVolumeHandle string) (*CommonParameters, error) {
	// 获取快照类
	class, err := r.getSnapshotClassInfo(volumeSnapshotClassName)
	if err != nil {
		return nil, err
	}

	masterSecret, err := r.getSecret(class.Parameters[snapshotSecretNamespace], class.Parameters[snapshotSecretName])
	if err != nil {
		return nil, err
	}

	parame := map[string]string{
		"clusterID": class.Parameters["clusterID"],
		//"csi.storage.k8s.io/volumesnapshot/name":        "snap-pvc-1440-1",
		//"csi.storage.k8s.io/volumesnapshot/namespace":   "default",
		//"csi.storage.k8s.io/volumesnapshotcontent/name": "snapcontent-060d2c31-4cd1-4e2f-b0cf-78749f3ef3fa",
		"pool": class.Parameters["pool"],
	}

	slaveSecret := map[string]string{
		"adminID":  "admin",
		"adminKey": "AQAK3eVkP8wGLRAAf4/QRlKajw+r/Fb5TkSY8w==",
		"userID":   "admin",
		"userKey":  "AQAK3eVkP8wGLRAAf4/QRlKajw+r/Fb5TkSY8w==",
	}

	slaveVolumeHandle := r.getSlaveVolumeHandle(masterVolumeHandle, "5e709abc-419e-11ee-a132-af7f7bf3bfc0")

	return &CommonParameters{
		content:            content,
		masterVolumeHandle: masterVolumeHandle,
		slaveVolumeHandle:  slaveVolumeHandle,
		parame:             parame,
		masterSecret:       masterSecret,
		slaveSecret:        slaveSecret,
	}, nil
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

func (r *VirtualMachineSnapshotContentReconciler) getSlaveVolumeHandle(volumeHandle, clusterID string) string {
	replaceString := func(input, oldSubstring, newSubstring string) string {
		return strings.Replace(input, oldSubstring, newSubstring, -1)
	}
	re := regexp.MustCompile(`\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b`)
	//slaveVolumeHandle := replaceString(volumeHandle, re.FindString(volumeHandle), "5e709abc-419e-11ee-a132-af7f7bf3bfc0")
	return replaceString(volumeHandle, re.FindString(volumeHandle), clusterID)
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
