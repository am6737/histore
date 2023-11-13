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
	vsv1 "github.com/kubernetes-csi/external-snapshotter/client/v6/apis/volumesnapshot/v1"
	"google.golang.org/grpc/codes"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/equality"
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
	volumeSnapshotCreateEvent  = "SuccessfulVolumeSnapshotCreate"
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

	if content.Status == nil {
		f := false
		content.Status = &hitoseacomv1.VirtualMachineSnapshotContentStatus{}
		content.Status.VolumeStatus = []hitoseacomv1.VolumeStatus{}
		content.Status.ReadyToUse = &f
	}

	log.Log.V(1).Info("测试日志----------------------------1")
	//var volumeSnapshotStatus []hitoseacomv1.VolumeSnapshotStatus

	vmSnapshot, err := r.getVMSnapshot(content)
	if err != nil {
		return ctrl.Result{}, err
	}

	if vmSnapshot == nil || vmSnapshotTerminating(vmSnapshot) {
		if err = r.removeFinalizerFromVmsc(content); err != nil {
			logger.Error(err, "Failed to remove VolumeReplication finalizer")
			return reconcile.Result{}, err
		}

		if vmSnapshot != nil && shouldDeleteContent(vmSnapshot, content) {
			return ctrl.Result{}, err
		}
	}

	//if !content.GetDeletionTimestamp().IsZero() {
	//	if err = r.removeFinalizerFromVmsc(content); err != nil {
	//		r.Log.Error(err, "Failed to remove VolumeReplication finalizer")
	//		return reconcile.Result{}, err
	//	}
	//	r.Log.Info(fmt.Sprintf("successfully delete volumeHandle %s", *content.Status.MasterVolumeHandle))
	//	return reconcile.Result{}, nil
	//}

	if vmSnapshotContentDeleting(content) {
		logger.Info("Content deleting %s/%s", "namespace", content.Namespace, "name", content.Name)
		return reconcile.Result{Requeue: true, RequeueAfter: 5 * time.Second}, nil
	}

	currentlyCreated := vmSnapshotContentCreated(content)
	currentlyError := (content.Status != nil && content.Status.Error != nil) || vmSnapshotError(vmSnapshot) != nil

	//if content.Status == nil {
	//	content.Status = &hitoseacomv1.VirtualMachineSnapshotContentStatus{}
	//	content.Status.Phase = hitoseacomv1.EnableReplication
	//	if err = r.Client.Status().Update(context.TODO(), content); err != nil {
	//		return reconcile.Result{}, err
	//	}
	//}

	var deletedSnapshots, skippedSnapshots []string
	var volumeSnapshotStatus []hitoseacomv1.VolumeStatus

	for _, volumeBackup := range content.Spec.VolumeBackups {
		if volumeBackup.VolumeSnapshotName == nil {
			continue
		}

		vsName := *volumeBackup.VolumeSnapshotName

		volumeSnapshot, err := r.GetVolumeSnapshot(content.Namespace, vsName)
		if err != nil {
			return ctrl.Result{}, err
		}

		if volumeSnapshot == nil {
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
				continue
			}

			if vmSnapshot == nil || r.vmSnapshotDeleting(vmSnapshot) {
				log.Log.V(3).Info("Not creating snapshot %s because vm snapshot is deleted", vsName)
				skippedSnapshots = append(skippedSnapshots, vsName)
				continue
			}

			if currentlyError {
				log.Log.V(3).Info("Not creating snapshot %s because in error state", vsName)
				skippedSnapshots = append(skippedSnapshots, vsName)
				continue
			}

			volumeSnapshot, err = r.CreateVolumeSnapshot(content, &volumeBackup)
			if err != nil {
				return ctrl.Result{}, err
			}

			updatedSnapshot, err := r.watchVolumeSnapshot(ctx, *volumeBackup.VolumeSnapshotName, content.Namespace)
			if err != nil {
				r.Log.Error(err, "watchVolumeSnapshot err")
				continue
			}

			if err = r.watchVolumeSnapshotContent(ctx, *updatedSnapshot.Status.BoundVolumeSnapshotContentName, &volumeBackup, content); err != nil {
				r.Log.Error(err, "watchVolumeSnapshot err")
				continue
			}
		}

		vss := hitoseacomv1.VolumeStatus{
			//VolumeSnapshotName: volumeSnapshot.Name,
		}

		if volumeSnapshot.Status != nil {
			vss.ReadyToUse = volumeSnapshot.Status.ReadyToUse
			vss.CreationTime = volumeSnapshot.Status.CreationTime
			vss.Error = translateError(volumeSnapshot.Status.Error)
		}

		volumeSnapshotStatus = append(volumeSnapshotStatus, vss)
	}

	created, ready := true, true
	errorMessage := ""
	contentCpy := content.DeepCopy()

	if len(deletedSnapshots) > 0 {
		created, ready = false, false
		errorMessage = fmt.Sprintf("VolumeSnapshots (%s) missing", strings.Join(deletedSnapshots, ","))
	} else if len(skippedSnapshots) > 0 {
		created, ready = false, false
		if vmSnapshot == nil || vmSnapshotDeleting(vmSnapshot) {
			errorMessage = fmt.Sprintf("VolumeSnapshots (%s) skipped because vm snapshot is deleted", strings.Join(skippedSnapshots, ","))
		} else {
			errorMessage = fmt.Sprintf("VolumeSnapshots (%s) skipped because in error state", strings.Join(skippedSnapshots, ","))
		}
	} else {
		for _, vss := range volumeSnapshotStatus {
			if vss.CreationTime == nil {
				created = false
			}

			if vss.ReadyToUse == nil || !*vss.ReadyToUse {
				ready = false
			}
		}
	}

	if created && contentCpy.Status.CreationTime == nil {
		contentCpy.Status.CreationTime = currentTime()
	}

	if errorMessage != "" {
		contentCpy.Status.Error = &hitoseacomv1.Error{
			Time:    currentTime(),
			Message: &errorMessage,
		}
	}

	contentCpy.Status.ReadyToUse = &ready
	contentCpy.Status.VolumeStatus = volumeSnapshotStatus

	if !equality.Semantic.DeepEqual(content, contentCpy) {
		//if _, err := ctrl.Client.VirtualMachineSnapshotContent(contentCpy.Namespace).Update(context.Background(), contentCpy, metav1.UpdateOptions{}); err != nil {
		//	return 0, err
		//}
		if err = r.Update(ctx, contentCpy); err != nil {
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}

func translateError(e *vsv1.VolumeSnapshotError) *hitoseacomv1.Error {
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

//func (r *VirtualMachineSnapshotContentReconciler) updateStatusWithRetry(ctx context.Context, instance *hitoseacomv1.VirtualMachineSnapshotContent) error {
//	const maxRetries = 5
//	retryCount := 0
//	for {
//		// 尝试更新对象状态
//		err := r.Status().Update(ctx, instance)
//		if err == nil {
//			break
//		}
//		fmt.Println("status update 1", err)
//		// 处理更新错误
//		if apierrors.IsConflict(err) && retryCount < maxRetries {
//			fmt.Println("status update 2", err)
//			// 如果是资源版本过时错误，并且尝试次数未达到上限，重新获取对象
//			retryCount++
//			existingVMSC := &hitoseacomv1.VirtualMachineSnapshotContent{}
//			if err = r.Get(ctx, client.ObjectKey{
//				Namespace: instance.Namespace,
//				Name:      instance.Name,
//			}, existingVMSC); err != nil {
//				r.Log.Error(err, "无法重新获取 VirtualMachineSnapshotContent 对象")
//				return err
//			}
//			// 更新 instance 对象
//			instance = existingVMSC
//			continue
//		} else {
//			fmt.Println("status update 3", err)
//			// 如果不是资源版本过时错误，或者尝试次数达到上限，处理其他错误
//			r.Log.Error(err, "无法更新 VirtualMachineSnapshotContent 对象的状态")
//			return err
//		}
//	}
//	return nil
//}

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

func (r *VirtualMachineSnapshotContentReconciler) CreateVolumeSnapshot(
	content *hitoseacomv1.VirtualMachineSnapshotContent,
	volumeBackup *hitoseacomv1.VolumeBackup,
) (*vsv1.VolumeSnapshot, error) {
	log.Log.Info(fmt.Sprintf("Attempting to create VolumeSnapshot %s", *volumeBackup.VolumeSnapshotName))

	sc := volumeBackup.PersistentVolumeClaim.Spec.StorageClassName
	if sc == nil {
		return nil, fmt.Errorf("%s/%s VolumeSnapshot requested but no storage class",
			content.Namespace, volumeBackup.PersistentVolumeClaim.Name)
	}

	volumeSnapshotClass, err := r.getVolumeSnapshotClass(*sc)
	if err != nil {
		r.Log.Info(fmt.Sprintf("Couldn't find VolumeSnapshotClass for %s", *sc))
		return nil, err
	}

	t := true
	vs := &vsv1.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:      *volumeBackup.VolumeSnapshotName,
			Namespace: content.Namespace,
			Labels: map[string]string{
				snapshotSourceNameLabel:      content.Spec.Source.VirtualMachine.Name,
				snapshotSourceNamespaceLabel: content.Spec.Source.VirtualMachine.Namespace,
			},

			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion:         hitoseacomv1.GroupVersion.String(),
					Kind:               "VirtualMachineSnapshotContent",
					Name:               content.Name,
					UID:                content.UID,
					Controller:         &t,
					BlockOwnerDeletion: &t,
				},
			},
		},
		Spec: vsv1.VolumeSnapshotSpec{
			Source: vsv1.VolumeSnapshotSource{
				PersistentVolumeClaimName: &volumeBackup.PersistentVolumeClaim.Name,
			},
			VolumeSnapshotClassName: &volumeSnapshotClass,
		},
	}

	if err = r.Client.Create(context.Background(), vs, &client.CreateOptions{}); err != nil {
		return nil, err
	}

	r.Recorder.Eventf(
		content,
		corev1.EventTypeNormal,
		volumeSnapshotCreateEvent,
		"Successfully created VolumeSnapshot %s",
		vs.Name,
	)

	return vs, nil

	//return volumeSnapshot, nil
}

func (r *VirtualMachineSnapshotContentReconciler) watchVolumeSnapshotContent(ctx context.Context, name string, volumeBackup *hitoseacomv1.VolumeBackup, content *hitoseacomv1.VirtualMachineSnapshotContent) error {
	updatedSnapshotContent := &vsv1.VolumeSnapshotContent{}
	if err := r.Client.Get(ctx, types.NamespacedName{Name: name}, updatedSnapshotContent); err != nil {
		return err
	}
	// 定义条件函数，检查对象状态是否已更新
	conditionFunc := func() (bool, error) {
		if err := r.Client.Get(ctx, client.ObjectKey{Name: name}, updatedSnapshotContent); err != nil {
			if apierrors.IsNotFound(err) {
				// 对象不存在，返回 false 表示条件未满足，继续等待
				return false, nil
			}
			return false, err
		}
		// 检查对象状态是否已更新
		return updatedSnapshotContent.Status != nil && *updatedSnapshotContent.Status.ReadyToUse, nil
	}
	if err := wait.PollImmediate(time.Second, time.Minute*5, conditionFunc); err != nil {
		return err
	}

	masterVolumeHandle := *updatedSnapshotContent.Status.SnapshotHandle

	msc, err := getCephCsiConfigForSC(r.Client, r.MasterScName)
	if err != nil {
		return err
	}

	masterSecret, err := r.getSecret(msc.NodeStageSecretNamespace, msc.NodeStageSecretName)
	if err != nil {
		return err
	}

	//// 获取 ceph 凭证信息
	cr, err := util.NewUserCredentials(masterSecret)
	if err != nil {
		r.Log.Error(err, "get cr ")
		return err
	}
	defer cr.DeleteCredentials()
	//
	//// Fetch source volume information
	//rbdVol, err := rbd.GenVolFromVolSnapID(ctx, masterVolumeHandle, cr, masterSecret)
	//defer rbdVol.Destroy()
	//if err != nil {
	//	r.Log.Error(err, "get rbdVol")
	//	return err
	//}
	//
	//parame := map[string]string{
	//	"clusterID": msc.ClusterID,
	//	//"csi.storage.k8s.io/volumesnapshot/name":        "snap-pvc-1440-1",
	//	//"csi.storage.k8s.io/volumesnapshot/namespace":   "default",
	//	//"csi.storage.k8s.io/volumesnapshotcontent/name": "snapcontent-060d2c31-4cd1-4e2f-b0cf-78749f3ef3fa",
	//	"pool": msc.Parameters["pool"],
	//}
	//rbdSnap, err := rbd.GenSnapFromOptions(ctx, rbdVol, parame)
	//if err != nil {
	//	fmt.Println("get rbdSnap ", err)
	//	return err
	//}
	//rbdSnap.RbdImageName = rbdVol.RbdImageName
	//rbdSnap.VolSize = rbdVol.VolSize
	//rbdSnap.SourceVolumeID = masterVolumeHandle
	//rbdSnap.RbdSnapName = rbdVol.RbdImageName
	////rbdSnap.RequestName = "test-1"
	//
	//_, err = rbd.CreateRBDVolumeFromSnapshot(ctx, rbdVol, rbdSnap, cr)
	//if err != nil {
	//	r.Log.Error(err, "get cloneRBD ")
	//	return err
	//}

	// 获取源pvc的集群id替换为备份集群id
	//slaveVolumeHandle := r.getSlaveVolumeHandle(masterVolumeHandle, "5e709abc-419e-11ee-a132-af7f7bf3bfc0")

	if err = r.rbdHandler(ctx, masterVolumeHandle, volumeBackup, content); err != nil {
		return err
	}

	return nil
}

func (r *VirtualMachineSnapshotContentReconciler) watchVolumeSnapshot(ctx context.Context, name, namespace string) (*vsv1.VolumeSnapshot, error) {
	updatedSnapshot := &vsv1.VolumeSnapshot{}
	// 定义条件函数，检查对象状态是否已更新
	conditionFunc := func() (bool, error) {
		if err := r.Client.Get(ctx, client.ObjectKey{Name: name, Namespace: namespace}, updatedSnapshot); err != nil {
			if apierrors.IsNotFound(err) {
				// 对象不存在，返回 false 表示条件未满足，继续等待
				return false, nil
			}
			return false, err
		}
		// 检查对象状态是否已更新
		return updatedSnapshot.Status != nil && *updatedSnapshot.Status.ReadyToUse, nil
	}
	if err := wait.PollImmediate(time.Second, time.Minute*5, conditionFunc); err != nil {
		return nil, err
	}
	return updatedSnapshot, nil
}

func (r *VirtualMachineSnapshotContentReconciler) rbdHandler(ctx context.Context, masterVolumeHandle string, volumeBackup *hitoseacomv1.VolumeBackup, content *hitoseacomv1.VirtualMachineSnapshotContent) error {
	var (
		err                 error
		masterRBD, slaveRBD *rbd.RbdVolume
		slaveVolumeHandle   string
		masterSecret        map[string]string
		slaveSecret         map[string]string
		masterCr            *util.Credentials
		slaveCr             *util.Credentials
	)
	const (
		scheduleSyncPeriod = 5 * time.Second
		TTL                = 3 * time.Minute
	)

	ssc, err := getCephCsiConfigForSC(r.Client, r.SlaveScName)
	if err != nil {
		return err
	}
	slaveVolumeHandle = r.getSlaveVolumeHandle(masterVolumeHandle, ssc.ClusterID)

	msc, err := getCephCsiConfigForSC(r.Client, r.MasterScName)
	if err != nil {
		return err
	}

	masterSecret, err = r.getSecret(msc.NodeStageSecretNamespace, msc.NodeStageSecretName)
	if err != nil {
		return err
	}

	slaveSecret, err = r.getSecret(ssc.NodeStageSecretNamespace, ssc.NodeStageSecretName)
	if err != nil {
		return err
	}

	vindex := 0
	is := false
	for k, v := range content.Status.VolumeStatus {
		if v.VolumeName == volumeBackup.VolumeName {
			is = true
			vindex = k
			break
		}
	}

	if !is {
		f := false
		content.Status.VolumeStatus = append(content.Status.VolumeStatus, hitoseacomv1.VolumeStatus{
			MasterVolumeHandle: masterVolumeHandle,
			SlaveVolumeHandle:  slaveVolumeHandle,
			VolumeName:         volumeBackup.VolumeName,
			Phase:              hitoseacomv1.EnableReplication,
			CreationTime:       currentTime(),
			ReadyToUse:         &f,
		})
		vindex = len(content.Status.VolumeStatus) - 1
	}

	if *content.Status.VolumeStatus[vindex].ReadyToUse == true {
		return nil
	}

	DemoteImageHandler := func(rbdVol *rbd.RbdVolume) error {
		//patch := client.MergeFrom(content.DeepCopy())
		//if err = r.Client.Status().Patch(context.Background(), content, patch); err != nil {
		//	r.Log.Error(err, "Failed to update VirtualMachineSnapshotContent status")
		//	return err
		//}
		content.Status.VolumeStatus[vindex].Phase = hitoseacomv1.VolumeDemote
		if err = r.Status().Update(ctx, content); err != nil {
			log.Log.V(0).Error(err, "s 1")
			return err
		}
		if err = wait.PollImmediate(scheduleSyncPeriod, TTL, func() (done bool, err error) {
			//if err = rbdVol.DemoteImage(); err != nil {
			//	if strings.Contains(err.Error(), "Device or resource busy") {
			//		return false, nil
			//	}
			//	r.Log.Error(err, "demote master rbd failed")
			//	return false, err
			//}
			r.Log.Info("demote master rbd success")
			return true, nil
		}); err != nil {
			return err
		}
		return nil
	}

	PromoteImageHandler := func(rbdVol *rbd.RbdVolume) error {
		//patch := client.MergeFrom(content.DeepCopy())
		//if err = r.Client.Status().Patch(context.Background(), content, patch); err != nil {
		//	r.Log.Error(err, "Failed to update VirtualMachineSnapshotContent status")
		//	return err
		//}
		if err = wait.PollImmediate(scheduleSyncPeriod, TTL, func() (done bool, err error) {
			//if err = rbdVol.PromoteImage(false); err != nil {
			//	if strings.Contains(err.Error(), "Device or resource busy") {
			//		r.Log.Info(err.Error())
			//		return false, nil
			//	}
			//	r.Log.Error(err, "promote slave rbd failed")
			//	return false, err
			//}
			r.Log.Info("promote slave rbd success")
			return true, nil
		}); err != nil {
			return err
		}
		content.Status.VolumeStatus[vindex].Phase = hitoseacomv1.VolumePromote
		if err = r.Status().Update(ctx, content); err != nil {
			log.Log.V(1).Error(err, "s 2")
			return err
		}
		return nil
	}

	DisableImageHandler := func(rbdVol *rbd.RbdVolume) error {
		//patch := client.MergeFrom(content.DeepCopy())
		//if err = r.Client.Status().Patch(context.Background(), content, patch); err != nil {
		//	r.Log.Error(err, "Failed to update VirtualMachineSnapshotContent status")
		//	return err
		//}
		content.Status.VolumeStatus[vindex].Phase = hitoseacomv1.DisableReplication
		if err = r.Status().Update(ctx, content); err != nil {
			log.Log.V(0).Error(err, "s 3")
			return err
		}
		if err = wait.PollImmediate(scheduleSyncPeriod, TTL, func() (done bool, err error) {
			//if err = rbdVol.DisableImageMirroring(false); err != nil {
			//	if strings.Contains(err.Error(), "Device or resource busy") {
			//		r.Log.Info(err.Error())
			//		return false, nil
			//	}
			//	r.Log.Error(err, "disable slave rbd image failed")
			//}
			r.Log.Info("disable slave rbd image success")
			return true, nil
		}); err != nil {
			return err
		}
		return err
	}

	masterCr, err = util.NewUserCredentials(masterSecret)
	if err != nil {
		return err
	}
	defer masterCr.DeleteCredentials()

	//masterRBD, err = rbd.GenVolFromVolID(ctx, masterVolumeHandle, masterCr, masterSecret)
	//defer masterRBD.Destroy()
	//if err != nil {
	//	if errors.Is(err, librbd.ErrNotFound) {
	//		//log.DebugLog(ctx, "image %s encrypted state not set", ri)
	//		r.Log.Info(fmt.Sprintf("source Volume ID %s not found", masterVolumeHandle))
	//		return err
	//	}
	//	return err
	//}

	if content.Status.VolumeStatus[vindex].Phase < hitoseacomv1.VolumePromote {
		if err = wait.PollImmediate(scheduleSyncPeriod, TTL, func() (done bool, err error) {
			//if err = masterRBD.EnableImageMirroring(librbd.ImageMirrorModeSnapshot); err != nil {
			//	r.Log.Error(err, "master rbd enable image mirror failed")
			//	log.Log.V(0).Error(err, "master rbd enable image mirror failed")
			//	return false, nil
			//}
			return true, nil
		},
		); err != nil {
			return err
		}
	}

	slaveCr, err = util.NewUserCredentials(slaveSecret)
	if err != nil {
		return err
	}
	defer slaveCr.DeleteCredentials()

	if err = wait.PollImmediate(scheduleSyncPeriod, TTL, func() (done bool, err error) {
		//slaveRBD, err = rbd.GenVolFromVolID(ctx, slaveVolumeHandle, slaveCr, slaveSecret)
		//if err != nil {
		//	if errors.Is(err, librbd.ErrNotFound) {
		//		//log.DebugLog(ctx, "image %s encrypted state not set", ri)
		//		r.Log.Info(fmt.Sprintf("slave source Volume ID %s not found", slaveVolumeHandle))
		//		return false, nil
		//	}
		//	r.Log.Error(err, "failed to get slave rbd")
		//	return false, nil
		//}
		r.Log.Info(fmt.Sprintf("slave source Volume ID %s success", slaveVolumeHandle))
		return true, nil
	}); err != nil {
		return err
	}
	//defer slaveRBD.Destroy()

	slaveRbdHandle := func() error {
		currentStep := hitoseacomv1.EnableReplication
		// 定义一个变量来跟踪当前步骤
		if len(content.Status.VolumeStatus) != 0 {
			currentStep = content.Status.VolumeStatus[vindex].Phase
		}
		// 定义条件函数，检查对象状态是否已更新
		rbdConditionFunc := func() (bool, error) {
			// 根据当前步骤执行相应的操作
			switch currentStep {
			case 0:
				currentStep++
				fallthrough
			case 1:
				err = DemoteImageHandler(masterRBD)
			case 2:
				err = PromoteImageHandler(slaveRBD)
			case 3:
				//err = DisableImageHandler(slaveRBD)
			}
			if err != nil {
				// 如果发生错误，记录当前步骤，并返回错误
				r.Log.Error(err, fmt.Sprintf("rbdConditionFunc index %v", currentStep))
				return false, nil
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
		// 要等待image同步完毕在下降提升
		if err = slaveRbdHandle(); err != nil {
			r.Log.Info(err.Error())
			return false, nil
		}
		return true, nil

		//lcoalSt, err := slaveRBD.GetLocalState()
		//if err != nil {
		//	return false, nil
		//}
		//if !lcoalSt.Up {
		//	r.Log.Info(fmt.Sprintf("image已禁用%v", lcoalSt))
		//	Disable = true
		//	return true, nil
		//}
		//
		//sRbdStatus, err := slaveRBD.GetImageMirroringStatus()
		//if err != nil {
		//	//fmt.Println("GetImageMirroringStatus err ", err)
		//	r.Log.Error(err, "GetImageMirroringStatus err")
		//	return false, nil
		//}
		//for _, srs := range sRbdStatus.SiteStatuses {
		//	if srs.MirrorUUID == "" {
		//
		//		dump.P("slave image status", srs)
		//		//if srs.State == librbd.MirrorImageStatusStateReplaying || (srs.State == librbd.MirrorImageStatusStateUnknown && strings.Contains(srs.Description, "remote image is not primary")) {
		//		//	// 要等待image同步完毕在下降提升
		//		//	if err = slaveRbdHandle(); err != nil {
		//		//		r.Log.Info(err.Error())
		//		//		return false, nil
		//		//	}
		//		//	return true, nil
		//		//}
		//		replayStatus, err := srs.DescriptionReplayStatus()
		//		if err != nil {
		//			// 错误包含 "No such file or directory"，忽略此错误
		//			if strings.Contains(err.Error(), "No such file or directory") {
		//				return false, nil
		//			}
		//			r.Log.Error(err, "replayStatus err")
		//			return false, nil
		//		}
		//		if replayStatus.ReplayState == "idle" {
		//			// 要等待image同步完毕在下降提升
		//			if err = slaveRbdHandle(); err != nil {
		//				r.Log.Info(err.Error())
		//				return false, nil
		//			}
		//			return true, nil
		//		}
		//	}
		//}
		//return false, nil
	}); err != nil {
		return err
	}

	if err = wait.PollImmediate(scheduleSyncPeriod, TTL, func() (done bool, err error) {
		if Disable {
			return true, nil
		}
		//masterRbdImageStatus, err = masterRBD.GetImageMirroringStatus()
		//if err != nil {
		//	fmt.Println("masterRBD.GetImageMirroringStatus err ", err)
		//	return false, nil
		//}
		//for _, mrs := range masterRbdImageStatus.SiteStatuses {
		//	if mrs.MirrorUUID == "" {
		//		dump.P("master image status", mrs)
		//		//if mrs.State == librbd.MirrorImageStatusStateReplaying {
		//		//	if err = DisableImageHandler(slaveRBD); err != nil {
		//		//		return false, err
		//		//	}
		//		//	return true, nil
		//		//}
		//		rs1, err := mrs.DescriptionReplayStatus()
		//		if err != nil {
		//			// 错误包含 "No such file or directory"，忽略此错误
		//			if strings.Contains(err.Error(), "No such file or directory") {
		//				return false, nil
		//			}
		//			r.Log.Error(err, "replayStatus err")
		//			return false, nil
		//		}
		//		if rs1.ReplayState == "idle" {
		//			if err = DisableImageHandler(slaveRBD); err != nil {
		//				return false, err
		//			}
		//			return true, nil
		//		}
		//		//return false, nil
		//	}
		//}
		//return false, nil
		if err = DisableImageHandler(slaveRBD); err != nil {
			return false, err
		}
		return true, nil
	}); err != nil {
		return err
	}

	//content.Status.VolumeStatus[vindex].Phase = hitoseacomv1.Complete
	//patch := client.MergeFrom(content.DeepCopy())
	//if err = r.Client.Status().Patch(context.Background(), content, patch); err != nil {
	//	r.Log.Error(err, "Failed to update VirtualMachineSnapshotContent status")
	//	return err
	//}

	t := true
	content.Status.VolumeStatus[vindex].Phase = hitoseacomv1.Complete
	content.Status.VolumeStatus[vindex].ReadyToUse = &t
	if err = r.Status().Update(ctx, content); err != nil {
		log.Log.V(0).Error(err, "s 4")
		return err
	}

	//newContent := &hitoseacomv1.VirtualMachineSnapshotContent{}
	//if err = r.Client.Get(ctx, client.ObjectKey{Namespace: content.Namespace, Name: content.Name}, newContent); err != nil {
	//	return err
	//}
	//newContent.Status.Phase = hitoseacomv1.Complete
	//newContent.Status.MasterVolumeHandle = &slaveVolumeHandle
	//if err = r.Client.Status().Update(ctx, newContent); err != nil {
	//	r.Log.Error(err, "update VirtualMachineSnapshotContent status")
	//	return err
	//}

	r.Log.Info("sync rbd complete")

	volumeSnapshot, err := r.GetVolumeSnapshot(content.Namespace, *content.Spec.VolumeBackups[0].VolumeSnapshotName)
	if err != nil {
		return err
	}
	if volumeSnapshot == nil {
		r.Log.Info("volumeSnapshot is Empty")
		return nil
	}
	if err = r.Client.Delete(ctx, volumeSnapshot); err != nil {
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

func (r *VirtualMachineSnapshotContentReconciler) getSnapshotClassInfo(name string) (*vsv1.VolumeSnapshotClass, error) {
	snapshotClass := &vsv1.VolumeSnapshotClass{}
	if err := r.Client.Get(context.Background(), types.NamespacedName{Name: name}, snapshotClass); err != nil {
		fmt.Println("get snapshotClass ", err)
		return nil, err
	}
	return snapshotClass.DeepCopy(), nil
}
