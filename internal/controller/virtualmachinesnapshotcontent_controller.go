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
	grpcClient "github.com/am6737/histore/pkg/client"
	librbd "github.com/ceph/go-ceph/rbd"
	"github.com/go-logr/logr"
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
	"regexp"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
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
)

var (
	volumePromotionKnownErrors    = []codes.Code{codes.FailedPrecondition}
	disableReplicationKnownErrors = []codes.Code{codes.NotFound}
)

// VirtualMachineSnapshotContentReconciler reconciles a VirtualMachineSnapshotContent object
type VirtualMachineSnapshotContentReconciler struct {
	client.Client
	Scheme            *runtime.Scheme
	Log               logr.Logger
	MasterReplication grpcClient.VolumeReplication
	SlaveReplication  grpcClient.VolumeReplication
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

	//var volumeSnapshotStatus []hitoseacomv1.VolumeSnapshotStatus

	vmSnapshot, err := r.getVMSnapshot(content)
	if err != nil {
		return ctrl.Result{}, err
	}

	// TODO 判断vmSnapshot是否为nil或者状态为停止
	//if vmSnapshot == nil || vmSnapshotTerminating(vmSnapshot) {
	//	// TODO 移除终结器
	//	return ctrl.Result{}, nil
	//}

	if content.Status == nil {
		content.Status = &hitoseacomv1.VirtualMachineSnapshotContentStatus{}
		content.Status.Phase = hitoseacomv1.EnableReplication
		if err = r.Client.Status().Update(context.Background(), content); err != nil {
			logger.Error(err, "无法更新 VirtualMachineSnapshot 对象的状态")
			return ctrl.Result{}, err
		}
	}

	var _, skippedSnapshots []string

	for _, volumeBackup := range content.Spec.VolumeBackups {
		if volumeBackup.VolumeSnapshotName == nil {
			continue
		}

		vsName := *volumeBackup.VolumeSnapshotName

		volumeSnapshot, err := r.GetVolumeSnapshot(content.Namespace, vsName)
		if err != nil {
			return ctrl.Result{}, err
		}

		if volumeSnapshot == nil && content.Status.Phase != hitoseacomv1.Complete {

			if vmSnapshot == nil || r.vmSnapshotDeleting(vmSnapshot) {
				log.Log.V(3).Info("Not creating snapshot %s because vm snapshot is deleted", vsName)
				skippedSnapshots = append(skippedSnapshots, vsName)
				continue
			}

			volumeSnapshot, err = r.CreateVolumeSnapshot(content, &volumeBackup)
			if err != nil {
				return ctrl.Result{}, err
			}

			if err = r.watchVolumeSnapshotContent(ctx, *volumeBackup.VolumeSnapshotName, content.Namespace, content); err != nil {
				r.Log.Error(err, "watchVolumeSnapshotContent err")
				continue
			}
		}

		newContent := &hitoseacomv1.VirtualMachineSnapshotContent{}
		if err = r.Client.Get(ctx, client.ObjectKey{Namespace: content.Namespace, Name: content.Name}, newContent); err != nil {
			r.Log.Error(err, "VirtualMachineSnapshotContent err")
			continue
		}

		if newContent.Status.Phase != hitoseacomv1.Complete {
			snapshotContent, err := r.getVolumeSnapshotContent(ctx, content)
			if err != nil {
				r.Log.Error(err, "getVolumeSnapshotContent err")
				continue
			}

			cp, err := r.getCommonParameters(content, *snapshotContent.Spec.VolumeSnapshotClassName, *snapshotContent.Status.SnapshotHandle)
			if err != nil {
				r.Log.Error(err, "getCommonParameters err")
				continue
			}

			//newContent := &hitoseacomv1.VirtualMachineSnapshotContent{}
			//if err := r.Client.Get(ctx, client.ObjectKey{Namespace: content.Namespace, Name: content.Name}, newContent); err != nil {
			//	return ctrl.Result{}, err
			//}
			cp.content = newContent.DeepCopy()
			if err = r.rbdHandler(context.Background(), cp); err != nil {
				r.Log.Error(err, "rbdHandler err")
				continue
			}

			//snapshotContent.Status.SnapshotHandle = &cp.slaveVolumeHandle
			//snapshotContent.Spec.Source.VolumeHandle = &cp.slaveVolumeHandle
			//snapshotContent.Spec.Driver = "slave.rbd.csi.ceph.com"
			//snapshotContent.Annotations["snapshot.storage.kubernetes.io/deletion-secret-name"] = "csi-ceph-secret-slave"
			//if err = r.Client.Status().Update(ctx, snapshotContent); err != nil {
			//	r.Log.Error(err, "Update snapshotContent err")
			//	continue
			//}
		}

		//vss := hitoseacomv1.VolumeSnapshotStatus{
		//	VolumeSnapshotName: volumeSnapshot.Name,
		//}
		//if volumeSnapshot.Status != nil {
		//	content.Status.ReadyToUse = volumeSnapshot.Status.ReadyToUse
		//	content.Status.CreationTime = volumeSnapshot.Status.CreationTime
		//	//vss.Error = translateError(volumeSnapshot.Status.Error)
		//}

		//volumeSnapshotStatus = append(volumeSnapshotStatus, vss)
	}

	//created, ready := true, true
	//
	//contentCpy := &hitoseacomv1.VirtualMachineSnapshotContent{}
	//if err = r.Get(ctx, req.NamespacedName, contentCpy); err != nil {
	//	if apierrors.IsNotFound(err) {
	//		return ctrl.Result{}, nil
	//	}
	//	logger.Error(err, "无法获取 VirtualMachineSnapshot 对象")
	//	return ctrl.Result{}, err
	//}
	//
	////contentCpy := newC.DeepCopy()
	//contentCpy.Status.Error = nil
	//
	//for _, vss := range volumeSnapshotStatus {
	//	if vss.CreationTime == nil {
	//		created = false
	//	}
	//
	//	if vss.ReadyToUse == nil || !*vss.ReadyToUse {
	//		ready = false
	//	}
	//}
	//
	//if created && contentCpy.Status.CreationTime == nil {
	//	contentCpy.Status.CreationTime = currentTime()
	//	//
	//	//err = ctrl.unfreezeSource(vmSnapshot)
	//	//if err != nil {
	//	//	return 0, err
	//	//}
	//}
	//
	////if errorMessage != "" {
	////	contentCpy.Status.Error = &snapshotv1.Error{
	////		Time:    currentTime(),
	////		Message: &errorMessage,
	////	}
	////}
	//
	//contentCpy.Status.ReadyToUse = &ready
	//contentCpy.Status.VolumeSnapshotStatus = volumeSnapshotStatus
	//
	////if !equality.Semantic.DeepEqual(newC, contentCpy) {
	//if err = r.Client.Status().Update(context.Background(), contentCpy); err != nil {
	//	logger.Error(err, "无法更新 VirtualMachineSnapshot 对象的状态")
	//	return ctrl.Result{}, err
	//}
	//}

	return ctrl.Result{}, nil
}

// variable so can be overridden in tests
var currentTime = func() *metav1.Time {
	t := metav1.Now()
	return &t
}

// SetupWithManager sets up the controller with the Manager.
func (r *VirtualMachineSnapshotContentReconciler) SetupWithManager(mgr ctrl.Manager) error {
	//c, err := grpcClient.New(config.DC.MasterDriverEndpoint, config.DC.RPCTimeout)
	//if err != nil {
	//	r.Log.Error(err, "failed to create GRPC Client", "Endpoint", config.DC.MasterDriverEndpoint, "GRPC Timeout", config.DC.RPCTimeout)
	//	return err
	//}
	//r.MasterReplication = grpcClient.NewReplicationClient(c.Client, config.DC.RPCTimeout)
	//
	//cc, err := grpcClient.New(config.DC.SlaveDriverEndpoint, config.DC.RPCTimeout)
	//if err != nil {
	//	r.Log.Error(err, "failed to create GRPC Client", "Endpoint", config.DC.SlaveDriverEndpoint, "GRPC Timeout", config.DC.RPCTimeout)
	//	return err
	//}
	//r.SlaveReplication = grpcClient.NewReplicationClient(cc.Client, config.DC.RPCTimeout)

	return ctrl.NewControllerManagedBy(mgr).
		WithEventFilter(predicate.Funcs{
			DeleteFunc: r.deleteVolumeHandler,
		}).
		For(&hitoseacomv1.VirtualMachineSnapshotContent{}).
		Owns(&hitoseacomv1.VirtualMachineSnapshot{}).
		Complete(r)
}

func (r *VirtualMachineSnapshotContentReconciler) deleteVolumeHandler(e event.DeleteEvent) bool {
	fmt.Println("deleteVolumeHandler ---> ", e.Object)
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
	fmt.Println("content => ", content)
	SecretName := content.Annotations[prefixedSnapshotDeleteSecretNameKey]
	SecretNamespace := content.Annotations[prefixedSnapshotDeleteSecretNamespaceKey]
	r.Log.Info("GetDeletionTimestamp => ", content.GetDeletionTimestamp().String())
	// check if the object is being deleted
	if !vmSnapshot.GetDeletionTimestamp().IsZero() && content.Status.VolumeHandle != nil {
		secret, err := r.getSecret(SecretNamespace, SecretName)
		if err != nil {
			r.Log.Error(err, "getSecret")
		}
		if err = r.DeleteVolumeSnapshot(context.Background(), *content.Status.VolumeHandle, secret, map[string]string{}); err != nil {
			r.Log.Error(err, "Failed to add PersistentVolumeClaim finalizer")
		}
		r.Log.Info(fmt.Sprintf("successfully delete volumeHandle %s", *content.Status.VolumeHandle))
		// 移除终结器
		RemoveFinalizer(vmSnapshot, vmSnapshotFinalizer)
	}
	return false
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

	if err := r.Client.Create(context.Background(), vs, &client.CreateOptions{}); err != nil {
		return nil, err
	}

	//ctrl.Recorder.Eventf(
	//	content,
	//	corev1.EventTypeNormal,
	//	volumeSnapshotCreateEvent,
	//	"Successfully created VolumeSnapshot %s",
	//	snapshot.Name,
	//)

	return vs, nil

	//return volumeSnapshot, nil
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

func (r *VirtualMachineSnapshotContentReconciler) watchVolumeSnapshotContent(ctx context.Context, name, namespace string, content *hitoseacomv1.VirtualMachineSnapshotContent) error {
	updatedSnapshot := &vsv1.VolumeSnapshot{}
	// 定义条件函数，检查对象状态是否已更新
	conditionFunc := func() (bool, error) {
		if err := r.Client.Get(ctx, client.ObjectKey{Name: name, Namespace: namespace}, updatedSnapshot); err != nil {
			if apierrors.IsNotFound(err) {
				// 对象不存在，返回 false 表示条件未满足，继续等待
				return false, nil
			}
			// 其他错误，返回错误
			return false, err
		}

		// 检查对象状态是否已更新
		return updatedSnapshot.Status != nil && *updatedSnapshot.Status.ReadyToUse, nil
	}

	// 使用 wait.PollImmediate 等待条件满足
	if err := wait.PollImmediate(time.Second, time.Minute*5, conditionFunc); err != nil {
		return err
	}

	// 获取更新后的对象
	updatedSnapshotContent := &vsv1.VolumeSnapshotContent{}
	if err := r.Client.Get(ctx, types.NamespacedName{Name: *updatedSnapshot.Status.BoundVolumeSnapshotContentName}, updatedSnapshotContent); err != nil {
		return err
	}

	if updatedSnapshotContent.Status != nil && *updatedSnapshotContent.Status.ReadyToUse {
		// 获取快照的 rbd 名称
		masterVolumeHandle := *updatedSnapshotContent.Status.SnapshotHandle

		// 获取快照类
		class, err := r.getSnapshotClassInfo(*updatedSnapshot.Spec.VolumeSnapshotClassName)
		if err != nil {
			return err
		}

		masterSecret, err := r.getSecret(class.Parameters[snapshotSecretNamespace], class.Parameters[snapshotSecretName])
		if err != nil {
			return err
		}

		// 获取 ceph 凭证信息
		cr, err := util.NewUserCredentials(masterSecret)
		if err != nil {
			fmt.Println("get cr ", err)
			return err
		}
		defer cr.DeleteCredentials()

		// Fetch source volume information
		rbdVol, err := rbd.GenVolFromVolSnapID(ctx, masterVolumeHandle, cr, masterSecret)
		defer rbdVol.Destroy()
		if err != nil {
			r.Log.Error(err, "get rbdVol")
			return err
		}

		parame := map[string]string{
			"clusterID": class.Parameters["clusterID"],
			//"csi.storage.k8s.io/volumesnapshot/name":        "snap-pvc-1440-1",
			//"csi.storage.k8s.io/volumesnapshot/namespace":   "default",
			//"csi.storage.k8s.io/volumesnapshotcontent/name": "snapcontent-060d2c31-4cd1-4e2f-b0cf-78749f3ef3fa",
			"pool": class.Parameters["pool"],
		}
		rbdSnap, err := rbd.GenSnapFromOptions(ctx, rbdVol, parame)
		if err != nil {
			fmt.Println("get rbdSnap ", err)
			return err
		}
		rbdSnap.RbdImageName = rbdVol.RbdImageName
		rbdSnap.VolSize = rbdVol.VolSize
		rbdSnap.SourceVolumeID = masterVolumeHandle
		rbdSnap.RbdSnapName = rbdVol.RbdImageName
		//rbdSnap.RequestName = "test-1"

		_, err = rbd.CreateRBDVolumeFromSnapshot(ctx, rbdVol, rbdSnap, cr)
		if err != nil {
			r.Log.Error(err, "get cloneRBD ")
			return err
		}

		// 获取源pvc的集群id替换为备份集群id
		//slaveVolumeHandle := r.getSlaveVolumeHandle(masterVolumeHandle, "5e709abc-419e-11ee-a132-af7f7bf3bfc0")

		cp, err := r.getCommonParameters(content, class.Name, masterVolumeHandle)
		if err != nil {
			return err
		}

		newContent := &hitoseacomv1.VirtualMachineSnapshotContent{}
		if err = r.Client.Get(ctx, client.ObjectKey{Namespace: content.Namespace, Name: content.Name}, newContent); err != nil {
			return err
		}
		cp.content = newContent.DeepCopy()

		if err = r.rbdHandler(context.Background(), cp); err != nil {
			return err
		}

		//newSC := &vsv1.VolumeSnapshotContent{}
		//if err = r.Client.Get(ctx, types.NamespacedName{Name: *updatedSnapshot.Status.BoundVolumeSnapshotContentName}, newSC); err != nil {
		//	return err
		//}
		//
		//newSC.Status.SnapshotHandle = &slaveVolumeHandle
		//if err = r.Client.Status().Update(ctx, newSC); err != nil {
		//	return err
		//}
		//fmt.Println("updatedSnapshotContent 1")
		//
		//newSC1 := &vsv1.VolumeSnapshotContent{}
		//if err = r.Client.Get(ctx, types.NamespacedName{Name: *updatedSnapshot.Status.BoundVolumeSnapshotContentName}, newSC1); err != nil {
		//	return err
		//}
		//
		//newSC1.Spec.Source.VolumeHandle = &slaveVolumeHandle
		//newSC1.Spec.Driver = "slave.rbd.csi.ceph.com"
		//newSC1.Annotations["snapshot.storage.kubernetes.io/deletion-secret-name"] = "csi-ceph-secret-slave"
		//if err = r.Client.Update(ctx, newSC1); err != nil {
		//	return err
		//}
		//fmt.Println("updatedSnapshotContent 2")
		//
		//content.Status.Phase = hitoseacomv1.Complete
		//if err = r.Client.Status().Update(ctx, content); err != nil {
		//	return err
		//}

	}
	return nil
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

func (r *VirtualMachineSnapshotContentReconciler) rbdHandler(ctx context.Context, cp *CommonParameters) error {
	var (
		err                 error
		masterRBD, slaveRBD *rbd.RbdVolume
		masterVolumeHandle  = cp.masterVolumeHandle
		slaveVolumeHandle   = cp.slaveVolumeHandle
		masterSecret        = cp.masterSecret
		slaveSecret         = cp.slaveSecret
		masterCr            *util.Credentials
		slaveCr             *util.Credentials
	)
	const (
		scheduleSyncPeriod = 5 * time.Second
		TTL                = 3 * time.Minute
	)

	DemoteImageHandler := func(rbdVol *rbd.RbdVolume) error {
		newContent := &hitoseacomv1.VirtualMachineSnapshotContent{}
		if err = r.Client.Get(ctx, client.ObjectKey{Namespace: cp.content.Namespace, Name: cp.content.Name}, newContent); err != nil {
			return err
		}
		newContent.Status.Phase = hitoseacomv1.VolumeDemote
		if err = r.Client.Status().Update(ctx, newContent); err != nil {
			r.Log.Error(err, "update status")
			return err
		}
		if err = wait.PollImmediate(scheduleSyncPeriod, TTL, func() (done bool, err error) {
			if err = rbdVol.DemoteImage(); err != nil {
				if strings.Contains(err.Error(), "Device or resource busy") {
					r.Log.Info(fmt.Sprintf("DemoteImageHandler 1 ---> %v", err.Error()))
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
		newContent := &hitoseacomv1.VirtualMachineSnapshotContent{}
		if err = r.Client.Get(ctx, client.ObjectKey{Namespace: cp.content.Namespace, Name: cp.content.Name}, newContent); err != nil {
			return err
		}
		newContent.Status.Phase = hitoseacomv1.VolumePromote
		if err = r.Client.Status().Update(ctx, newContent); err != nil {
			r.Log.Error(err, "update status")
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
		newContent := &hitoseacomv1.VirtualMachineSnapshotContent{}
		if err = r.Client.Get(ctx, client.ObjectKey{Namespace: cp.content.Namespace, Name: cp.content.Name}, newContent); err != nil {
			return err
		}
		newContent.Status.Phase = hitoseacomv1.DisableReplication
		if err = r.Client.Status().Update(ctx, newContent); err != nil {
			r.Log.Error(err, "update status")
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

	masterCr, err = util.NewUserCredentials(masterSecret)
	if err != nil {
		return err
	}
	defer masterCr.DeleteCredentials()

	masterRBD, err = rbd.GenVolFromVolID(ctx, masterVolumeHandle, masterCr, masterSecret)
	defer masterRBD.Destroy()
	if err != nil {
		if errors.Is(err, librbd.ErrNotFound) {
			//log.DebugLog(ctx, "image %s encrypted state not set", ri)
			r.Log.Info(fmt.Sprintf("source Volume ID %s not found", masterVolumeHandle))
			return err
		}
		return err
	}

	if cp.content.Status.Phase < hitoseacomv1.VolumePromote {
		if err = masterRBD.EnableImageMirroring(librbd.ImageMirrorModeSnapshot); err != nil {
			r.Log.Error(err, "master rbd enable image mirror failed")
			return err
		}
	}

	slaveCr, err = util.NewUserCredentials(slaveSecret)
	if err != nil {
		return err
	}
	defer slaveCr.DeleteCredentials()

	if err = wait.PollImmediate(scheduleSyncPeriod, TTL, func() (done bool, err error) {
		slaveRBD, err = rbd.GenVolFromVolID(ctx, slaveVolumeHandle, slaveCr, slaveSecret)
		if err != nil {
			if errors.Is(err, librbd.ErrNotFound) {
				//log.DebugLog(ctx, "image %s encrypted state not set", ri)
				r.Log.Info(fmt.Sprintf("slave source Volume ID %s not found", slaveVolumeHandle))
				return false, nil
			}
			r.Log.Error(err, "failed to get slave rbd")
			return false, nil
		}
		r.Log.Info(fmt.Sprintf("slave source Volume ID %s success", slaveVolumeHandle))
		return true, nil
	}); err != nil {
		return err
	}
	defer slaveRBD.Destroy()

	slaveRbdHandle := func() error {
		// 定义一个变量来跟踪当前步骤
		currentStep := cp.content.Status.Phase
		// 定义条件函数，检查对象状态是否已更新
		rbdConditionFunc := func() (bool, error) {
			// 根据当前步骤执行相应的操作
			switch currentStep {
			case 0:
				//fmt.Println("start enableReplicationPhase")
				//err = masterRBD.EnableImageMirroring(librbd.ImageMirrorModeSnapshot)
				//err = r.enableReplicationPhase(ctx, cp.content, cp.masterVolumeHandle, cp.parame, cp.masterSecret)
				//time.Sleep(5 * time.Second)
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
			// 如果发生错误，可以根据 currentStep 判断是哪一步出错了
			// 在这里可以根据需要处理错误
			return err
		}
		return nil
	}

	masterRbdImageStatus := &librbd.GlobalMirrorImageStatus{}

	if err = wait.PollImmediate(scheduleSyncPeriod, TTL, func() (done bool, err error) {
		sRbdStatus, err := slaveRBD.GetImageMirroringStatus()
		if err != nil {
			//fmt.Println("GetImageMirroringStatus err ", err)
			r.Log.Error(err, "GetImageMirroringStatus err")
			return false, nil
		}
		for _, srs := range sRbdStatus.SiteStatuses {
			if srs.MirrorUUID == "" {

				dump.P("slave image status", srs)
				if srs.State == librbd.MirrorImageStatusStateReplaying || (srs.State == librbd.MirrorImageStatusStateUnknown && strings.Contains(srs.Description, "remote image is not primary")) {
					// 要等待image同步完毕在下降提升
					if err = slaveRbdHandle(); err != nil {
						r.Log.Info(err.Error())
						return false, nil
					}
					return true, nil
				}
				//replayStatus, err := srs.DescriptionReplayStatus()
				//if err != nil {
				//	// 错误包含 "No such file or directory"，忽略此错误
				//	if strings.Contains(err.Error(), "No such file or directory") {
				//		return false, nil
				//	}
				//	r.Log.Error(err, "replayStatus err")
				//	return false, nil
				//}
				//if replayStatus.ReplayState == "idle" {
				//	// 要等待image同步完毕在下降提升
				//	if err = slaveRbdHandle(); err != nil {
				//		r.Log.Info(err.Error())
				//		return false, nil
				//	}
				//	return true, nil
				//}
			}
		}
		return false, nil
	}); err != nil {
		return err
	}

	if err = wait.PollImmediate(scheduleSyncPeriod, TTL, func() (done bool, err error) {
		masterRbdImageStatus, err = masterRBD.GetImageMirroringStatus()
		if err != nil {
			fmt.Println("masterRBD.GetImageMirroringStatus err ", err)
			return false, nil
		}
		for _, mrs := range masterRbdImageStatus.SiteStatuses {
			if mrs.MirrorUUID == "" {
				dump.P("master image status", mrs)
				if mrs.State == librbd.MirrorImageStatusStateReplaying {
					if err = DisableImageHandler(slaveRBD); err != nil {
						return false, err
					}
					return true, nil
				}
				//rs1, err := mrs.DescriptionReplayStatus()
				//if err != nil {
				//	// 错误包含 "No such file or directory"，忽略此错误
				//	if strings.Contains(err.Error(), "No such file or directory") {
				//		return false, nil
				//	}
				//	r.Log.Error(err, "replayStatus err")
				//	return false, nil
				//}
				//if rs1.ReplayState == "idle" {
				//	if err = DisableImageHandler(slaveRBD); err != nil {
				//		return false, err
				//	}
				//	return true, nil
				//}
				//return false, nil
			}
		}
		return false, nil
	}); err != nil {
		return err
	}

	newContent := &hitoseacomv1.VirtualMachineSnapshotContent{}
	if err = r.Client.Get(ctx, client.ObjectKey{Namespace: cp.content.Namespace, Name: cp.content.Name}, newContent); err != nil {
		return err
	}
	newContent.Status.Phase = hitoseacomv1.Complete
	newContent.Status.VolumeHandle = &slaveVolumeHandle
	if err = r.Client.Status().Update(ctx, newContent); err != nil {
		r.Log.Error(err, "update VirtualMachineSnapshotContent status")
		return err
	}

	r.Log.Info("sync rbd complete")

	volumeSnapshot, err := r.GetVolumeSnapshot(cp.content.Namespace, *cp.content.Spec.VolumeBackups[0].VolumeSnapshotName)
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
