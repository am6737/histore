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
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	hitoseacomv1 "github.com/am6737/histore/api/v1"
	snapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v6/apis/volumesnapshot/v1"
	kubevirtv1 "kubevirt.io/api/core/v1"
)

const (
	defaultVolumeSnapshotClassAnnotation = "snapshot.storage.kubernetes.io/is-default-class"
	vmSnapshotContentFinalizer           = "snapshot.hitosea.com/vmsnapshotcontent-protection"
)

// VirtualMachineSnapshotReconciler reconciles a VirtualMachineSnapshot object
type VirtualMachineSnapshotReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

//+kubebuilder:rbac:groups=hitosea.com,resources=virtualmachinesnapshots,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=hitosea.com,resources=virtualmachinesnapshots/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=hitosea.com,resources=virtualmachinesnapshots/finalizers,verbs=update

//+kubebuilder:rbac:groups=kubevirt.io,resources=virtualmachines,verbs=get;list;watch
//+kubebuilder:rbac:groups=kubevirt.io,resources=virtualmachines/status,verbs=get

//+kubebuilder:rbac:groups=snapshot.storage.k8s.io,resources=volumesnapshots,verbs=get;list;watch;create
//+kubebuilder:rbac:groups=snapshot.storage.k8s.io,resources=volumesnapshots/status,verbs=get
//+kubebuilder:rbac:groups=snapshot.storage.k8s.io,resources=volumesnapshotclass,verbs=get;list;watch

//+kubebuilder:rbac:groups=snapshot.storage.k8s.io,resources=volumesnapshotclass,verbs=get;list;watch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the VirtualMachineSnapshot object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.14.4/pkg/reconcile
func (r *VirtualMachineSnapshotReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	// 1. 根据请求获取到 VirtualMachineSnapshot 对象
	var vmSnapshot hitoseacomv1.VirtualMachineSnapshot
	if err := r.Get(ctx, req.NamespacedName, &vmSnapshot); err != nil {
		if apierrors.IsNotFound(err) {
			// 对象不存在，可能已被删除，可以返回一个无需处理的结果
			return ctrl.Result{}, nil
		}
		log.Error(err, "无法获取 VirtualMachineSnapshot 对象")
		return ctrl.Result{}, err
	}

	content, err := r.getContent(&vmSnapshot)
	if err != nil {
		return ctrl.Result{}, err
	}

	// Make sure status is initialized before doing anything
	if vmSnapshot.Status != nil {
		//if source != nil {
		if vmSnapshotProgressing(&vmSnapshot) && !vmSnapshotTerminating(&vmSnapshot) {
			// create content if does not exist
			if content == nil {
				if err := r.createContent(&vmSnapshot); err != nil {
					return ctrl.Result{}, err
				}
			}
		}
		//}
	}

	// Make sure status is initialized before doing anything
	if vmSnapshot.Status == nil {
		vmSnapshot.Status = &hitoseacomv1.VirtualMachineSnapshotStatus{}
	}

	vm, err := r.getVM(&vmSnapshot)
	if err != nil {
		return ctrl.Result{}, err
	}

	// TODO: 在这里根据情况更新状态字段
	vmSnapshot.Status.SourceUID = &vm.UID
	// vmSnapshot.Status.Phase = ...
	// vmSnapshot.Status.ReadyToUse = ...
	// vmSnapshot.Status.Error = ...
	// vmSnapshot.Status.CreationTime = ...

	// 更新 VirtualMachineSnapshot 对象的状态
	if err := r.Status().Update(ctx, &vmSnapshot); err != nil {
		log.Error(err, "无法更新 VirtualMachineSnapshot 对象的状态")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func vmSnapshotTerminating(vmSnapshot *hitoseacomv1.VirtualMachineSnapshot) bool {
	return false
}

func (r *VirtualMachineSnapshotReconciler) createContent(vmSnapshot *hitoseacomv1.VirtualMachineSnapshot) error {
	//source, err := ctrl.getSnapshotSource(vmSnapshot)
	//if err != nil {
	//	return err
	//}
	vm, err := r.getVM(vmSnapshot)
	if err != nil {
		return err
	}

	var volumeBackups []hitoseacomv1.VolumeBackup
	pvcs := vm.Spec.Template.Spec.Volumes
	for _, v := range pvcs {
		pvc, err := r.getSnapshotPVC(vmSnapshot.Namespace, v.Name)
		if err != nil {
			return err
		}

		if pvc == nil {
			//log.Log.Warningf("No snapshot PVC for %s/%s", vmSnapshot.Namespace, pvcName)
			continue
		}

		volumeSnapshotName := fmt.Sprintf("vmsnapshot-%s-volume-%s", vmSnapshot.UID, v.Name)
		vb := hitoseacomv1.VolumeBackup{
			VolumeName: v.Name,
			PersistentVolumeClaim: hitoseacomv1.PersistentVolumeClaim{
				ObjectMeta: *getSimplifiedMetaObject(pvc.ObjectMeta),
				Spec:       *pvc.Spec.DeepCopy(),
			},
			VolumeSnapshotName: &volumeSnapshotName,
		}

		volumeBackups = append(volumeBackups, vb)
	}

	//sourceSpec, err := source.Spec()
	//if err != nil {
	//	return err
	//}
	content := &hitoseacomv1.VirtualMachineSnapshotContent{
		ObjectMeta: metav1.ObjectMeta{
			Name:       GetVMSnapshotContentName(vmSnapshot),
			Namespace:  vmSnapshot.Namespace,
			Finalizers: []string{vmSnapshotContentFinalizer},
		},
		Spec: hitoseacomv1.VirtualMachineSnapshotContentSpec{
			VirtualMachineSnapshotName: &vmSnapshot.Name,
			//Source:                     sourceSpec,
			VolumeBackups: volumeBackups,
		},
	}

	if err := r.Client.Create(context.Background(), content, &client.CreateOptions{}); err != nil {
		return err
	}

	//
	//ctrl.Recorder.Eventf(
	//	vmSnapshot,
	//	corev1.EventTypeNormal,
	//	vmSnapshotContentCreateEvent,
	//	"Successfully created VirtualMachineSnapshotContent %s",
	//	content.Name,
	//)

	return nil
}

func getSimplifiedMetaObject(meta metav1.ObjectMeta) *metav1.ObjectMeta {
	result := meta.DeepCopy()
	result.ManagedFields = nil

	return result
}

func (r *VirtualMachineSnapshotReconciler) getSnapshotPVC(namespace, volumeName string) (*corev1.PersistentVolumeClaim, error) {

	var obj corev1.PersistentVolumeClaim
	if err := r.Client.Get(context.TODO(), client.ObjectKey{Namespace: namespace, Name: volumeName}, &obj); err != nil {
		return nil, err
	}

	pvc := obj.DeepCopy()

	if pvc.Spec.VolumeName == "" {
		//log.Log.Warningf("Unbound PVC %s/%s", pvc.Namespace, pvc.Name)
		return nil, nil
	}

	if pvc.Spec.StorageClassName == nil {
		//log.Log.Warningf("No storage class for PVC %s/%s", pvc.Namespace, pvc.Name)
		return nil, nil
	}

	volumeSnapshotClass, err := r.getVolumeSnapshotClass(*pvc.Spec.StorageClassName)
	if err != nil {
		return nil, err
	}

	if volumeSnapshotClass != "" {
		return pvc, nil
	}

	return nil, nil
}

func (r *VirtualMachineSnapshotReconciler) getVolumeSnapshotClass(storageClassName string) (string, error) {

	var obj storagev1.StorageClass
	if err := r.Client.Get(context.TODO(), client.ObjectKey{Name: storageClassName}, &obj); err != nil {
		return "", err
	}

	storageClass := obj.DeepCopy()

	var matches []snapshotv1.VolumeSnapshotClass
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

func (r *VirtualMachineSnapshotReconciler) getVolumeSnapshotClasses() []snapshotv1.VolumeSnapshotClass {

	var objs snapshotv1.VolumeSnapshotClassList
	var vscs []snapshotv1.VolumeSnapshotClass

	if err := r.Client.List(context.TODO(), &objs); err != nil {
		return nil
	}

	for _, obj := range objs.Items {
		vsc := obj.DeepCopy()
		vscs = append(vscs, *vsc)
	}

	return vscs
}

func vmSnapshotProgressing(vmSnapshot *hitoseacomv1.VirtualMachineSnapshot) bool {
	return vmSnapshotError(vmSnapshot) == nil && !VmSnapshotReady(vmSnapshot) &&
		!vmSnapshotFailed(vmSnapshot) && !vmSnapshotSucceeded(vmSnapshot)
}

func vmSnapshotSucceeded(vmSnapshot *hitoseacomv1.VirtualMachineSnapshot) bool {
	return vmSnapshot.Status != nil && vmSnapshot.Status.Phase == hitoseacomv1.Succeeded
}

func vmSnapshotFailed(vmSnapshot *hitoseacomv1.VirtualMachineSnapshot) bool {
	return vmSnapshot.Status != nil && vmSnapshot.Status.Phase == hitoseacomv1.Failed
}

func VmSnapshotReady(vmSnapshot *hitoseacomv1.VirtualMachineSnapshot) bool {
	return vmSnapshot.Status != nil && vmSnapshot.Status.ReadyToUse != nil && *vmSnapshot.Status.ReadyToUse
}

func vmSnapshotError(vmSnapshot *hitoseacomv1.VirtualMachineSnapshot) *hitoseacomv1.Error {
	if vmSnapshot != nil && vmSnapshot.Status != nil && vmSnapshot.Status.Error != nil {
		return vmSnapshot.Status.Error
	}
	return nil
}

func (r *VirtualMachineSnapshotReconciler) getContent(vmSnapshot *hitoseacomv1.VirtualMachineSnapshot) (*hitoseacomv1.VirtualMachineSnapshotContent, error) {
	contentName := GetVMSnapshotContentName(vmSnapshot)
	var vmsc *hitoseacomv1.VirtualMachineSnapshotContent

	err := r.Client.Get(context.TODO(), client.ObjectKey{
		Namespace: vmSnapshot.Namespace,
		Name:      contentName,
	}, vmsc)
	if err != nil && !apierrors.IsAlreadyExists(err) {
		// 如果不是已存在的错误，则返回错误
		return nil, err
	}

	return vmsc.DeepCopy(), nil
}

func GetVMSnapshotContentName(vmSnapshot *hitoseacomv1.VirtualMachineSnapshot) string {
	if vmSnapshot.Status != nil && vmSnapshot.Status.VirtualMachineSnapshotContentName != nil {
		return *vmSnapshot.Status.VirtualMachineSnapshotContentName
	}

	return fmt.Sprintf("%s-%s", "vmsnapshot-content", vmSnapshot.UID)
}

func (r *VirtualMachineSnapshotReconciler) getVM(vmSnapshot *hitoseacomv1.VirtualMachineSnapshot) (*kubevirtv1.VirtualMachine, error) {
	vmName := vmSnapshot.Spec.Source.Name

	// 创建一个虚拟机对象
	vm := &kubevirtv1.VirtualMachine{}

	// 尝试从 API 服务器获取虚拟机对象
	if err := r.Client.Get(context.TODO(), types.NamespacedName{
		Namespace: vmSnapshot.Namespace,
		Name:      vmName,
	}, vm); err != nil {
		// 如果对象不存在，返回 nil
		if apierrors.IsNotFound(err) {
			return vm, nil
		}
		return vm, err
	}

	return vm.DeepCopy(), nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *VirtualMachineSnapshotReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&hitoseacomv1.VirtualMachineSnapshot{}).
		Complete(r)
}

func GetPVCsFromVolumes(volumes []kubevirtv1.Volume) map[string]string {
	pvcs := map[string]string{}

	for _, volume := range volumes {
		pvcName := PVCNameFromVirtVolume(&volume)
		if pvcName == "" {
			continue
		}

		pvcs[volume.Name] = pvcName
	}

	return pvcs
}

func PVCNameFromVirtVolume(volume *kubevirtv1.Volume) string {
	if volume.DataVolume != nil {
		// TODO, look up the correct PVC name based on the datavolume, right now they match, but that will not always be true.
		return volume.DataVolume.Name
	} else if volume.PersistentVolumeClaim != nil {
		return volume.PersistentVolumeClaim.ClaimName
	} else if volume.MemoryDump != nil {
		return volume.MemoryDump.ClaimName
	}

	return ""
}
