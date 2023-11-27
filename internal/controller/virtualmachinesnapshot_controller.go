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
	"github.com/am6737/histore/pkg/config"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"time"

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

	vmSnapshotContentCreateEvent = "SuccessfulVirtualMachineSnapshotContentCreate"

	vmSnapshotDeadlineExceededError = "snapshot deadline exceeded"
)

// VirtualMachineSnapshotReconciler reconciles a VirtualMachineSnapshot object
type VirtualMachineSnapshotReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
	Log      logr.Logger
}

//+kubebuilder:rbac:groups=hitosea.com,resources=virtualmachinesnapshots,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=hitosea.com,resources=virtualmachinesnapshots/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=hitosea.com,resources=virtualmachinesnapshots/finalizers,verbs=update
//+kubebuilder:rbac:groups=kubevirt.io,resources=virtualmachines,verbs=get;list;watch;update;patch
//+kubebuilder:rbac:groups=kubevirt.io,resources=virtualmachines/status,verbs=get
//+kubebuilder:rbac:groups=kubevirt.io,resources=virtualmachineinstances,verbs=get;list;watch
//+kubebuilder:rbac:groups=kubevirt.io,resources=virtualmachineinstances/status,verbs=get
//+kubebuilder:rbac:groups=cdi.kubevirt.io,resources=datavolumes,verbs=get;list;watch;create;delete
//+kubebuilder:rbac:groups=snapshot.storage.k8s.io,resources=volumesnapshots,verbs=get;list;watch;create;delete
//+kubebuilder:rbac:groups=snapshot.storage.k8s.io,resources=volumesnapshots/status,verbs=get
//+kubebuilder:rbac:groups=snapshot.storage.k8s.io,resources=volumesnapshotclasses,verbs=get;list;watch
//+kubebuilder:rbac:groups=snapshot.storage.k8s.io,resources=volumesnapshotcontents,verbs=get;list;watch;update;delete
//+kubebuilder:rbac:groups=snapshot.storage.k8s.io,resources=volumesnapshotcontents/status,verbs=get;list;watch;update
//+kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update
//+kubebuilder:rbac:groups="",resources=persistentvolumeclaims/status,verbs=get;list;watch;update
//+kubebuilder:rbac:groups="",resources=persistentvolumes,verbs=get;list;watch;create
//+kubebuilder:rbac:groups="",resources=events,verbs=create;patch
//+kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
//+kubebuilder:rbac:groups=storage.k8s.io,resources=storageclasses,verbs=get;list;watch

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
	logger := log.FromContext(ctx)

	// 1. 根据请求获取到 VirtualMachineSnapshot 对象
	vmSnapshot := &hitoseacomv1.VirtualMachineSnapshot{}
	if err := r.Get(ctx, req.NamespacedName, vmSnapshot); err != nil {
		if apierrors.IsNotFound(err) {
			// 对象不存在，可能已被删除，可以返回一个无需处理的结果
			return ctrl.Result{}, nil
		}
		logger.Error(err, "无法获取 VirtualMachineSnapshot 对象")
		return ctrl.Result{}, err
	}

	if vmSnapshotDeleting(vmSnapshot) {
		if err := r.removeFinalizerFromVms(vmSnapshot); err != nil {
			logger.Error(err, "Failed to remove VirtualMachineSnapshot finalizer")
			return reconcile.Result{RequeueAfter: 5 * time.Second}, nil
		}
		return reconcile.Result{}, nil
	} else {
		if err := r.addFinalizerToVms(vmSnapshot); err != nil {
			logger.Error(err, "Failed to add VirtualMachineSnapshot finalizer")
			return reconcile.Result{RequeueAfter: 5 * time.Second}, nil
		}
	}

	content, err := r.getContent(vmSnapshot)
	if err != nil {
		return ctrl.Result{}, err
	}

	// Make sure status is initialized before doing anything
	//if vmSnapshot.Status.VirtualMachineSnapshotContentName == nil {
	//	//if source != nil {
	//	if vmSnapshotProgressing(vmSnapshot) && !vmSnapshotTerminating(vmSnapshot) {
	//		// create content if does not exist
	//		if content == nil {
	//			if err := r.createContent(vmSnapshot); err != nil {
	//				return ctrl.Result{}, err
	//			}
	//		}
	//	}
	//	//}
	//}

	// create content if does not exist
	if content == nil {
		if err = r.createContent(vmSnapshot); err != nil {
			return ctrl.Result{}, err
		}
		//vmSnapshot.Status.Phase = hitoseacomv1.InProgress
	}

	if vmSnapshotTerminating(vmSnapshot) && content != nil {
		//vmSnapshot.Status.Phase = hitoseacomv1.Deleting
		// Delete content if that's the policy or if the snapshot
		// is marked to be deleted and the content is not ready yet
		// - no point of keeping an unready content
		if shouldDeleteContent(vmSnapshot, content) {
			r.Log.Info("Deleting vmsnapshotcontent", "namespace", content.Namespace, "name", content.Name)
			if err = r.Delete(ctx, content); err != nil && !apierrors.IsNotFound(err) {
				return ctrl.Result{}, err
			}
		} else {
			r.Log.Info("NOT deleting vmsnapshotcontent %s/%s", "namespace", content.Namespace, "name", content.Name)
		}
	}

	if err = r.updateSnapshotStatus(vmSnapshot); err != nil {
		r.Log.Error(err, "updateSnapshotStatus")
		return ctrl.Result{
			RequeueAfter: 5 * time.Second,
		}, nil
	}

	//_, err = r.getVM(vmSnapshot)
	//if err != nil {
	//	return ctrl.Result{}, err
	//}

	//if err = r.Status().Update(ctx, vmSnapshot); err != nil {
	//	logger.Error(err, "无法更新 VirtualMachineSnapshot 对象的状态")
	//	return reconcile.Result{}, nil
	//}

	return ctrl.Result{}, nil
}

func shouldDeleteContent(vmSnapshot *hitoseacomv1.VirtualMachineSnapshot, content *hitoseacomv1.VirtualMachineSnapshotContent) bool {
	return deleteContentPolicy(vmSnapshot) || !vmSnapshotContentReady(content)
}

func deleteContentPolicy(vmSnapshot *hitoseacomv1.VirtualMachineSnapshot) bool {
	return vmSnapshot.Spec.DeletionPolicy == nil ||
		*vmSnapshot.Spec.DeletionPolicy == hitoseacomv1.VirtualMachineSnapshotContentDelete
}

func vmSnapshotTerminating(vmSnapshot *hitoseacomv1.VirtualMachineSnapshot) bool {
	return vmSnapshotDeleting(vmSnapshot) || vmSnapshotDeadlineExceeded(vmSnapshot)
}

func vmSnapshotDeadlineExceeded(snapshot *hitoseacomv1.VirtualMachineSnapshot) bool {
	return false
}

func vmSnapshotDeleting(vmSnapshot *hitoseacomv1.VirtualMachineSnapshot) bool {
	return vmSnapshot != nil && vmSnapshot.DeletionTimestamp != nil
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
	if vm == nil {
		return fmt.Errorf("virtual machine %s not found", vmSnapshot.Spec.Source.Name)
	}

	var volumeBackups []hitoseacomv1.VolumeBackup
	pvcs := GetPVCsFromVolumes(vm.Spec.Template.Spec.Volumes)
	for volumeName, pvcName := range pvcs {
		pvc, err := r.getSnapshotPVC(vmSnapshot.Namespace, pvcName)
		if err != nil {
			return err
		}

		if pvc == nil {
			r.Log.Info("No snapshot PVC", vmSnapshot.Namespace, pvcName)
			continue
		}

		if _, ok := pvc.Labels["hitosea.com/histore"]; ok {
			r.Log.Info("PVC snapshots not supported", vmSnapshot.Namespace, pvcName)
			continue
		}

		//volumeSnapshotName := fmt.Sprintf("vmsnapshot-%s-volume-%s", vmSnapshot.UID, pvcName)
		vb := hitoseacomv1.VolumeBackup{
			VolumeName: volumeName,
			PersistentVolumeClaim: hitoseacomv1.PersistentVolumeClaim{
				ObjectMeta: *getSimplifiedMetaObject(pvc.ObjectMeta),
				Spec:       *pvc.Spec.DeepCopy(),
			},
			//VolumeSnapshotName: &volumeSnapshotName,
			VolumeSnapshotName: &vmSnapshot.Name,
		}

		volumeBackups = append(volumeBackups, vb)
	}

	msc, err := config.GetCephCsiConfigForSC(r.Client, config.DC.SlaveStorageClass)
	if err != nil {
		r.Log.Error(err, "getCephCsiConfigForSC")
		return err
	}

	vm.Status = kubevirtv1.VirtualMachineStatus{}
	content := &hitoseacomv1.VirtualMachineSnapshotContent{
		ObjectMeta: metav1.ObjectMeta{
			Name:      GetVMSnapshotContentName(vmSnapshot),
			Namespace: vmSnapshot.Namespace,
			//Finalizers: []string{vmSnapshotContentFinalizer},
			Annotations: map[string]string{
				prefixedSnapshotDeleteSecretNameKey:      msc.NodeStageSecretName,
				prefixedSnapshotDeleteSecretNamespaceKey: msc.NodeStageSecretNamespace,
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: hitoseacomv1.GroupVersion.String(),
					Kind:       "VirtualMachineSnapshot",
					Name:       vmSnapshot.Name,
					UID:        vmSnapshot.UID,
				},
			},
		},
		Spec: hitoseacomv1.VirtualMachineSnapshotContentSpec{
			VirtualMachineSnapshotName: &vmSnapshot.Name,
			Source: hitoseacomv1.SourceSpec{
				VirtualMachine: &hitoseacomv1.VirtualMachine{
					ObjectMeta:     *getSimplifiedMetaObject(vm.ObjectMeta),
					VirtualMachine: vm,
				},
			},
			VolumeBackups: volumeBackups,
		},
	}

	if err = r.Client.Create(context.Background(), content); err != nil {
		return err
	}

	r.Recorder.Eventf(
		vmSnapshot,
		corev1.EventTypeNormal,
		vmSnapshotContentCreateEvent,
		"Successfully created VirtualMachineSnapshotContent %s",
		content.Name,
	)

	return nil
}

func getSimplifiedMetaObject(meta metav1.ObjectMeta) *metav1.ObjectMeta {
	result := meta.DeepCopy()
	result.ManagedFields = nil

	return result
}

func (r *VirtualMachineSnapshotReconciler) getSnapshotPVC(namespace, volumeName string) (*corev1.PersistentVolumeClaim, error) {

	obj := &corev1.PersistentVolumeClaim{}
	if err := r.Client.Get(context.TODO(), client.ObjectKey{Namespace: namespace, Name: volumeName}, obj); err != nil {
		return nil, err
	}

	pvc := obj.DeepCopy()

	if pvc.Spec.VolumeName == "" {
		r.Log.Info("Unbound PVC", pvc.Namespace, pvc.Name)
		return nil, nil
	}

	if pvc.Spec.StorageClassName == nil {
		r.Log.Info("No storage class for PVC", pvc.Namespace, pvc.Name)
		return nil, nil
	}

	volumeSnapshotClass, err := r.getVolumeSnapshotClass(*pvc.Spec.StorageClassName)
	if err != nil {
		r.Log.Error(err, "No snapshot storage class for PVC", pvc.Namespace, pvc.Name)
		return nil, err
	}
	//
	//if volumeSnapshotClass == "" {
	//	r.Log.Info("No snapshot storage class for PVC", pvc.Namespace, pvc.Name)
	//	return pvc, nil
	//}

	if volumeSnapshotClass != "" {
		return pvc, nil
	}

	return nil, nil
}

func (r *VirtualMachineSnapshotReconciler) getVolumeSnapshotClass(storageClassName string) (string, error) {

	obj := &storagev1.StorageClass{}
	if err := r.Client.Get(context.TODO(), client.ObjectKey{Name: storageClassName}, obj); err != nil {
		if apierrors.IsNotFound(err) {
			return "", nil
		}
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
		log.Log.Info(fmt.Sprintf("No VolumeSnapshotClass for %s", storageClassName))
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

	objs := &snapshotv1.VolumeSnapshotClassList{}
	var vscs []snapshotv1.VolumeSnapshotClass

	if err := r.Client.List(context.TODO(), objs); err != nil {
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

func vmSnapshotContentReady(vmSnapshotContent *hitoseacomv1.VirtualMachineSnapshotContent) bool {
	return vmSnapshotContent.Status.ReadyToUse != nil && *vmSnapshotContent.Status.ReadyToUse
}

func vmSnapshotError(vmSnapshot *hitoseacomv1.VirtualMachineSnapshot) *hitoseacomv1.Error {
	if vmSnapshot != nil && vmSnapshot.Status != nil && vmSnapshot.Status.Error != nil {
		return vmSnapshot.Status.Error
	}
	return nil
}

func (r *VirtualMachineSnapshotReconciler) getContent(vmSnapshot *hitoseacomv1.VirtualMachineSnapshot) (*hitoseacomv1.VirtualMachineSnapshotContent, error) {
	contentName := GetVMSnapshotContentName(vmSnapshot)
	vmsc := &hitoseacomv1.VirtualMachineSnapshotContent{}
	err := r.Client.Get(context.TODO(), client.ObjectKey{
		Namespace: vmSnapshot.Namespace,
		Name:      contentName,
	}, vmsc)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// 如果资源不存在，则返回一个空的对象
			return nil, nil
		}
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

func (r *VirtualMachineSnapshotReconciler) updateSnapshotStatus(vmSnapshot *hitoseacomv1.VirtualMachineSnapshot) error {
	vmSnapshotCpy := vmSnapshot.DeepCopy()
	f := false
	if vmSnapshotCpy.Status == nil {
		vmSnapshotCpy.Status = &hitoseacomv1.VirtualMachineSnapshotStatus{
			ReadyToUse:   &f,
			CreationTime: currentTime(),
		}
	}

	content, err := r.getContent(vmSnapshot)
	if err != nil {
		return err
	}
	if vmSnapshotDeleting(vmSnapshotCpy) {
		// Enable the vmsnapshot to be deleted only in case it completed
		// or after waiting until the content is deleted if needed
		if !vmSnapshotProgressing(vmSnapshot) || contentDeletedIfNeeded(vmSnapshotCpy, content) {
			//RemoveFinalizer(vmSnapshotCpy, vmSnapshotFinalizer)
			time.Sleep(5 * time.Second)
			if err = r.removeFinalizerFromVms(vmSnapshotCpy); err != nil {
				return err
			}
		}
	} else {
		//AddFinalizer(vmSnapshotCpy, vmSnapshotFinalizer)
		if content != nil && content.Status != nil {
			// content exists and is initialized
			vmSnapshotCpy.Status.VirtualMachineSnapshotContentName = &content.Name
			vmSnapshotCpy.Status.CreationTime = content.Status.CreationTime
			vmSnapshotCpy.Status.ReadyToUse = content.Status.ReadyToUse
			vmSnapshotCpy.Status.Error = content.Status.Error
		}
	}
	if vmSnapshotDeadlineExceeded(vmSnapshotCpy) {
		vmSnapshotCpy.Status.Phase = hitoseacomv1.Failed
		updateSnapshotCondition(vmSnapshotCpy, newProgressingCondition(corev1.ConditionFalse, vmSnapshotDeadlineExceededError))
		updateSnapshotCondition(vmSnapshotCpy, newFailureCondition(corev1.ConditionTrue, vmSnapshotDeadlineExceededError))
	} else if vmSnapshotProgressing(vmSnapshotCpy) {
		vmSnapshotCpy.Status.Phase = hitoseacomv1.InProgress
		updateSnapshotCondition(vmSnapshotCpy, newProgressingCondition(corev1.ConditionTrue, "Operation complete"))
		updateSnapshotCondition(vmSnapshotCpy, newReadyCondition(corev1.ConditionFalse, "Not ready"))
		if vmSnapshotDeleting(vmSnapshotCpy) {
			vmSnapshotCpy.Status.Phase = hitoseacomv1.Deleting
			updateSnapshotCondition(vmSnapshotCpy, newProgressingCondition(corev1.ConditionFalse, "VM snapshot is deleting"))
			updateSnapshotCondition(vmSnapshotCpy, newReadyCondition(corev1.ConditionFalse, "VM snapshot is deleting"))
		}
	} else if vmSnapshotError(vmSnapshotCpy) != nil {
		updateSnapshotCondition(vmSnapshotCpy, newProgressingCondition(corev1.ConditionFalse, "In error state"))
		updateSnapshotCondition(vmSnapshotCpy, newReadyCondition(corev1.ConditionFalse, "Error"))
	} else if VmSnapshotReady(vmSnapshotCpy) {
		vmSnapshotCpy.Status.Phase = hitoseacomv1.Succeeded
		updateSnapshotCondition(vmSnapshotCpy, newProgressingCondition(corev1.ConditionFalse, "Operation complete"))
		updateSnapshotCondition(vmSnapshotCpy, newReadyCondition(corev1.ConditionTrue, "Operation complete"))
		updateSnapshotSnapshotableVolumes(vmSnapshotCpy, content)
	} else {
		vmSnapshotCpy.Status.Phase = hitoseacomv1.Unknown
		updateSnapshotCondition(vmSnapshotCpy, newProgressingCondition(corev1.ConditionUnknown, "Unknown state"))
		updateSnapshotCondition(vmSnapshotCpy, newReadyCondition(corev1.ConditionUnknown, "Unknown state"))
	}

	//return r.Status().Update(context.Background(), vmSnapshotCpy)

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		ctx := context.Background()
		var newVmSnapshot hitoseacomv1.VirtualMachineSnapshot
		if err = r.Client.Get(ctx, client.ObjectKey{
			Namespace: vmSnapshotCpy.Namespace,
			Name:      vmSnapshotCpy.Name,
		}, &newVmSnapshot); err != nil {
			return err
		}
		newVmSnapshot.Status = vmSnapshotCpy.Status
		if err = r.Client.Status().Update(ctx, &newVmSnapshot); err != nil {
			return fmt.Errorf("failed to update resource status: %w", err)
		}

		return nil
	})
}

func contentDeletedIfNeeded(vmSnapshot *hitoseacomv1.VirtualMachineSnapshot, content *hitoseacomv1.VirtualMachineSnapshotContent) bool {
	return content == nil || !shouldDeleteContent(vmSnapshot, content)
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
