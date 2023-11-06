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
	"github.com/am6737/histore/internal/snapshot"
	vsv1 "github.com/kubernetes-csi/external-snapshotter/client/v6/apis/volumesnapshot/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	hitoseacomv1 "github.com/am6737/histore/api/v1"
)

// VirtualMachineSnapshotContentReconciler reconciles a VirtualMachineSnapshotContent object
type VirtualMachineSnapshotContentReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	vs snapshot.VMSnapshotController
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

	var content hitoseacomv1.VirtualMachineSnapshotContent
	if err := r.Get(ctx, req.NamespacedName, &content); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		logger.Error(err, "无法获取 VirtualMachineSnapshot 对象")
		return ctrl.Result{}, err
	}
	vmSnapshot, err := r.getVMSnapshot(content)
	if err != nil {
		return ctrl.Result{}, err
	}

	// TODO 判断vmSnapshot是否为nil或者状态为停止
	if vmSnapshot == nil || vmSnapshotTerminating(vmSnapshot) {
		// TODO 移除终结器
		return ctrl.Result{}, nil
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

		if volumeSnapshot == nil {

			if vmSnapshot == nil || r.vmSnapshotDeleting(vmSnapshot) {
				log.Log.V(3).Info("Not creating snapshot %s because vm snapshot is deleted", vsName)
				skippedSnapshots = append(skippedSnapshots, vsName)
				continue
			}

			volumeSnapshot, err = r.vs.CreateVolumeSnapshot(&content, &volumeBackup)
			if err != nil {
				return ctrl.Result{}, err
			}
		}

		vss := hitoseacomv1.VirtualMachineSnapshotContentStatus{}

		if volumeSnapshot.Status != nil {
			vss.ReadyToUse = volumeSnapshot.Status.ReadyToUse
			vss.CreationTime = volumeSnapshot.Status.CreationTime
			//vss.Error = translateError(volumeSnapshot.Status.Error)
		}
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *VirtualMachineSnapshotContentReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&hitoseacomv1.VirtualMachineSnapshotContent{}).
		Complete(r)
}

func (r *VirtualMachineSnapshotContentReconciler) getVMSnapshot(vmsc hitoseacomv1.VirtualMachineSnapshotContent) (*hitoseacomv1.VirtualMachineSnapshot, error) {
	vmSnapshot := &hitoseacomv1.VirtualMachineSnapshot{}
	if err := r.Get(context.TODO(), client.ObjectKey{
		Namespace: vmsc.Namespace,
		Name:      *vmsc.Spec.VirtualMachineSnapshotName,
	}, vmSnapshot); err != nil {
		return nil, err
	}
	return vmSnapshot, nil
}

func (r *VirtualMachineSnapshotContentReconciler) GetVolumeSnapshot(namespace string, name string) (*vsv1.VolumeSnapshot, error) {
	volumeSnapshot := &vsv1.VolumeSnapshot{}
	if err := r.Get(context.TODO(), client.ObjectKey{
		Namespace: namespace,
		Name:      name,
	}, volumeSnapshot); err != nil {
		return volumeSnapshot, err
	}
	return volumeSnapshot, nil
}

func (r *VirtualMachineSnapshotContentReconciler) vmSnapshotDeleting(snapshot *hitoseacomv1.VirtualMachineSnapshot) bool {
	return !snapshot.ObjectMeta.DeletionTimestamp.IsZero()
}
