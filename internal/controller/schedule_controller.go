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
	"github.com/am6737/histore/pkg/util/filter"
	"github.com/am6737/histore/pkg/util/kube"
	"github.com/go-logr/logr"
	"github.com/pkg/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	clocks "k8s.io/utils/clock"
	kubevirtv1 "kubevirt.io/api/core/v1"
	"strconv"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	hitoseacomv1 "github.com/am6737/histore/api/v1"
)

const (
	scheduleSyncPeriod = time.Minute
	// the default TTL for a backup
	defaultBackupTTL = 30 * 24 * time.Hour

	defaultBackupSchedule = "168h"
)

// ScheduleReconciler reconciles a Schedule object
type ScheduleReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Clock  clocks.WithTickerAndDelayedExecution

	Snap Snapshotter

	Log logr.Logger
}

//+kubebuilder:rbac:groups=snapshot.hitosea.com,resources=schedules,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=snapshot.hitosea.com,resources=schedules/Status,verbs=get;update;patch
//+kubebuilder:rbac:groups=snapshot.hitosea.com,resources=schedules/finalizers,verbs=update
//+kubebuilder:rbac:groups=snapshot.hitosea.com,resources=virtualmachinesnapshots,verbs=get;list;watch;create;delete
//+kubebuilder:rbac:groups=snapshot.hitosea.com,resources=virtualmachineschedulesnapshots/Status,verbs=get;update;patch
//+kubebuilder:rbac:groups=kubevirt.io,resources=virtualmachines,verbs=get;list;watch
//+kubebuilder:rbac:groups=kubevirt.io,resources=virtualmachines/Status,verbs=get

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the Schedule object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.14.4/pkg/reconcile
func (r *ScheduleReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	_ = log.FromContext(ctx)

	schedule := &hitoseacomv1.Schedule{}
	if err := r.Get(ctx, req.NamespacedName, schedule); err != nil {
		if apierrors.IsNotFound(err) {
			r.Log.Error(err, "schedule not found")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, errors.Wrapf(err, "error getting schedule %s", req.String())
	}

	if err := r.updateScheduleStatus(schedule); err != nil {
		return ctrl.Result{}, err
	}

	if schedule.Status.Phase != hitoseacomv1.SchedulePhaseEnabled {
		r.Log.Info(fmt.Sprintf("the schedule's phase is %s, isn't %s, skip\n", schedule.Status.Phase, hitoseacomv1.SchedulePhaseEnabled))
		return ctrl.Result{}, nil
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	var e []error

	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := r.backupSnapshots(ctx, schedule); err != nil {
			mu.Lock()
			e = append(e, err)
			mu.Unlock()
		}
	}()
	go func() {
		defer wg.Done()
		if err := r.deleteExpiredSnapshots(ctx, schedule); err != nil {
			mu.Lock()
			e = append(e, err)
			mu.Unlock()
		}
	}()
	wg.Wait()

	if len(e) > 0 {
		r.Log.Info(fmt.Sprintf("Requeuing reconciliation due to %v", e))
		return ctrl.Result{Requeue: true}, nil
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ScheduleReconciler) SetupWithManager(mgr ctrl.Manager) error {
	s := kube.NewPeriodicalEnqueueSource(mgr.GetClient(), r.Log, &hitoseacomv1.ScheduleList{}, scheduleSyncPeriod, kube.PeriodicalEnqueueSourceOption{})
	return ctrl.NewControllerManagedBy(mgr).
		WithEventFilter(kube.NewAllEventPredicate(func(obj client.Object) bool {
			schedule := obj.(*hitoseacomv1.Schedule)
			if pause := schedule.Spec.Paused; pause {
				schedule.Status.Phase = hitoseacomv1.SchedulePhasePause
				if err := r.Client.Status().Update(context.Background(), schedule); err != nil {
					r.Log.Error(err, "Update Status")
				}
				return false
			}
			return true
		})).
		For(&hitoseacomv1.Schedule{}).
		Watches(s, nil).
		Complete(r)
}

func (r *ScheduleReconciler) updateScheduleStatus(vms *hitoseacomv1.Schedule) error {
	if vms.Spec.Paused {
		vms.Status.Phase = hitoseacomv1.SchedulePhasePause
	} else {
		vms.Status.Phase = hitoseacomv1.SchedulePhaseEnabled
	}

	if vms.Spec.Schedule == "" {
		vms.Spec.Schedule = defaultBackupSchedule
	}

	if vms.Spec.Template.TTL.Duration == 0 {
		vms.Spec.Template.TTL.Duration = defaultBackupTTL
	}

	if vms.Status.NextBackup == nil {
		nextRunTime := r.getNextRunTime(vms.Spec.Schedule, r.Clock.Now())
		vms.Status.NextBackup = &metav1.Time{Time: nextRunTime}
	}

	if vms.Status.NextDelete == nil {
		nextRunTime := r.getNextRunTime(vms.Spec.Template.TTL.Duration.String(), r.Clock.Now())
		vms.Status.NextDelete = &metav1.Time{Time: nextRunTime}
	}

	if vms.Status.Schedule == "" {
		vms.Status.Schedule = vms.Spec.Schedule
	}

	if vms.Status.TTL.Duration == 0 {
		vms.Status.TTL = vms.Spec.Template.TTL
	}

	if vms.Status.Schedule != vms.Spec.Schedule {
		nextRunTime := r.getNextRunTime(vms.Spec.Schedule, r.Clock.Now())
		r.Log.Info("Update Backup Task", "oldNextBackup", vms.Status.NextBackup.Time.String(), "newNextBackup", nextRunTime)
		vms.Status.NextBackup = &metav1.Time{Time: nextRunTime}
		vms.Status.Schedule = vms.Spec.Schedule
	}

	if vms.Status.TTL != vms.Spec.Template.TTL {
		nextRunTime := r.getNextRunTime(vms.Spec.Template.TTL.Duration.String(), r.Clock.Now())
		r.Log.Info("Update Delete Task", "oldNextDelete", vms.Status.NextDelete, "newNextDelete", nextRunTime)
		vms.Status.NextDelete = &metav1.Time{Time: nextRunTime}
		vms.Status.TTL = vms.Spec.Template.TTL
	}

	return r.Client.Status().Update(context.Background(), vms)
}

func (r *ScheduleReconciler) getNextRunTime(schedule string, now time.Time) time.Time {
	interval, err := time.ParseDuration(schedule)
	if err != nil {
		r.Log.Error(err, "error parsing schedule")
		return time.Time{}
	}
	// 计算下一次运行时间
	nextRunTime := now.Add(interval)
	return nextRunTime
}

func (r *ScheduleReconciler) backupSnapshots(ctx context.Context, schedule *hitoseacomv1.Schedule) error {
	nextRunTime := schedule.Status.NextBackup.Time
	if r.ifDue(nextRunTime, r.Clock.Now()) {

		filteredResources, err := r.getResourcesToBackup(schedule)
		if err != nil {
			return err
		}

		for _, vm := range filteredResources {
			name := "schedule-snapshot-" + strconv.FormatInt(time.Now().Unix(), 10) + "-" + vm.GetName()
			r.Log.Info("Attempting to create Scheduled Tasks", "Name", name)
			if err := r.Snap.CreateSnapshot(ctx, name, vm.GetNamespace(), "", vm.GetName()); err != nil {
				r.Log.Error(err, "CreateSnapshot")
				continue
			}
		}

		oldt := schedule.Status.LastBackup
		t1 := r.Clock.Now()
		schedule.Status.LastBackup = &metav1.Time{Time: t1}
		schedule.Status.NextBackup = &metav1.Time{Time: r.getNextRunTime(schedule.Spec.Schedule, t1)}
		if err = r.Client.Status().Update(context.Background(), schedule); err != nil {
			return err
		}

		r.Log.Info("Backup Task Complete", "lastBackup", oldt, "nextBackup", schedule.Status.NextBackup)
	}
	return nil
}

func (r *ScheduleReconciler) getResourcesToBackup(schedule *hitoseacomv1.Schedule) ([]*unstructured.Unstructured, error) {
	vmList := &kubevirtv1.VirtualMachineList{}
	if err := r.Client.List(context.TODO(), vmList); err != nil {
		r.Log.Error(err, "get vm list")
		return nil, err
	}

	var resources []*unstructured.Unstructured
	for i := range vmList.Items {
		vm := vmList.Items[i]
		obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&vm)
		if err != nil {
			r.Log.Error(err, "Failed to convert to Unstructured")
			return nil, err
		}
		unstructuredObj := &unstructured.Unstructured{Object: obj}
		resources = append(resources, unstructuredObj)
	}

	filteredResources := (&filter.VirtualMachineFilter{
		IncludedNamespaces:       schedule.Spec.Template.IncludedNamespaces,
		ExcludedNamespaces:       schedule.Spec.Template.ExcludedNamespaces,
		LabelSelector:            schedule.Spec.Template.LabelSelector,
		OrLabelSelectors:         schedule.Spec.Template.OrLabelSelectors,
		ExcludedLabelSelector:    schedule.Spec.Template.ExcludedLabelSelector,
		ExcludedOrLabelSelectors: schedule.Spec.Template.ExcludedOrLabelSelectors,
	}).Filter(resources)

	return filteredResources, nil
}

func (r *ScheduleReconciler) deleteExpiredSnapshots(ctx context.Context, schedule *hitoseacomv1.Schedule) error {
	nextRunTime := schedule.Status.NextDelete.Time
	// 检查下次删除的时间是否已到
	if r.ifDue(nextRunTime, r.Clock.Now()) {
		vmsList := &hitoseacomv1.VirtualMachineSnapshotList{}
		if err := r.Client.List(context.TODO(), vmsList); err != nil {
			r.Log.Error(err, "get VirtualMachineSnapshotList")
			return err
		}

		// 只获取Schedule创建的虚拟机快照资源
		var filteredVMSSList []hitoseacomv1.VirtualMachineSnapshot
		for _, vms := range vmsList.Items {
			if _, ok := vms.ObjectMeta.Labels[ScheduleSnapshotLabelKey]; ok && vms.Status.Phase != hitoseacomv1.InProgress {
				filteredVMSSList = append(filteredVMSSList, vms)
			}
		}

		for _, vmss := range filteredVMSSList {
			// 检查快照创建时间是否超过了 vms.Spec.Template.TTL
			creationTime := vmss.ObjectMeta.CreationTimestamp.Time
			expirationTime := creationTime.Add(schedule.Spec.Template.TTL.Duration)

			// 判断是否超过了TTL过期时间
			if r.Clock.Now().After(expirationTime) {
				if err := r.Snap.DeleteSnapshot(ctx, vmss.Name, vmss.Namespace); err != nil {
					r.Log.Error(err, "DeleteSnapshot")
					continue
				}
			}
		}
		oldtd := schedule.Status.LastDelete
		t1 := r.Clock.Now()
		schedule.Status.LastDelete = &metav1.Time{Time: t1}
		schedule.Status.NextDelete = &metav1.Time{Time: r.getNextRunTime(schedule.Spec.Template.TTL.Duration.String(), t1)}
		if err := r.Client.Status().Update(context.Background(), schedule); err != nil {
			return err
		}

		r.Log.Info("Delete Task Complete", "lastDelete", oldtd, "nextDelete", schedule.Status.NextDelete)
	}
	return nil
}

// ifDue check whether schedule is due to
func (r *ScheduleReconciler) ifDue(nextRunTime, now time.Time) bool {
	//log := r.Log.WithValues("VirtualMachineScheduleReconciler", NamespaceAndName(schedule))
	//r.Log.Info("ifDueHandler", "当前时间", now.String(), "下次执行任务时间", nextRunTime.String())

	// 计算下次运行时间
	return now.After(nextRunTime)
}
