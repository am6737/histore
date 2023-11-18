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
	"github.com/go-logr/logr"
	"github.com/pkg/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clocks "k8s.io/utils/clock"
	kubevirtv1 "kubevirt.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
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

//+kubebuilder:rbac:groups=hitosea.com,resources=schedules,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=hitosea.com,resources=schedules/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=hitosea.com,resources=schedules/finalizers,verbs=update
//+kubebuilder:rbac:groups=hitosea.com,resources=virtualmachinesnapshots,verbs=get;list;watch;create;delete
//+kubebuilder:rbac:groups=hitosea.com,resources=virtualmachineschedulesnapshots/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=kubevirt.io,resources=virtualmachines,verbs=get;list;watch
//+kubebuilder:rbac:groups=kubevirt.io,resources=virtualmachines/status,verbs=get

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

	fmt.Println("ScheduleReconciler 1")

	// 更新状态
	if err := r.updateScheduleStatus(schedule); err != nil {
		return ctrl.Result{}, err
	}
	fmt.Println("ScheduleReconciler 2")

	if schedule.Status.Phase != hitoseacomv1.SchedulePhaseEnabled {
		r.Log.Info(fmt.Sprintf("the schedule's phase is %s, isn't %s, skip\n", schedule.Status.Phase, hitoseacomv1.SchedulePhaseEnabled))
		return ctrl.Result{}, nil
	}
	fmt.Println("ScheduleReconciler 3")

	var err error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		fmt.Println("backupSnapshots")
		if err = r.backupSnapshots(ctx, schedule); err != nil {
			r.Log.Error(err, "Error while creating snapshots")
		}
	}()
	go func() {
		defer wg.Done()
		fmt.Println("deleteExpiredSnapshots")
		if err = r.deleteExpiredSnapshots(ctx, schedule); err != nil {
			r.Log.Error(err, "Error while deleting snapshots")
		}
	}()
	wg.Wait()
	fmt.Println("ScheduleReconciler 4")

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ScheduleReconciler) SetupWithManager(mgr ctrl.Manager) error {
	s := NewPeriodicalEnqueueSource(mgr.GetClient(), r.Log, &hitoseacomv1.ScheduleList{}, scheduleSyncPeriod, PeriodicalEnqueueSourceOption{})
	return ctrl.NewControllerManagedBy(mgr).
		WithEventFilter(NewAllEventPredicate(func(obj client.Object) bool {
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

// NewAllEventPredicate creates a new Predicate that checks all the events with the provided func
func NewAllEventPredicate(f func(object client.Object) bool) predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(event event.CreateEvent) bool {
			return f(event.Object)
		},
		DeleteFunc: func(event event.DeleteEvent) bool {
			return f(event.Object)
		},
		UpdateFunc: func(event event.UpdateEvent) bool {
			return f(event.ObjectNew)
		},
		GenericFunc: func(event event.GenericEvent) bool {
			return f(event.Object)
		},
	}
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
	// 计算下次运行时间
	nextRunTime := now.Add(interval)
	return nextRunTime
}

func (r *ScheduleReconciler) backupSnapshots(ctx context.Context, schedule *hitoseacomv1.Schedule) error {
	nextRunTime := schedule.Status.NextBackup.Time
	if r.ifDue(nextRunTime, r.Clock.Now()) {
		r.Log.Info("创建快照handler", "namespace", schedule.Namespace)
		vmList := &kubevirtv1.VirtualMachineList{}
		if err := r.Client.List(context.TODO(), vmList); err != nil {
			r.Log.Error(err, "get vm list")
			return err
		}
		oldt := schedule.Status.LastBackup
		for _, vm := range vmList.Items {
			r.Log.Info("创建快照", "Name", "Snap-"+vm.Name)
			if err := r.Snap.CreateSnapshot(ctx, "schedule-snapshot-"+strconv.FormatInt(time.Now().UnixNano(), 10), vm.Namespace, "", vm.Name); err != nil {
				r.Log.Error(err, "CreateSnapshot")
				continue
			}
		}
		t1 := r.Clock.Now()
		schedule.Status.LastBackup = &metav1.Time{Time: t1}
		schedule.Status.NextBackup = &metav1.Time{Time: r.getNextRunTime(schedule.Spec.Schedule, t1)}
		if err := r.Client.Status().Update(context.Background(), schedule); err != nil {
			return err
		}

		r.Log.Info("Backup Task Complete", "lastBackup", oldt, "nextBackup", schedule.Status.NextBackup)
	}
	return nil
}

func (r *ScheduleReconciler) deleteExpiredSnapshots(ctx context.Context, schedule *hitoseacomv1.Schedule) error {
	nextRunTime := schedule.Status.NextDelete.Time
	if r.ifDue(nextRunTime, r.Clock.Now()) {
		vmsList := &hitoseacomv1.VirtualMachineSnapshotList{}
		if err := r.Client.List(context.TODO(), vmsList); err != nil {
			r.Log.Error(err, "get VirtualMachineSnapshotList")
			return err
		}

		// 先过滤掉特定标签的快照
		var filteredVMSSList []hitoseacomv1.VirtualMachineSnapshot
		for _, vmss := range vmsList.Items {
			if _, ok := vmss.ObjectMeta.Labels[ScheduleSnapshotLabelKey]; ok && vmss.Status.Phase != hitoseacomv1.InProgress {
				filteredVMSSList = append(filteredVMSSList, vmss)
			}
		}

		for _, vmss := range filteredVMSSList {
			// 检查快照创建时间是否超过了 vms.Spec.Template.TTL
			creationTime := vmss.ObjectMeta.CreationTimestamp.Time
			expirationTime := creationTime.Add(schedule.Spec.Template.TTL.Duration)

			// ttl到期
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
