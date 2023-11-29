package controller

import (
	"context"
	"fmt"
	hitoseacomv1 "github.com/am6737/histore/api/v1"
	"github.com/go-logr/logr"
	"k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	kubevirtv1 "kubevirt.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"time"
)

// Snapshotter implements CreateSnapshot/DeleteSnapshot operations against a remote CSI driver.
type Snapshotter interface {
	// CreateSnapshot creates a VirtualMachineSnapshot for a volume
	CreateSnapshot(ctx context.Context, snapshotName, namespace, volumeSnapshotClassName, vm string) (err error)

	// DeleteSnapshot deletes a VirtualMachineSnapshot from a volume
	DeleteSnapshot(ctx context.Context, snapshotName, namespace string) (err error)

	// GetSnapshotStatus returns if a VirtualMachineSnapshot is ready to use, creation time, and restore size.
	GetSnapshotStatus(ctx context.Context, snapshotID string) (bool, time.Time, int64, error)
}

const ScheduleSnapshotLabelKey = "schedule.snapshot.hitosea.com"
const ScheduleSnapshotLabelValue = "true"

type VirtualMachineSnapshot struct {
	Client   client.Client
	Scheme   *runtime.Scheme
	recorder record.EventRecorder
	log      logr.Logger
}

func NewVirtualMachineSnapshot(client client.Client, scheme *runtime.Scheme) *VirtualMachineSnapshot {
	return &VirtualMachineSnapshot{
		Client: client,
		Scheme: scheme,
	}
}

func getRetentionPeriod(snapshot hitoseacomv1.VirtualMachineSnapshot) time.Duration {
	// 通过标签或其他方式获取快照的保留时间
	// 例如，如果快照对象有一个名为 "retention-period" 的标签，可以这样获取：
	// retentionStr := VirtualMachineSnapshot.Labels["retention-period"]
	// retention, err := time.ParseDuration(retentionStr)
	// 如果使用其他方式，请相应地获取保留时间
	// 这里使用一个示例保留时间 30 天
	return 30 * 24 * time.Hour
}

func isExpired(creationTime time.Time, retentionPeriod time.Duration) bool {
	now := time.Now()
	expirationTime := creationTime.Add(retentionPeriod)
	return now.After(expirationTime)
}

func (s *VirtualMachineSnapshot) CreateSnapshot(ctx context.Context, snapshotName, namespace, volumeSnapshotClassName, vm string) (err error) {
	klog.V(5).Infof("CSI CreateSnapshot: %s", snapshotName)

	//driverName := "YOUR_CSI_DRIVER_NAME" // 请替换为你的 CSI 驱动程序的名称

	snap := &hitoseacomv1.VirtualMachineSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:      snapshotName,
			Namespace: namespace,
			Labels: map[string]string{
				ScheduleSnapshotLabelKey: ScheduleSnapshotLabelValue,
			},
		},
		Spec: hitoseacomv1.VirtualMachineSnapshotSpec{
			Source: v1.TypedLocalObjectReference{
				APIGroup: &kubevirtv1.GroupVersion.Group,
				Kind:     "VirtualMachine",
				Name:     vm,
			},
			DeletionPolicy:  nil,
			FailureDeadline: nil,
		},
	}

	if err = s.Client.Create(ctx, snap); err != nil {
		return err
	}

	// 获取创建时间和大小
	//creationTime := Snap.CreationTimestamp.Time
	//size := Snap.Status.RestoreSize
	//readyToUse := Snap.Status.ReadyToUse

	return err
}

func (s *VirtualMachineSnapshot) DeleteSnapshot(ctx context.Context, snapshotName, namespace string) (err error) {
	snap := &hitoseacomv1.VirtualMachineSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:      snapshotName,
			Namespace: namespace,
		},
	}

	if err = s.Client.Delete(ctx, snap); err != nil && !apierrors.IsNotFound(err) {
		return err
	}

	return err
}

func (s *VirtualMachineSnapshot) GetSnapshotStatus(ctx context.Context, snapshotID string) (bool, time.Time, int64, error) {
	klog.V(5).Infof("GetSnapshotStatus: %s", snapshotID)

	snap := &hitoseacomv1.VirtualMachineSnapshot{}
	err := s.Client.Get(ctx, client.ObjectKey{Name: snapshotID}, snap)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return false, time.Time{}, 0, fmt.Errorf("Snap %s not found", snapshotID)
		}
		return false, time.Time{}, 0, err
	}

	creationTime := snap.CreationTimestamp.Time
	//size := Snap.Status.RestoreSize
	readyToUse := snap.Status.ReadyToUse

	return *readyToUse, creationTime, 0, nil
}
