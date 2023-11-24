/*
Copyright 2020 The Ceph-CSI Authors.

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
package rbd

import (
	"context"
	"errors"
	"fmt"
	"github.com/am6737/histore/pkg/ceph/util"
	"github.com/am6737/histore/pkg/util/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func CreateRBDVolumeFromSnapshotForce(
	ctx context.Context,
	parentVol *RbdVolume,
	rbdSnap *RbdSnapshot,
	cr *util.Credentials,
) (*RbdVolume, error) {
	// 1. 生成克隆卷的rbdVolume结构
	cloneRbd := GenerateVolFromSnap(rbdSnap)
	defer cloneRbd.Destroy()

	// 2. 连接到Ceph集群
	err := cloneRbd.Connect(cr)
	if err != nil {
		return cloneRbd, err
	}

	// 3. 从指定的快照创建RBD克隆
	err = createRBDClone(ctx, parentVol, cloneRbd, rbdSnap)
	if err != nil {
		fmt.Println(err.Error() + "  createRBDClone")
		return cloneRbd, err
	}

	// 4. 拷贝父卷的加密配置到克隆卷
	err = parentVol.copyEncryptionConfig(&cloneRbd.rbdImage, false)
	if err != nil {
		return nil, err
	}

	// 5. 获取克隆卷的ImageID
	err = cloneRbd.getImageID()
	if err != nil {
		fmt.Println(err.Error() + "  cloneRbd.getImageID")
		return cloneRbd, err
	}

	// 6. 将ImageID保存到journal
	j, err := snapJournal.Connect(cloneRbd.Monitors, cloneRbd.RadosNamespace, cr)
	if err != nil {
		fmt.Println(err.Error() + "  snapJournal.Connec")
		return cloneRbd, err
	}
	defer j.Destroy()

	err = j.StoreImageID(ctx, cloneRbd.JournalPool, cloneRbd.ReservedID, cloneRbd.ImageID)
	if err != nil {
		fmt.Println(err.Error() + " StoreImageID")
		return cloneRbd, err
	}

	// 7. 扁平化RBD克隆
	err = cloneRbd.flattenForceRbdImage(ctx)
	if err != nil {
		fmt.Println(err.Error() + "  cloneRbd.flattenRbdImage")
		return cloneRbd, err
	}

	return cloneRbd, nil
}

func CreateVolumeFromSnapshot(
	ctx context.Context,
	parentVol *RbdVolume,
	rbdSnap *RbdSnapshot,
	cr *util.Credentials,
) (*RbdVolume, error) {
	// 1. 生成克隆卷的rbdVolume结构
	cloneRbd := GenerateVolFromSnap(rbdSnap)
	defer cloneRbd.Destroy()

	// 2. 连接到Ceph集群
	err := cloneRbd.Connect(cr)
	if err != nil {
		return cloneRbd, err
	}

	// 3. 从指定的快照创建RBD克隆
	err = createRBDClone(ctx, parentVol, cloneRbd, rbdSnap)
	if err != nil {
		fmt.Println(err.Error() + "  createRBDClone")
		return cloneRbd, err
	}

	// 4. 拷贝父卷的加密配置到克隆卷
	err = parentVol.copyEncryptionConfig(&cloneRbd.rbdImage, false)
	if err != nil {
		return nil, err
	}

	// 5. 获取克隆卷的ImageID
	err = cloneRbd.getImageID()
	if err != nil {
		fmt.Println(err.Error() + "  cloneRbd.getImageID")
		return cloneRbd, err
	}

	// 6. 将ImageID保存到journal
	j, err := snapJournal.Connect(cloneRbd.Monitors, cloneRbd.RadosNamespace, cr)
	if err != nil {
		fmt.Println(err.Error() + "  snapJournal.Connec")
		return cloneRbd, err
	}
	defer j.Destroy()

	err = j.StoreImageID(ctx, cloneRbd.JournalPool, cloneRbd.ReservedID, cloneRbd.ImageID)
	if err != nil {
		fmt.Println(err.Error() + " StoreImageID")
		return cloneRbd, err
	}

	// 7. 扁平化RBD克隆
	err = cloneRbd.flattenRbdImage(ctx, false, rbdHardMaxCloneDepth, rbdSoftMaxCloneDepth)
	if err != nil {
		fmt.Println(err.Error() + "  cloneRbd.flattenRbdImage")
		return cloneRbd, err
	}

	return cloneRbd, nil
}

func createRBDClone(
	ctx context.Context,
	parentVol, cloneRbdVol *RbdVolume,
	snap *RbdSnapshot,
) error {
	//create snapshot
	err := parentVol.CreateSnapshot(ctx, snap)
	if err != nil {
		log.ErrorLog(ctx, "failed to create snapshot %s: %v", snap, err)
		return err
	}

	snap.RbdImageName = parentVol.RbdImageName
	// create clone image and delete snapshot
	err = cloneRbdVol.cloneRbdImageFromSnapshot(ctx, snap, parentVol)
	if err != nil {
		log.ErrorLog(
			ctx,
			"failed to clone rbd image %s from snapshot %s: %v",
			cloneRbdVol.RbdImageName,
			snap.RbdSnapName,
			err)
		err = fmt.Errorf(
			"failed to clone rbd image %s from snapshot %s: %w",
			cloneRbdVol.RbdImageName,
			snap.RbdSnapName,
			err)
	}
	errSnap := parentVol.deleteSnapshot(ctx, snap)
	if errSnap != nil {
		log.ErrorLog(ctx, "failed to delete snapshot: %v", errSnap)
		delErr := cloneRbdVol.deleteImage(ctx)
		if delErr != nil {
			log.ErrorLog(ctx, "failed to delete rbd image: %s with error: %v", cloneRbdVol, delErr)
		}

		return err
	}

	return nil
}

// CleanUpSnapshot removes the RBD-snapshot (rbdSnap) from the RBD-image
// (parentVol) and deletes the RBD-image rbdVol.
func CleanUpSnapshot(
	ctx context.Context,
	parentVol *RbdVolume,
	rbdSnap *RbdSnapshot,
	rbdVol *RbdVolume,
) error {
	err := parentVol.deleteSnapshot(ctx, rbdSnap)
	if err != nil {
		if !errors.Is(err, ErrSnapNotFound) {
			//log.ErrorLog(ctx, "failed to delete snapshot %q: %v", rbdSnap, err)

			return err
		}
	}

	if rbdVol != nil {
		err := rbdVol.deleteImage(ctx)
		if err != nil {
			if !errors.Is(err, ErrImageNotFound) {
				//log.ErrorLog(ctx, "failed to delete rbd image %q with error: %v", rbdVol, err)

				return err
			}
		}
	}

	return nil
}

func GenerateVolFromSnap(rbdSnap *RbdSnapshot) *RbdVolume {
	vol := new(RbdVolume)
	vol.ClusterID = rbdSnap.ClusterID
	vol.VolID = rbdSnap.VolID
	vol.Monitors = rbdSnap.Monitors
	vol.Pool = rbdSnap.Pool
	vol.JournalPool = rbdSnap.JournalPool
	vol.RadosNamespace = rbdSnap.RadosNamespace
	vol.RbdImageName = rbdSnap.RbdSnapName
	vol.ImageID = rbdSnap.ImageID
	// copyEncryptionConfig cannot be used here because the volume and the
	// snapshot will have the same volumeID which cases the panic in
	// copyEncryptionConfig function.
	vol.blockEncryption = rbdSnap.blockEncryption
	vol.fileEncryption = rbdSnap.fileEncryption

	return vol
}

func undoSnapshotCloning(
	ctx context.Context,
	parentVol *RbdVolume,
	rbdSnap *RbdSnapshot,
	cloneVol *RbdVolume,
	cr *util.Credentials,
) error {
	err := CleanUpSnapshot(ctx, parentVol, rbdSnap, cloneVol)
	if err != nil {
		//log.ErrorLog(ctx, "failed to clean up  %s or %s: %v", cloneVol, rbdSnap, err)

		return err
	}
	err = UndoSnapReservation(ctx, rbdSnap, cr)

	return err
}

var (
	maxSnapshotsOnImage               = 450
	minSnapshotsOnImageToStartFlatten = 250
)

// check snapshots on the rbd image, as we have limit from krbd that an image
// cannot have more than 510 snapshot at a given point of time. If the
// snapshots are more than the `maxSnapshotsOnImage` Add a task to flatten all
// the temporary cloned images and return ABORT error message. If the snapshots
// are more than the `minSnapshotOnImage` Add a task to flatten all the
// temporary cloned images.
func FlattenTemporaryClonedImages(ctx context.Context, rbdVol *RbdVolume, cr *util.Credentials) error {
	snaps, err := rbdVol.listSnapshots()
	if err != nil {
		if errors.Is(err, ErrImageNotFound) {
			return status.Error(codes.InvalidArgument, err.Error())
		}

		return status.Error(codes.Internal, err.Error())
	}

	if len(snaps) > int(maxSnapshotsOnImage) {
		log.DebugLog(
			ctx,
			"snapshots count %d on image: %s reached configured hard limit %d",
			len(snaps),
			rbdVol,
			maxSnapshotsOnImage)
		err = flattenClonedRbdImages(
			ctx,
			snaps,
			rbdVol.Pool,
			rbdVol.Monitors,
			rbdVol.RbdImageName,
			cr)
		if err != nil {
			return status.Error(codes.Internal, err.Error())
		}

		return status.Errorf(codes.ResourceExhausted, "rbd image %s has %d snapshots", rbdVol, len(snaps))
	}

	if len(snaps) > int(minSnapshotsOnImageToStartFlatten) {
		log.DebugLog(
			ctx,
			"snapshots count %d on image: %s reached configured soft limit %d",
			len(snaps),
			rbdVol,
			minSnapshotsOnImageToStartFlatten)
		// If we start flattening all the snapshots at one shot the volume
		// creation time will be affected,so we will flatten only the extra
		// snapshots.
		snaps = snaps[minSnapshotsOnImageToStartFlatten-1:]
		err = flattenClonedRbdImages(
			ctx,
			snaps,
			rbdVol.Pool,
			rbdVol.Monitors,
			rbdVol.RbdImageName,
			cr)
		if err != nil {
			return status.Error(codes.Internal, err.Error())
		}
	}

	return nil
}
