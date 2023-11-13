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
)

func CreateRBDVolumeFromSnapshot(
	ctx context.Context,
	parentVol *RbdVolume,
	rbdSnap *rbdSnapshot,
	cr *util.Credentials,
) (*RbdVolume, error) {
	// 1. 生成克隆卷的rbdVolume结构
	cloneRbd := GenerateVolFromSnap(rbdSnap) // 传入nil表示不指定快照
	//cloneRbd.RbdImageName = strings.Replace(cloneRbd.RbdImageName, "csi-snap-", "csi-vol-", 1)
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
	err = cloneRbd.flattenRbdImage(ctx, false, 4, 8)
	if err != nil {
		fmt.Println(err.Error() + "  cloneRbd.flattenRbdImage")
		return cloneRbd, err
	}

	return cloneRbd, nil
}

func createRBDClone(
	ctx context.Context,
	parentVol, cloneRbdVol *RbdVolume,
	snap *rbdSnapshot,
) error {
	//create snapshot
	err := parentVol.CreateSnapshot(ctx, snap)
	if err != nil {
		//log.ErrorLog(ctx, "failed to create snapshot %s: %v", snap, err)
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
		//log.ErrorLog(ctx, "failed to delete snapshot: %v", errSnap)
		fmt.Println(ctx, "failed to delete snapshot: %v", errSnap)
		delErr := cloneRbdVol.DeleteImage(ctx)
		if delErr != nil {
			//log.ErrorLog(ctx, "failed to delete rbd image: %s with error: %v", cloneRbdVol, delErr)
		}

		return err
	}

	return nil
}

// cleanUpSnapshot removes the RBD-snapshot (rbdSnap) from the RBD-image
// (parentVol) and deletes the RBD-image rbdVol.
func cleanUpSnapshot(
	ctx context.Context,
	parentVol *RbdVolume,
	rbdSnap *rbdSnapshot,
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
		err := rbdVol.DeleteImage(ctx)
		if err != nil {
			if !errors.Is(err, ErrImageNotFound) {
				//log.ErrorLog(ctx, "failed to delete rbd image %q with error: %v", rbdVol, err)

				return err
			}
		}
	}

	return nil
}

func GenerateVolFromSnap(rbdSnap *rbdSnapshot) *RbdVolume {
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
	rbdSnap *rbdSnapshot,
	cloneVol *RbdVolume,
	cr *util.Credentials,
) error {
	err := cleanUpSnapshot(ctx, parentVol, rbdSnap, cloneVol)
	if err != nil {
		//log.ErrorLog(ctx, "failed to clean up  %s or %s: %v", cloneVol, rbdSnap, err)

		return err
	}
	err = undoSnapReservation(ctx, rbdSnap, cr)

	return err
}
