/*
Copyright 2019 The Ceph-CSI Authors.

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
	"github.com/am6737/histore/pkg/ceph/journal"
	"github.com/am6737/histore/pkg/ceph/util"
)

var (
	//snapJournal *journal.Config
	snapJournal = journal.NewCSISnapshotJournal("default")
)

const (
	// volIDVersion is the version number of volume ID encoding scheme.
	volIDVersion uint16 = 1
)

func validateNonEmptyField(field, fieldName, structName string) error {
	if field == "" {
		return fmt.Errorf("value '%s' in '%s' structure cannot be empty", fieldName, structName)
	}

	return nil
}

func validateRbdSnap(rbdSnap *rbdSnapshot) error {
	var err error

	if err = validateNonEmptyField(rbdSnap.RequestName, "RequestName", "rbdSnapshot"); err != nil {
		return err
	}

	if err = validateNonEmptyField(rbdSnap.Monitors, "Monitors", "rbdSnapshot"); err != nil {
		return err
	}

	if err = validateNonEmptyField(rbdSnap.Pool, "Pool", "rbdSnapshot"); err != nil {
		return err
	}

	if err = validateNonEmptyField(rbdSnap.RbdImageName, "RbdImageName", "rbdSnapshot"); err != nil {
		return err
	}

	if err = validateNonEmptyField(rbdSnap.ClusterID, "ClusterID", "rbdSnapshot"); err != nil {
		return err
	}

	return err
}

func getEncryptionConfig(rbdVol *RbdVolume) (string, util.EncryptionType) {
	switch {
	case rbdVol.isBlockEncrypted():
		return rbdVol.blockEncryption.GetID(), util.EncryptionTypeBlock
	case rbdVol.isFileEncrypted():
		return rbdVol.fileEncryption.GetID(), util.EncryptionTypeFile
	default:
		return "", util.EncryptionTypeNone
	}
}

// undoVolReservation is a helper routine to undo a name reservation for rbdVolume.
func UndoVolReservation(ctx context.Context, rbdVol *RbdVolume, cr *util.Credentials) error {
	j, err := volJournal.Connect(rbdVol.Monitors, rbdVol.RadosNamespace, cr)
	if err != nil {
		return err
	}
	defer j.Destroy()

	err = j.UndoReservation(ctx, rbdVol.JournalPool, rbdVol.Pool,
		rbdVol.RbdImageName, rbdVol.RequestName)

	return err
}

// ReserveVol is a helper routine to request a rbdVolume name reservation and generate the
// volume ID for the generated name.
func ReserveVol(ctx context.Context, rbdVol *RbdVolume, rbdSnap *rbdSnapshot, cr *util.Credentials) error {
	var err error

	//err = updateTopologyConstraints(rbdVol, rbdSnap)
	//if err != nil {
	//	return err
	//}

	journalPoolID, imagePoolID, err := util.GetPoolIDs(ctx, rbdVol.Monitors, rbdVol.JournalPool, rbdVol.Pool, cr)
	if err != nil {
		return err
	}

	kmsID, encryptionType := getEncryptionConfig(rbdVol)

	j, err := volJournal.Connect(rbdVol.Monitors, rbdVol.RadosNamespace, cr)
	if err != nil {
		return err
	}
	defer j.Destroy()

	rbdVol.ReservedID, rbdVol.RbdImageName, err = j.ReserveName(
		ctx, rbdVol.JournalPool, journalPoolID, rbdVol.Pool, imagePoolID,
		rbdVol.RequestName, rbdVol.NamePrefix, "", kmsID, rbdVol.ReservedID, rbdVol.Owner, "", encryptionType)
	if err != nil {
		return err
	}

	rbdVol.VolID, err = util.GenerateVolID(ctx, rbdVol.Monitors, cr, imagePoolID, rbdVol.Pool,
		rbdVol.ClusterID, rbdVol.ReservedID, volIDVersion)
	if err != nil {
		return err
	}

	fmt.Println(ctx, "generated Volume ID (%s) and image name (%s) for request name (%s)",
		rbdVol.VolID, rbdVol.RbdImageName, rbdVol.RequestName)

	return nil
}

func CheckSnapCloneExists(
	ctx context.Context,
	parentVol *RbdVolume,
	rbdSnap *rbdSnapshot,
	cr *util.Credentials,
) (bool, error) {
	err := validateRbdSnap(rbdSnap)
	if err != nil {
		return false, err
	}

	j, err := snapJournal.Connect(rbdSnap.Monitors, rbdSnap.RadosNamespace, cr)
	if err != nil {
		return false, err
	}
	defer j.Destroy()

	snapData, err := j.CheckReservation(ctx, rbdSnap.JournalPool,
		rbdSnap.RequestName, rbdSnap.NamePrefix, rbdSnap.RbdImageName, "", util.EncryptionTypeNone)
	if err != nil {
		return false, err
	}
	if snapData == nil {
		return false, nil
	}
	snapUUID := snapData.ImageUUID
	rbdSnap.RbdSnapName = snapData.ImageAttributes.ImageName
	rbdSnap.ImageID = snapData.ImageAttributes.ImageID

	// it should never happen that this disagrees, but check
	if rbdSnap.Pool != snapData.ImagePool {
		return false, fmt.Errorf("stored snapshot pool (%s) and expected snapshot pool (%s) mismatch",
			snapData.ImagePool, rbdSnap.Pool)
	}

	vol := GenerateVolFromSnap(rbdSnap)
	defer vol.Destroy()
	err = vol.Connect(cr)
	if err != nil {
		return false, err
	}
	vol.ReservedID = snapUUID
	// Fetch on-disk image attributes
	err = vol.GetImageInfo()
	if err != nil {
		if errors.Is(err, ErrImageNotFound) {
			err = parentVol.deleteSnapshot(ctx, rbdSnap)
			if err != nil {
				if !errors.Is(err, ErrSnapNotFound) {
					//log.ErrorLog(ctx, "failed to delete snapshot %s: %v", rbdSnap, err)

					return false, err
				}
			}
			err = undoSnapshotCloning(ctx, parentVol, rbdSnap, vol, cr)
		}

		return false, err
	}

	// Snapshot creation transaction is rolled forward if rbd clone image
	// representing the snapshot is found. Any failures till finding the image
	// causes a roll back of the snapshot creation transaction.
	// Code from here on, rolls the transaction forward.

	rbdSnap.CreatedAt = vol.CreatedAt
	rbdSnap.VolSize = vol.VolSize
	// found a snapshot already available, process and return its information
	rbdSnap.VolID, err = util.GenerateVolID(ctx, rbdSnap.Monitors, cr, snapData.ImagePoolID, rbdSnap.Pool,
		rbdSnap.ClusterID, snapUUID, volIDVersion)
	if err != nil {
		return false, err
	}

	// check snapshot exists if not create it
	err = vol.checkSnapExists(rbdSnap)
	if errors.Is(err, ErrSnapNotFound) {
		// create snapshot
		sErr := vol.createSnapshot(ctx, rbdSnap)
		if sErr != nil {
			//log.ErrorLog(ctx, "failed to create snapshot %s: %v", rbdSnap, sErr)
			err = undoSnapshotCloning(ctx, parentVol, rbdSnap, vol, cr)

			return false, err
		}
	}
	if err != nil {
		return false, err
	}

	if vol.ImageID == "" {
		sErr := vol.getImageID()
		if sErr != nil {
			//log.ErrorLog(ctx, "failed to get image id %s: %v", vol, sErr)
			err = undoSnapshotCloning(ctx, parentVol, rbdSnap, vol, cr)

			return false, err
		}
		sErr = j.StoreImageID(ctx, vol.JournalPool, vol.ReservedID, vol.ImageID)
		if sErr != nil {
			//log.ErrorLog(ctx, "failed to store volume id %s: %v", vol, sErr)
			err = undoSnapshotCloning(ctx, parentVol, rbdSnap, vol, cr)

			return false, err
		}
	}

	//log.DebugLog(ctx, "found existing image (%s) with name (%s) for request (%s)",
	//	rbdSnap.VolID, rbdSnap.RbdSnapName, rbdSnap.RequestName)

	fmt.Println(ctx, "found existing image (%s) with name (%s) for request (%s)",
		rbdSnap.VolID, rbdSnap.RbdSnapName, rbdSnap.RequestName)

	return true, nil
}

// undoSnapReservation is a helper routine to undo a name reservation for rbdSnapshot.
func undoSnapReservation(ctx context.Context, rbdSnap *rbdSnapshot, cr *util.Credentials) error {
	j, err := snapJournal.Connect(rbdSnap.Monitors, rbdSnap.RadosNamespace, cr)
	if err != nil {
		return err
	}
	defer j.Destroy()

	err = j.UndoReservation(
		ctx, rbdSnap.JournalPool, rbdSnap.Pool, rbdSnap.RbdSnapName,
		rbdSnap.RequestName)

	return err
}

// storeImageID retrieves the image ID and stores it in OMAP.
func (rv *RbdVolume) storeImageID(ctx context.Context, j *journal.Connection) error {
	err := rv.getImageID()
	if err != nil {
		//log.ErrorLog(ctx, "failed to get image id %s: %v", rv, err)
		fmt.Println(ctx, "failed to get image id %s: %v", rv, err)

		return err
	}
	err = j.StoreImageID(ctx, rv.JournalPool, rv.ReservedID, rv.ImageID)
	if err != nil {
		//log.ErrorLog(ctx, "failed to store volume id %s: %v", rv, err)
		fmt.Println(ctx, "failed to store volume id %s: %v", rv, err)

		return err
	}

	return nil
}
