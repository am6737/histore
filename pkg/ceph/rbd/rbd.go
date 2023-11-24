package rbd

import (
	"context"
	"errors"
	"fmt"
	"github.com/am6737/histore/pkg/ceph/util"
	"github.com/am6737/histore/pkg/util/log"
	librbd "github.com/ceph/go-ceph/rbd"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"strings"
)

// CloneFromSnapshot is a helper for CreateSnapshot that continues creating an
// RBD image from an RBD snapshot if the process was interrupted at one point.
func CloneFromSnapshot(
	ctx context.Context,
	rbdVol *RbdVolume,
	rbdSnap *RbdSnapshot,
	cr *util.Credentials,
	parameters map[string]string,
) (*csi.CreateSnapshotResponse, error) {
	vol := GenerateVolFromSnap(rbdSnap)
	err := vol.Connect(cr)
	if err != nil {
		uErr := undoSnapshotCloning(ctx, rbdVol, rbdSnap, vol, cr)
		if uErr != nil {
			fmt.Println(ctx, fmt.Sprintf("failed undoing reservation of snapshot: %s %v", rbdSnap.RequestName, uErr))
		}

		return nil, status.Errorf(codes.Internal, err.Error())
	}
	defer vol.Destroy()

	err = rbdVol.copyEncryptionConfig(&vol.rbdImage, false)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	err = vol.flattenRbdImage(ctx, false, rbdHardMaxCloneDepth, rbdSoftMaxCloneDepth)
	if errors.Is(err, ErrFlattenInProgress) {
		// if flattening is in progress, return error and do not cleanup
		return nil, status.Errorf(codes.Internal, err.Error())
	} else if err != nil {
		uErr := undoSnapshotCloning(ctx, rbdVol, rbdSnap, vol, cr)
		if uErr != nil {
			fmt.Println(ctx, "failed undoing reservation of snapshot: %s %v", rbdSnap.RequestName, uErr)
		}

		return nil, status.Errorf(codes.Internal, err.Error())
	}

	// Update snapshot-name/snapshot-namespace/snapshotcontent-name details on
	// RBD backend image as metadata on restart of provisioner pod when image exist
	if len(parameters) != 0 {
		metadata := GetSnapshotMetadata(parameters)
		err = rbdVol.setAllMetadata(metadata)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}

	return &csi.CreateSnapshotResponse{
		Snapshot: &csi.Snapshot{
			SizeBytes:      rbdSnap.VolSize,
			SnapshotId:     rbdSnap.VolID,
			SourceVolumeId: rbdSnap.SourceVolumeID,
			CreationTime:   rbdSnap.CreatedAt,
			ReadyToUse:     true,
		},
	}, nil
}

// GetSnapshotMetadata filter parameters, only return
// snapshot-name/snapshot-namespace/snapshotcontent-name metadata.
func GetSnapshotMetadata(parameters map[string]string) map[string]string {
	keys := []string{volSnapNameKey, volSnapNamespaceKey, volSnapContentNameKey}
	newParam := map[string]string{}
	for k, v := range parameters {
		for _, key := range keys {
			if strings.Contains(k, key) {
				newParam[k] = v
			}
		}
	}

	return newParam
}

const (
	csiParameterPrefix = "csi.storage.k8s.io/"

	volSnapNameKey        = csiParameterPrefix + "volumesnapshot/name"
	volSnapNamespaceKey   = csiParameterPrefix + "volumesnapshot/namespace"
	volSnapContentNameKey = csiParameterPrefix + "volumesnapshotcontent/name"
)

func CleanupRBDImage(ctx context.Context,
	rbdVol *RbdVolume, cr *util.Credentials,
) (*csi.DeleteVolumeResponse, error) {
	mirroringInfo, err := rbdVol.GetImageMirroringInfo()
	if err != nil {
		log.ErrorLog(ctx, err.Error())

		return nil, status.Error(codes.Internal, err.Error())
	}
	// Cleanup only omap data if the following condition is met
	// Mirroring is enabled on the image
	// Local image is secondary
	// Local image is in up+replaying state
	if mirroringInfo.State == librbd.MirrorImageEnabled && !mirroringInfo.Primary {
		// If the image is in a secondary state and its up+replaying means its
		// an healthy secondary and the image is primary somewhere in the
		// remote cluster and the local image is getting replayed. Delete the
		// OMAP data generated as we cannot delete the secondary image. When
		// the image on the primary cluster gets deleted/mirroring disabled,
		// the image on all the remote (secondary) clusters will get
		// auto-deleted. This helps in garbage collecting the OMAP, PVC and PV
		// objects after failback operation.
		localStatus, rErr := rbdVol.GetLocalState()
		if rErr != nil {
			return nil, status.Error(codes.Internal, rErr.Error())
		}
		if localStatus.Up && localStatus.State == librbd.MirrorImageStatusStateReplaying {
			if err = undoVolReservation(ctx, rbdVol, cr); err != nil {
				log.ErrorLog(ctx, "failed to remove reservation for volume (%s) with backing image (%s) (%s)",
					rbdVol.RequestName, rbdVol.RbdImageName, err)

				return nil, status.Error(codes.Internal, err.Error())
			}

			return &csi.DeleteVolumeResponse{}, nil
		}
		log.ErrorLog(ctx,
			"secondary image status is up=%t and state=%s",
			localStatus.Up,
			localStatus.State)
	}

	inUse, err := rbdVol.isInUse()
	if err != nil {
		log.ErrorLog(ctx, "failed getting information for image (%s): (%s)", rbdVol, err)

		return nil, status.Error(codes.Internal, err.Error())
	}
	if inUse {
		log.ErrorLog(ctx, "rbd %s is still being used", rbdVol)

		return nil, status.Errorf(codes.Internal, "rbd %s is still being used", rbdVol.RbdImageName)
	}

	// delete the temporary rbd image created as part of volume clone during
	// create volume
	tempClone := rbdVol.generateTempClone()
	err = tempClone.deleteImage(ctx)
	if err != nil {
		if errors.Is(err, ErrImageNotFound) {
			err = tempClone.ensureImageCleanup(ctx)
			if err != nil {
				return nil, status.Error(codes.Internal, err.Error())
			}
		} else {
			// return error if it is not ErrImageNotFound
			log.ErrorLog(ctx, "failed to delete rbd image: %s with error: %v",
				tempClone, err)

			return nil, status.Error(codes.Internal, err.Error())
		}
	}

	// Deleting rbd image
	log.DebugLog(ctx, "deleting image %s", rbdVol.RbdImageName)
	if err = rbdVol.deleteImage(ctx); err != nil {
		log.ErrorLog(ctx, "failed to delete rbd image: %s with error: %v",
			rbdVol, err)

		return nil, status.Error(codes.Internal, err.Error())
	}

	if err = undoVolReservation(ctx, rbdVol, cr); err != nil {
		log.ErrorLog(ctx, "failed to remove reservation for volume (%s) with backing image (%s) (%s)",
			rbdVol.RequestName, rbdVol.RbdImageName, err)

		return nil, status.Error(codes.Internal, err.Error())
	}

	return &csi.DeleteVolumeResponse{}, nil
}

func DoSnapshotClone(
	ctx context.Context,
	parentVol *RbdVolume,
	rbdSnap *RbdSnapshot,
	cr *util.Credentials,
) (*RbdVolume, error) {
	// generate cloned volume details from snapshot
	cloneRbd := GenerateVolFromSnap(rbdSnap)
	defer cloneRbd.Destroy()
	// add image feature for cloneRbd
	f := []string{librbd.FeatureNameLayering, librbd.FeatureNameDeepFlatten}
	cloneRbd.ImageFeatureSet = librbd.FeatureSetFromNames(f)

	err := cloneRbd.Connect(cr)
	if err != nil {
		return cloneRbd, err
	}

	err = createRBDClone(ctx, parentVol, cloneRbd, rbdSnap)
	if err != nil {
		log.ErrorLog(ctx, "failed to create snapshot: %v", err)

		return cloneRbd, err
	}

	defer func() {
		if err != nil {
			if !errors.Is(err, ErrFlattenInProgress) {
				// cleanup clone and snapshot
				errCleanUp := CleanUpSnapshot(ctx, cloneRbd, rbdSnap, cloneRbd)
				if errCleanUp != nil {
					log.ErrorLog(ctx, "failed to cleanup snapshot and clone: %v", errCleanUp)
				}
			}
		}
	}()

	err = parentVol.copyEncryptionConfig(&cloneRbd.rbdImage, false)
	if err != nil {
		log.ErrorLog(ctx, "failed to copy encryption "+
			"config for %q: %v", cloneRbd, err)

		return nil, err
	}

	err = cloneRbd.CreateSnapshot(ctx, rbdSnap)
	if err != nil {
		// update rbd image name for logging
		rbdSnap.RbdImageName = cloneRbd.RbdImageName
		log.ErrorLog(ctx, "failed to create snapshot %s: %v", rbdSnap, err)

		return cloneRbd, err
	}

	err = cloneRbd.getImageID()
	if err != nil {
		log.ErrorLog(ctx, "failed to get image id: %v", err)

		return cloneRbd, err
	}
	// save image ID
	j, err := snapJournal.Connect(rbdSnap.Monitors, rbdSnap.RadosNamespace, cr)
	if err != nil {
		log.ErrorLog(ctx, "failed to connect to cluster: %v", err)

		return cloneRbd, err
	}
	defer j.Destroy()

	err = j.StoreImageID(ctx, rbdSnap.JournalPool, rbdSnap.ReservedID, cloneRbd.ImageID)
	if err != nil {
		log.ErrorLog(ctx, "failed to reserve volume id: %v", err)

		return cloneRbd, err
	}

	err = cloneRbd.flattenRbdImage(ctx, false, 4, 8)
	if err != nil {
		return cloneRbd, err
	}

	return cloneRbd, nil
}

// reserveSnap is a helper routine to request a rbdSnapshot name reservation and generate the
// volume ID for the generated name.
func ReserveSnap(ctx context.Context, rbdSnap *RbdSnapshot, rbdVol *RbdVolume, cr *util.Credentials) error {
	var err error

	journalPoolID, imagePoolID, err := util.GetPoolIDs(ctx, rbdSnap.Monitors, rbdSnap.JournalPool, rbdSnap.Pool, cr)
	if err != nil {
		return err
	}

	j, err := snapJournal.Connect(rbdSnap.Monitors, rbdSnap.RadosNamespace, cr)
	if err != nil {
		return err
	}
	defer j.Destroy()

	kmsID, encryptionType := getEncryptionConfig(rbdVol)

	rbdSnap.ReservedID, rbdSnap.RbdSnapName, err = j.ReserveName(
		ctx, rbdSnap.JournalPool, journalPoolID, rbdSnap.Pool, imagePoolID,
		rbdSnap.RequestName, rbdSnap.NamePrefix, rbdVol.RbdImageName, kmsID, rbdSnap.ReservedID, rbdVol.Owner,
		"", encryptionType)
	if err != nil {
		return err
	}

	rbdSnap.VolID, err = util.GenerateVolID(ctx, rbdSnap.Monitors, cr, imagePoolID, rbdSnap.Pool,
		rbdSnap.ClusterID, rbdSnap.ReservedID, volIDVersion)
	if err != nil {
		return err
	}

	log.DebugLog(ctx, "generated Volume ID (%s) and image name (%s) for request name (%s)",
		rbdSnap.VolID, rbdSnap.RbdSnapName, rbdSnap.RequestName)

	return nil
}

var (
	// rbdHardMaxCloneDepth is the hard limit for maximum number of nested volume clones that are taken before flatten
	// occurs.
	rbdHardMaxCloneDepth uint

	// rbdSoftMaxCloneDepth is the soft limit for maximum number of nested volume clones that are taken before flatten
	// occurs.
	rbdSoftMaxCloneDepth uint
)

//func createRBDClone(
//	ctx context.Context,
//	parentVol, cloneRbdVol *rbdVolume,
//	snap *rbdSnapshot,
//) error {
//	// create snapshot
//	err := parentVol.CreateSnapshot(ctx, snap)
//	if err != nil {
//		fmt.Println(ctx, "failed to create snapshot %s: %v", snap, err)
//
//		return err
//	}
//
//	snap.RbdImageName = parentVol.RbdImageName
//	// create clone image and delete snapshot
//	err = cloneRbdVol.cloneRbdImageFromSnapshot(ctx, snap, parentVol)
//	if err != nil {
//		fmt.Println(
//			ctx,
//			"failed to clone rbd image %s from snapshot %s: %v",
//			cloneRbdVol.RbdImageName,
//			snap.RbdSnapName,
//			err)
//		err = fmt.Errorf(
//			"failed to clone rbd image %s from snapshot %s: %w",
//			cloneRbdVol.RbdImageName,
//			snap.RbdSnapName,
//			err)
//	}
//	errSnap := parentVol.deleteSnapshot(ctx, snap)
//	if errSnap != nil {
//		fmt.Println(ctx, "failed to delete snapshot: %v", errSnap)
//		delErr := cloneRbdVol.deleteImage(ctx)
//		if delErr != nil {
//			fmt.Println(ctx, "failed to delete rbd image: %s with error: %v", cloneRbdVol, delErr)
//		}
//
//		return err
//	}
//
//	return nil
//}
