package rbd

import (
	"context"
	"errors"
	"fmt"
	"github.com/am6737/histore/pkg/ceph/util"
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
	rbdSnap *rbdSnapshot,
	cr *util.Credentials,
	parameters map[string]string,
) (*csi.CreateSnapshotResponse, error) {
	vol := GenerateVolFromSnap(rbdSnap)
	err := vol.Connect(cr)
	if err != nil {
		fmt.Println("vol.Connect ---> ", err)
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

	var (
		// occurs.
		rbdHardMaxCloneDepth uint

		// rbdSoftMaxCloneDepth is the soft limit for maximum number of nested volume clones that are taken before flatten
		// occurs.
		rbdSoftMaxCloneDepth uint
	)

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

func DoSnapshotClone(
	ctx context.Context,
	parentVol *RbdVolume,
	rbdSnap *rbdSnapshot,
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
		fmt.Println(ctx, "failed to create snapshot: %v", err)

		return cloneRbd, err
	}

	defer func() {
		if err != nil {
			if !errors.Is(err, ErrFlattenInProgress) {
				// cleanup clone and snapshot
				errCleanUp := cleanUpSnapshot(ctx, cloneRbd, rbdSnap, cloneRbd)
				if errCleanUp != nil {
					fmt.Println(ctx, "failed to cleanup snapshot and clone: %v", errCleanUp)
				}
			}
		}
	}()

	err = parentVol.copyEncryptionConfig(&cloneRbd.rbdImage, false)
	if err != nil {
		fmt.Println(ctx, "failed to copy encryption "+
			"config for %q: %v", cloneRbd, err)

		return nil, err
	}

	err = cloneRbd.createSnapshot(ctx, rbdSnap)
	if err != nil {
		// update rbd image name for logging
		rbdSnap.RbdImageName = cloneRbd.RbdImageName
		fmt.Println(ctx, "failed to create snapshot %s: %v", rbdSnap, err)

		return cloneRbd, err
	}

	err = cloneRbd.getImageID()
	if err != nil {
		fmt.Println(ctx, "failed to get image id: %v", err)

		return cloneRbd, err
	}
	// save image ID
	j, err := snapJournal.Connect(rbdSnap.Monitors, rbdSnap.RadosNamespace, cr)
	if err != nil {
		fmt.Println(ctx, "failed to connect to cluster: %v", err)

		return cloneRbd, err
	}
	defer j.Destroy()

	err = j.StoreImageID(ctx, rbdSnap.JournalPool, rbdSnap.ReservedID, cloneRbd.ImageID)
	if err != nil {
		fmt.Println(ctx, "failed to reserve volume id: %v", err)

		return cloneRbd, err
	}

	err = cloneRbd.flattenRbdImage(ctx, false, rbdHardMaxCloneDepth, rbdSoftMaxCloneDepth)
	if err != nil {
		return cloneRbd, err
	}

	return cloneRbd, nil
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
//	err := parentVol.createSnapshot(ctx, snap)
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
