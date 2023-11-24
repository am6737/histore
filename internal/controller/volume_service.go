package controller

import (
	"context"
	"errors"
	"fmt"
	"github.com/am6737/histore/pkg/ceph/rbd"
	"github.com/am6737/histore/pkg/ceph/util"
	"github.com/am6737/histore/pkg/util/log"
	librbd "github.com/ceph/go-ceph/rbd"
	"github.com/go-logr/logr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var ErrImageNotFound = errors.New("image not found")
var ErrFlattenInProgress = errors.New("flatten in progress")

type VolumeService struct {
	Client client.Client
	Log    logr.Logger
}

func (v *VolumeService) CreateVolume(ctx context.Context, req *CreateVolumeRequest) (*CreateVolumeResponse, error) {

	masterSecret := req.MaterSecrets
	SlaveSecrets := req.SlaveSecrets

	masterCr, err := util.NewUserCredentials(masterSecret)
	if err != nil {
		v.Log.Error(err, "NewUserCredentials")
		return nil, err
	}
	defer masterCr.DeleteCredentials()

	slaveCr, err := util.NewUserCredentials(SlaveSecrets)
	if err != nil {
		v.Log.Error(err, "NewUserCredentials")
		return nil, err
	}
	defer slaveCr.DeleteCredentials()

	var (
		masterRbd    = &rbd.RbdVolume{}
		cloneRbd     = &rbd.RbdVolume{}
		cloneRbdName string
	)
	masterVolumeHandle := req.VolumeId

	masterRbd, err = rbd.GenVolFromVolID(ctx, masterVolumeHandle, masterCr, masterSecret)
	if err != nil {
		if errors.Is(err, librbd.ErrNotFound) {
			//log.DebugLog(ctx, "image %s encrypted state not set", ri)
			v.Log.Info(fmt.Sprintf("master source Volume ID %s not found", masterVolumeHandle))
			return nil, err
		}
		return nil, err
	}
	defer masterRbd.Destroy()

	clusterID, err := GetClusterIDFromVolumeHandle(masterVolumeHandle)
	if err != nil {
		v.Log.Error(err, "GetClusterIDFromVolumeHandle")
		return nil, err
	}

	rbdSnap, err := rbd.GenSnapFromOptions(ctx, masterRbd, map[string]string{"clusterID": clusterID})
	if err != nil {
		v.Log.Error(err, "GenSnapFromOptions")
		return nil, err
	}

	rbdSnap.RbdImageName = masterRbd.RbdImageName
	rbdSnap.VolSize = masterRbd.VolSize
	rbdSnap.SourceVolumeID = masterVolumeHandle
	rbdSnap.RequestName = req.Name

	v.Log.Info(fmt.Sprintf("Attempting to create RBD %s", rbdSnap.RbdSnapName))

	_, err = rbd.CreateRBDVolumeFromSnapshot(ctx, masterRbd, rbdSnap, masterCr)
	if err != nil {
		v.Log.Error(err, "CreateRBDVolumeFromSnapshot")
		return nil, err
	}

	cloneRbd, err = rbd.GenVolFromVolID(ctx, GetCloneVolumeHandleFromVolumeHandle(masterVolumeHandle, cloneRbdName), masterCr, masterSecret)
	if err != nil {
		v.Log.Error(err, "GenVolFromVolID")
		return nil, err
	}
	defer cloneRbd.Destroy()

	return &CreateVolumeResponse{
		VolumeId: cloneRbd.VolID,
	}, nil
}

func (v *VolumeService) DeleteVolume(ctx context.Context, req *DeleteVolumeRequest) (*DeleteVolumeResponse, error) {
	// For now the image get unconditionally deleted, but here retention policy can be checked
	volumeID := req.VolumeId
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "empty volume ID in request")
	}

	secrets := req.Secrets

	cr, err := util.NewUserCredentialsWithMigration(secrets)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	defer cr.DeleteCredentials()

	rbdSnap := &rbd.RbdSnapshot{}
	if err = rbd.GenSnapFromSnapID(ctx, rbdSnap, volumeID, cr, secrets); err != nil {
		panic(err)
	}

	// Deleting snapshot volume
	log.DebugLog(ctx, "deleting rbd volume %s", rbdSnap.RbdSnapName)

	rbdVol := rbd.GenerateVolFromSnap(rbdSnap)

	//rbdVol, err := rbd.GenVolFromVolID(ctx, volumeID, cr, secrets)
	//if err != nil {
	//	return nil, err
	//}
	//defer rbdVol.Destroy()

	_, err = rbd.CleanupRBDImage(ctx, rbdVol, cr)
	if err != nil {
		return nil, err
	}

	return &DeleteVolumeResponse{}, nil
}

func (v *VolumeService) CreateSnapshot(ctx context.Context, req *CreateSnapshotRequest) (*CreateSnapshotResponse, error) {
	//if err := v.validateSnapshotReq(ctx, req); err != nil {
	//	return nil, err
	//}

	secrets := req.Secrets

	cr, err := util.NewUserCredentials(secrets)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	defer cr.DeleteCredentials()

	// Fetch source volume information
	rbdVol, err := rbd.GenVolFromVolID(ctx, req.SourceVolumeId, cr, secrets)
	defer rbdVol.Destroy()
	if err != nil {
		switch {
		case errors.Is(err, ErrImageNotFound):
			err = status.Errorf(codes.NotFound, "source Volume ID %s not found", req.SourceVolumeId)
		case errors.Is(err, util.ErrPoolNotFound):
			log.ErrorLog(ctx, "failed to get backend volume for %s: %v", req.SourceVolumeId, err)
			err = status.Errorf(codes.NotFound, err.Error())
		default:
			err = status.Errorf(codes.Internal, err.Error())
		}
		return nil, err
	}

	// Check if source volume was created with required image features for snaps
	if !rbdVol.HasSnapshotFeature() {
		return nil, status.Errorf(
			codes.InvalidArgument,
			"volume(%s) has not snapshot feature(layering)",
			req.SourceVolumeId)
	}

	rbdSnap, err := rbd.GenSnapFromOptions(ctx, rbdVol, req.Parameters)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	rbdSnap.RbdImageName = rbdVol.RbdImageName
	rbdSnap.VolSize = rbdVol.VolSize
	rbdSnap.SourceVolumeID = req.SourceVolumeId
	rbdSnap.RequestName = req.Name

	// Need to check for already existing snapshot name, and if found
	// check for the requested source volume id and already allocated source volume id
	found, err := rbd.CheckSnapCloneExists(ctx, rbdVol, rbdSnap, cr)
	if err != nil {
		if errors.Is(err, util.ErrSnapNameConflict) {
			return nil, status.Error(codes.AlreadyExists, err.Error())
		}

		return nil, status.Errorf(codes.Internal, err.Error())
	}
	if found {
		_, err = rbd.CloneFromSnapshot(ctx, rbdVol, rbdSnap, cr, req.Parameters)
		if err != nil {
			return nil, err
		}
		return nil, err
	}

	if err = rbd.FlattenTemporaryClonedImages(ctx, rbdVol, cr); err != nil {
		return nil, err
	}

	err = rbd.ReserveSnap(ctx, rbdSnap, rbdVol, cr)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	defer func() {
		if err != nil && !errors.Is(err, ErrFlattenInProgress) {
			errDefer := rbd.UndoSnapReservation(ctx, rbdSnap, cr)
			if errDefer != nil {
				log.WarningLog(ctx, "failed undoing reservation of snapshot: %s %v", req.Name, errDefer)
			}
		}
	}()

	vol, err := rbd.DoSnapshotClone(ctx, rbdVol, rbdSnap, cr)
	if err != nil {
		return nil, err

	}
	defer func() {
		if err != nil {
			log.DebugLog(ctx, "Removing clone image %q", vol)
			_, err = rbd.CleanupRBDImage(ctx, vol, cr)
			if err != nil {
				log.ErrorLog(ctx, "failed to delete clone image %q: %v", vol, err)
			}
		}
	}()
	vol.RbdImageName = rbdSnap.RbdSnapName

	return &CreateSnapshotResponse{
		SnapshotId:     rbdSnap.RbdSnapName,
		SourceVolumeId: rbdSnap.SourceVolumeID,
		CreationTime:   rbdSnap.CreatedAt,
		ReadyToUse:     true,
	}, nil
}

func (v *VolumeService) DeleteSnapshot(ctx context.Context, req *DeleteSnapshotRequest) (*DeleteSnapshotResponse, error) {
	//if err := cs.Driver.ValidateControllerServiceRequest(
	//	csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT); err != nil {
	//	log.ErrorLog(ctx, "invalid delete snapshot req: %v", protosanitizer.StripSecrets(req))
	//
	//	return nil, err
	//}

	cr, err := util.NewUserCredentials(req.Secrets)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	defer cr.DeleteCredentials()

	snapshotID := req.SnapshotId
	if snapshotID == "" {
		return nil, status.Error(codes.InvalidArgument, "snapshot ID cannot be empty")
	}

	rbdSnap := &rbd.RbdSnapshot{}
	if err = rbd.GenSnapFromSnapID(ctx, rbdSnap, snapshotID, cr, req.Secrets); err != nil {
		// if error is ErrPoolNotFound, the pool is already deleted we don't
		// need to worry about deleting snapshot or omap data, return success
		if errors.Is(err, util.ErrPoolNotFound) {
			log.WarningLog(ctx, "failed to get backend snapshot for %s: %v", snapshotID, err)

			return &DeleteSnapshotResponse{}, nil
		}

		// if error is ErrKeyNotFound, then a previous attempt at deletion was complete
		// or partially complete (snap and snapOMap are garbage collected already), hence return
		// success as deletion is complete
		if errors.Is(err, util.ErrKeyNotFound) {
			return &DeleteSnapshotResponse{}, nil
		}

		// if the error is ErrImageNotFound, We need to cleanup the image from
		// trash and remove the metadata in OMAP.
		if errors.Is(err, ErrImageNotFound) {
			err = rbd.CleanUpImageAndSnapReservation(ctx, rbdSnap, cr)
			if err != nil {
				return nil, status.Error(codes.Internal, err.Error())
			}

			return &DeleteSnapshotResponse{}, nil
		}

		return nil, status.Error(codes.Internal, err.Error())
	}

	// Deleting snapshot and cloned volume
	log.DebugLog(ctx, "deleting cloned rbd volume %s", rbdSnap.RbdSnapName)

	rbdVol := rbd.GenerateVolFromSnap(rbdSnap)

	err = rbdVol.Connect(cr)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	defer rbdVol.Destroy()

	rbdVol.ImageID = rbdSnap.ImageID
	// update parent name to delete the snapshot
	rbdSnap.RbdImageName = rbdVol.RbdImageName
	err = rbd.CleanUpSnapshot(ctx, rbdVol, rbdSnap, rbdVol)
	if err != nil {
		log.ErrorLog(ctx, "failed to delete image: %v", err)

		return nil, status.Error(codes.Internal, err.Error())
	}
	err = rbd.UndoSnapReservation(ctx, rbdSnap, cr)
	if err != nil {
		log.ErrorLog(ctx, "failed to remove reservation for snapname (%s) with backing snap (%s) on image (%s) (%s)",
			rbdSnap.RequestName, rbdSnap.RbdSnapName, rbdSnap.RbdImageName, err)

		return nil, status.Error(codes.Internal, err.Error())
	}

	return &DeleteSnapshotResponse{}, nil
}
