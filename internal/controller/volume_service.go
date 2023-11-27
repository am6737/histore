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
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var ErrImageNotFound = errors.New("image not found")
var ErrFlattenInProgress = errors.New("flatten in progress")
var ErrFlattenTimeout = errors.New("flatten in timeout")
var ErrSnapNotFound = errors.New("snapshot not found")

type VolumeService struct {
	Client client.Client
	Log    logr.Logger
}

func (v *VolumeService) RestoreSnapshot(ctx context.Context, req *RestoreSnapshotRequest) (*RestoreSnapshotResponse, error) {

	if err := v.validateRestoreReq(ctx, req); err != nil {
		return nil, err
	}

	cr, err := util.NewUserCredentials(req.Secrets)
	if err != nil {
		return nil, err
	}
	defer cr.DeleteCredentials()

	rbdVol, err := rbd.GenVolFromVolID(ctx, req.SourceVolumeId, cr, req.Secrets)
	if err != nil {
		return nil, err
	}
	defer rbdVol.Destroy()

	rbdSnap, err := rbd.GenSnapFromOptions(ctx, rbdVol, req.Parameters)
	if err != nil {
		return nil, err
	}

	vid := GetVolUUId(req.VolumeId)
	rbdSnap.RbdImageName = rbdVol.RbdImageName
	rbdSnap.RbdSnapName = "csi-vol-" + vid

	_, err = rbd.CreateVolumeFromSnapshot(ctx, rbdVol, rbdSnap, cr)
	if err != nil {
		return nil, err
	}

	return nil, nil
}

func (v *VolumeService) CreateVolume(ctx context.Context, req *CreateVolumeRequest) (*CreateVolumeResponse, error) {

	if err := v.validateVolumeReq(ctx, req); err != nil {
		return nil, err
	}

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

	var masterRbd = &rbd.RbdVolume{}

	masterVolumeHandle := req.VolumeId

	masterRbd, err = rbd.GenVolFromVolID(ctx, masterVolumeHandle, masterCr, masterSecret)
	if err != nil {
		if errors.Is(err, librbd.ErrNotFound) {
			v.Log.Info(fmt.Sprintf("master source Volume ID %s not found", masterVolumeHandle))
			return nil, err
		}
		return nil, err
	}
	defer masterRbd.Destroy()

	rbdSnap, err := rbd.GenSnapFromOptions(ctx, masterRbd, req.Parameters)
	if err != nil {
		v.Log.Error(err, "GenSnapFromOptions")
		return nil, err
	}
	reservedID := uuid.New().String()
	rbdSnapName := "csi-vol-" + reservedID
	rbdSnap.RbdImageName = masterRbd.RbdImageName
	rbdSnap.VolSize = masterRbd.VolSize
	rbdSnap.SourceVolumeID = masterVolumeHandle
	rbdSnap.RequestName = req.Name

	if err = rbd.FlattenTemporaryClonedImages(ctx, masterRbd, masterCr); err != nil {
		v.Log.Error(err, "FlattenTemporaryClonedImages")
		return nil, err
	}

	//if err = rbd.ReserveSnap(ctx, rbdSnap, masterRbd, masterCr); err != nil {
	//	v.Log.Error(err, "ReserveSnap")
	//	return nil, err
	//}
	rbdSnap.ReservedID = reservedID
	rbdSnap.RbdSnapName = rbdSnapName

	volumeHandle := GetCloneVolumeHandleFromVolumeHandle(masterVolumeHandle, reservedID)

	_, err = rbd.CreateRBDVolumeFromSnapshotForce(ctx, masterRbd, rbdSnap, masterCr)
	if err != nil {
		v.Log.Error(err, "CreateRBDVolumeFromSnapshotForce")
		return nil, err
	}

	return &CreateVolumeResponse{
		VolumeId: volumeHandle,
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

	//rbdSnap := &rbd.RbdSnapshot{}
	//if err = rbd.GenSnapFromSnapID(ctx, rbdSnap, volumeID, cr, secrets); err != nil {
	//	return nil, err
	//}

	// Deleting snapshot volume
	//log.DebugLog(ctx, "deleting rbd volume %s", rbdSnap.RbdSnapName)
	//
	//rbdVol := rbd.GenerateVolFromSnap(rbdSnap)

	rbdVol, err := rbd.GenVolFromVolID(ctx, req.VolumeId, cr, secrets)
	if err != nil {
		if errors.Is(err, librbd.ErrNotFound) {
			v.Log.Info(fmt.Sprintf("master source Volume ID %s not found", req.VolumeId))
			return nil, err
		}
		return nil, err
	}
	defer rbdVol.Destroy()

	_, err = rbd.CleanupRBDImage(ctx, rbdVol, cr)
	if err != nil {
		return nil, err
	}

	return &DeleteVolumeResponse{}, nil
}

func (v *VolumeService) CreateSnapshot(ctx context.Context, req *CreateSnapshotRequest) (*CreateSnapshotResponse, error) {

	if err := v.validateSnapshotReq(ctx, req); err != nil {
		return nil, err
	}

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
	//rbdSnap.RbdImageName = rbdVol.RbdImageName
	//rbdSnap.VolSize = rbdVol.VolSize
	//rbdSnap.SourceVolumeID = req.SourceVolumeId
	//rbdSnap.RequestName = req.Name
	rbdSnap.RbdSnapName = "csi-snap-" + req.Name

	if err = rbdVol.CreateSnapshot(ctx, rbdSnap); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	sid := GetCloneVolumeHandleFromVolumeHandle(req.SourceVolumeId, req.Name)

	return &CreateSnapshotResponse{
		SnapshotId:     sid,
		SourceVolumeId: req.SourceVolumeId,
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

func (*VolumeService) validateVolumeReq(ctx context.Context, req *CreateVolumeRequest) error {
	// Check sanity of request Name, Volume Capabilities
	if req.Name == "" {
		return status.Error(codes.InvalidArgument, "volume Name cannot be empty")
	}
	if req.VolumeId == "" {
		return status.Error(codes.InvalidArgument, "source Volume ID cannot be empty")
	}
	options := req.Parameters
	if value, ok := options["clusterID"]; !ok || value == "" {
		return status.Error(codes.InvalidArgument, "missing or empty cluster ID to provision volume from")
	}
	if value, ok := options["pool"]; !ok || value == "" {
		return status.Error(codes.InvalidArgument, "missing or empty pool name to provision volume from")
	}

	if value, ok := options["dataPool"]; ok && value == "" {
		return status.Error(codes.InvalidArgument, "empty datapool name to provision volume from")
	}
	if value, ok := options["radosNamespace"]; ok && value == "" {
		return status.Error(codes.InvalidArgument, "empty namespace name to provision volume from")
	}
	if value, ok := options["volumeNamePrefix"]; ok && value == "" {
		return status.Error(codes.InvalidArgument, "empty volume name prefix to provision volume from")
	}

	return nil
}

func (v *VolumeService) validateSnapshotReq(ctx context.Context, req *CreateSnapshotRequest) error {
	// Check sanity of request Snapshot Name, Source Volume Id
	if req.Name == "" {
		return status.Error(codes.InvalidArgument, "snapshot Name cannot be empty")
	}
	if req.SourceVolumeId == "" {
		return status.Error(codes.InvalidArgument, "source Volume ID cannot be empty")
	}

	options := req.Parameters
	if value, ok := options["snapshotNamePrefix"]; ok && value == "" {
		return status.Error(codes.InvalidArgument, "empty snapshot name prefix to provision snapshot from")
	}
	if value, ok := options["pool"]; ok && value == "" {
		return status.Error(codes.InvalidArgument, "empty pool name in which rbd image will be created")
	}

	return nil
}

func (v *VolumeService) validateRestoreReq(ctx context.Context, req *RestoreSnapshotRequest) error {
	// Check sanity of request Snapshot Name, Source Volume Id
	if req.VolumeId == "" {
		return status.Error(codes.InvalidArgument, "Volume ID cannot be empty")
	}
	if req.SourceVolumeId == "" {
		return status.Error(codes.InvalidArgument, "source Volume ID cannot be empty")
	}

	options := req.Parameters
	if value, ok := options["snapshotNamePrefix"]; ok && value == "" {
		return status.Error(codes.InvalidArgument, "empty snapshot name prefix to provision snapshot from")
	}
	if value, ok := options["pool"]; ok && value == "" {
		return status.Error(codes.InvalidArgument, "empty pool name in which rbd image will be created")
	}

	return nil
}
