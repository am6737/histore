package controller

import (
	"context"
	"errors"
	"fmt"
	hitoseacomv1 "github.com/am6737/histore/api/v1"
	"github.com/am6737/histore/pkg/ceph/rbd"
	"github.com/am6737/histore/pkg/ceph/util"
	"github.com/am6737/histore/pkg/config"
	"github.com/am6737/histore/pkg/util/log"
	librbd "github.com/ceph/go-ceph/rbd"
	"github.com/go-logr/logr"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"strings"
	"time"
)

var ErrImageNotFound = errors.New("image not found")
var ErrFlattenInProgress = errors.New("flatten in progress")
var ErrFlattenTimeout = errors.New("flatten in timeout")
var ErrSnapNotFound = errors.New("snapshot not found")

type VolumeService struct {
	Client client.Client
	Log    logr.Logger
}

func (v *VolumeService) SyncMasterImage(ctx context.Context, rbdVol *rbd.RbdVolume) (bool, error) {
	masterRbdImageStatus, err := rbdVol.GetImageMirroringStatus()
	if err != nil {
		v.Log.Error(err, "masterRbd.GetImageMirroringStatus err")
		return false, nil
	}

	for _, mrs := range masterRbdImageStatus.SiteStatuses {
		if mrs.MirrorUUID == "" {
			rs1, err := mrs.DescriptionReplayStatus()
			if err != nil {
				if strings.Contains(err.Error(), "No such file or directory") {
					return false, nil
				}
				return false, err
			}
			if rs1.ReplayState == "idle" {
				return true, nil
			}
		}
	}

	return false, nil
}

func (v *VolumeService) SyncSlaveImage(ctx context.Context, rbdVol *rbd.RbdVolume) (bool, error) {
	sRbdStatus, err := rbdVol.GetImageMirroringStatus()
	if err != nil {
		v.Log.Error(err, "GetImageMirroringStatus err")
		return false, nil
	}

	for _, srs := range sRbdStatus.SiteStatuses {
		if srs.MirrorUUID == "" {
			if strings.Contains(srs.Description, "remote image is not primary") || strings.Contains(srs.Description, "local image is primary") {
				return true, nil
			}
			replayStatus, err := srs.DescriptionReplayStatus()
			if err != nil {
				if strings.Contains(err.Error(), "No such file or directory") {
					return false, nil
				}
				v.Log.Error(err, "SyncSlaveImage replayStatus err")
				return false, nil
			}
			if replayStatus.ReplayState == "idle" {
				return true, nil
			}
		}
	}

	return false, nil
}

func (v *VolumeService) Create(ctx context.Context, name string, volumeId string, parameters map[string]string, materSecrets map[string]string, slaveSecrets map[string]string) (string, error) {
	vol, err := v.CreateVolume(ctx, &CreateVolumeRequest{
		Name:         name,
		VolumeId:     volumeId,
		Parameters:   parameters,
		MaterSecrets: materSecrets,
		SlaveSecrets: slaveSecrets,
	})
	if err != nil {
		return "", err
	}

	vid := vol.VolumeId

	v.Log.Info(fmt.Sprintf("waiting for volume flatten"), "volumeId", vid)

	return vid, nil
}

func (v *VolumeService) Flatten(ctx context.Context, volumeHandle string, secret map[string]string, maxWait time.Duration) (bool, error) {
	cr, err := util.NewUserCredentials(secret)
	if err != nil {
		v.Log.Error(err, "NewUserCredentials")
		return false, err
	}
	defer cr.DeleteCredentials()

	rbdVol, err := rbd.GenVolFromVolID(ctx, volumeHandle, cr, secret)
	if err != nil {
		return false, err
	}
	defer rbdVol.Destroy()

	success, err := rbdVol.IsFlattenCompleted(maxWait)
	if err != nil {
		return false, err
	}

	if !success {
		return false, ErrFlattenTimeout
	}

	return true, nil
}

func (v *VolumeService) Enable(ctx context.Context, rbdVol *rbd.RbdVolume) (bool, error) {
	state, err := rbdVol.GetLocalState()
	if err != nil {
		return false, err
	}
	if state.Up {
		return true, nil
	}
	if err := rbdVol.EnableImageMirroring(librbd.ImageMirrorModeSnapshot); err != nil {
		if strings.Contains(err.Error(), "Device or resource busy") {
			return false, nil
		}
		v.Log.Error(err, "Master image enabled failed")
		return false, err
	}
	v.Log.Info("Master image enabled successfully", "key", fmt.Sprintf("%s/%s", rbdVol.Pool, rbdVol.RbdImageName))

	return true, nil
}

func (v *VolumeService) Demote(ctx context.Context, rbdVol *rbd.RbdVolume) (bool, error) {
	state, err := rbdVol.GetImageMirroringInfo()
	if err != nil {
		return false, err
	}
	if !state.Primary {
		return true, nil
	}

	v.Log.Info("wait mater image demote", "key", fmt.Sprintf("%s/%s", rbdVol.Pool, rbdVol.RbdImageName))
	if err = rbdVol.DemoteImage(); err != nil {
		if strings.Contains(err.Error(), "Device or resource busy") {
			return false, nil
		}
		v.Log.Error(err, "demote master image failed")
		return false, err
	}
	v.Log.Info("master image demote successfully", "key", fmt.Sprintf("%s/%s", rbdVol.Pool, rbdVol.RbdImageName))

	return true, nil
}

func (v *VolumeService) Promote(ctx context.Context, rbdVol *rbd.RbdVolume) (bool, error) {
	state, err := rbdVol.GetImageMirroringInfo()
	if err != nil {
		return false, err
	}
	if state.Primary {
		return true, nil
	}

	v.Log.Info("wait slave image promote", "key", fmt.Sprintf("%s/%s", rbdVol.Pool, rbdVol.RbdImageName))
	if err = rbdVol.PromoteImage(false); err != nil {
		if strings.Contains(err.Error(), "Device or resource busy") {
			return false, nil
		}
		v.Log.Error(err, "Promote slave image failed")
		return false, err
	}
	v.Log.Info("slave image promote successfully", "key", fmt.Sprintf("%s/%s", rbdVol.Pool, rbdVol.RbdImageName))

	return true, nil
}

func (v *VolumeService) Disable(ctx context.Context, rbdVol *rbd.RbdVolume) (bool, error) {
	state, err := rbdVol.GetLocalState()
	if err != nil {
		return false, err
	}
	if !state.Up {
		return true, nil
	}

	if err = rbdVol.DisableImageMirroring(false); err != nil {
		if strings.Contains(err.Error(), "Device or resource busy") {
			return false, nil
		}
		v.Log.Error(err, "disable slave image image failed")
	}
	v.Log.Info("slave image disable successfully", "key", fmt.Sprintf("%s/%s", rbdVol.Pool, rbdVol.RbdImageName))

	return true, nil
}

func (v *VolumeService) Snapshot(ctx context.Context, volumeHandle string, secret map[string]string) (string, error) {
	ssc, err := config.GetCephCsiConfigForSC(v.Client, config.DC.SlaveStorageClass)
	if err != nil {
		v.Log.Error(err, "getCephCsiConfigForSC")
		return "", err
	}

	snap, err := v.CreateSnapshot(ctx, &CreateSnapshotRequest{
		SourceVolumeId: volumeHandle,
		Name:           uuid.New().String(),
		Secrets:        secret,
		Parameters:     map[string]string{"clusterID": ssc.ClusterID, "pool": ssc.Pool},
	})
	if err != nil {
		return "", err
	}

	return snap.SnapshotId, nil
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

func (v *VolumeService) updateVolumeStatus(ctx context.Context, content *hitoseacomv1.VirtualMachineSnapshotContent, newVolumeStatus hitoseacomv1.VolumeStatus) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var newContent hitoseacomv1.VirtualMachineSnapshotContent
		if err := v.Client.Get(ctx, types.NamespacedName{
			Namespace: content.Namespace,
			Name:      content.Name,
		}, &newContent); err != nil {
			return err
		}

		var targetIndex int
		found := false
		for i, v := range content.Status.VolumeStatus {
			if v.VolumeName == newVolumeStatus.VolumeName {
				targetIndex = i
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("volume %s not found", newVolumeStatus.VolumeName)
		}
		copyNonEmptyFields(&newContent.Status.VolumeStatus[targetIndex], &newVolumeStatus)
		if err := v.Client.Status().Update(ctx, &newContent); err != nil {
			return fmt.Errorf("failed to update resource Status: %w", err)
		}

		return nil
	})
}
