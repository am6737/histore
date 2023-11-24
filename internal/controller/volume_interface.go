package controller

import (
	"context"
	"github.com/golang/protobuf/ptypes/timestamp"
)

type VolumeController interface {
	CreateVolume(context.Context, *CreateVolumeRequest) (*CreateVolumeResponse, error)
	DeleteVolume(context.Context, *DeleteVolumeRequest) (*DeleteVolumeResponse, error)
	CreateSnapshot(context.Context, *CreateSnapshotRequest) (*CreateSnapshotResponse, error)
	DeleteSnapshot(context.Context, *DeleteSnapshotRequest) (*DeleteSnapshotResponse, error)
}

type CreateVolumeRequest struct {
	Name         string            `json:"name,omitempty"`
	VolumeId     string            `json:"volume_id,omitempty"`
	Parameters   map[string]string `json:"parameters,omitempty"`
	MaterSecrets map[string]string `json:"materSecrets,omitempty"`
	SlaveSecrets map[string]string `json:"slaveSecrets,omitempty"`
}

type CreateVolumeResponse struct {
	VolumeId string `json:"volume_id,omitempty"`
}

type CreateSnapshotRequest struct {
	SourceVolumeId string            `json:"source_volume_id,omitempty"`
	Name           string            `json:"name,omitempty"`
	Secrets        map[string]string `json:"secrets,omitempty"`
	Parameters     map[string]string `json:"parameters,omitempty"`
}

type CreateSnapshotResponse struct {
	SnapshotId     string               `json:"snapshot_id,omitempty"`
	SourceVolumeId string               ` json:"source_volume_id,omitempty"`
	CreationTime   *timestamp.Timestamp `json:"creation_time,omitempty"`
	ReadyToUse     bool                 ` json:"ready_to_use,omitempty"`
}

type DeleteVolumeRequest struct {
	VolumeId string            `json:"volume_id,omitempty"`
	Secrets  map[string]string `json:"secrets,omitempty" `
}

type DeleteVolumeResponse struct {
}

type DeleteSnapshotRequest struct {
	SnapshotId string            `json:"snapshot_id,omitempty"`
	Secrets    map[string]string ` json:"secrets,omitempty" `
}

type DeleteSnapshotResponse struct {
}
