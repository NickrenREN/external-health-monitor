/*
Copyright 2020 The Kubernetes Authors.

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

package csi_handler

import (
	"context"

	"google.golang.org/grpc"

	"github.com/xing-yang/spec/lib/go/csi"
)

type CSIPVHandler struct {
	conn *grpc.ClientConn
}

func NewCSIPVHandler(conn *grpc.ClientConn) *CSIPVHandler {
	return &CSIPVHandler{
		conn: conn,
	}
}

func (handler *CSIPVHandler) ControllerVolumeChecking(ctx context.Context, volumeID string) (string, []string, error) {
	client := csi.NewControllerClient(handler.conn)

	req := csi.GetVolumeRequest{
		VolumeId: volumeID,
	}

	var condition string
	var errorMessages []string

	res, err := client.GetVolume(ctx, &req)
	if err != nil {
		return condition, errorMessages, err
	}

	/*switch  res.GetStatus().GetVolumeHealth().GetCondition() {
	case 0:
		condition = "VolumeHealth_UNKNOWN"
	case 1:
		condition = "VolumeHealth_HEALTHY"
	case 10:
		condition = "VolumeHealth_TEMPORARY_INACCESSIBLE"
	case 20:
		condition = "VolumeHealth_TEMPORARY_DEGRADED"
	case 30:
		condition = "VolumeHealth_FAILURE_LIKELY"
	case 40:
		condition = "VolumeHealth_FATAL"
	default:
		// This should never happen, return directly ?
		condition = "VolumeHealth_UnSupported"
	}*/

	// We reach here only when VOLUME_HEALTH controller capability is supported
	// so the Status and VolumeHealth in GetVolumeResponse must not be nil
	condition = csi.VolumeHealth_Condition_name[int32(res.GetStatus().GetVolumeHealth().GetCondition())]

	errorMessages = make([]string, len(res.Status.VolumeHealth.Error))
	for k, errM := range res.Status.VolumeHealth.Error {
		errorMessages[k] = "ErrorCode: " + errM.GetCode() + ", ErrorMessage: " + errM.GetMessage()
	}

	return condition, errorMessages, nil
}

func (handler *CSIPVHandler) NodeVolumeChecking(ctx context.Context, volumeID string, volumePath string, volumeStagingPath string) (string, []string, error) {
	client := csi.NewNodeClient(handler.conn)

	req := csi.NodeGetVolumeStatsRequest{
		VolumeId:          volumeID,
		VolumePath:        volumePath,
		StagingTargetPath: volumeStagingPath,
	}

	var condition string
	var errorMessages []string

	res, err := client.NodeGetVolumeStats(ctx, &req)
	if err != nil {
		return condition, errorMessages, err
	}

	if res.GetVolumeHealth() != nil {
		condition = csi.VolumeHealth_Condition_name[int32(res.GetVolumeHealth().GetCondition())]

		errorMessages = make([]string, len(res.GetVolumeHealth().GetError()))
		for k, errM := range res.GetVolumeHealth().GetError() {
			errorMessages[k] = "ErrorCode: " + errM.GetCode() + ", ErrorMessage: " + errM.GetMessage()
		}
	}
	return condition, errorMessages, nil
}
