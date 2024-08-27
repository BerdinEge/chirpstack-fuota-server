package api

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/gofrs/uuid"
	"github.com/golang/protobuf/ptypes"
	log "github.com/sirupsen/logrus"

	"github.com/brocaar/chirpstack-api/go/v3/as/external/api"
	fapi "github.com/brocaar/chirpstack-api/go/v3/fuota"
	"github.com/brocaar/chirpstack-fuota-server/internal/fuota"
	multicast "github.com/brocaar/chirpstack-fuota-server/internal/multicast"
	"github.com/brocaar/chirpstack-fuota-server/internal/storage"
	"github.com/brocaar/lorawan"
)

// FUOTAServerAPI implements the FUOTA server API.
type FUOTAServerAPI struct{}

// NewFUOTAServerAPI creates a new FUOTAServerAPI.
func NewFUOTAServerAPI() *FUOTAServerAPI {
	return &FUOTAServerAPI{}
}

// CreateDeployment creates the given FUOTA deployment.
func (a *FUOTAServerAPI) CreateDeployment(ctx context.Context, req *fapi.CreateDeploymentRequest) (*fapi.CreateDeploymentResponse, error) {
	opts := fuota.DeploymentOptions{
		ApplicationID:                     req.GetDeployment().ApplicationId,
		Devices:                           make(map[lorawan.EUI64]fuota.DeviceOptions),
		MulticastDR:                       uint8(req.GetDeployment().MulticastDr),
		MulticastFrequency:                req.GetDeployment().MulticastFrequency,
		MulticastGroupID:                  uint8(req.GetDeployment().MulticastGroupId),
		MulticastTimeout:                  uint8(req.GetDeployment().MulticastTimeout),
		FragSize:                          int(req.GetDeployment().FragmentationFragmentSize),
		Payload:                           req.GetDeployment().Payload,
		Redundancy:                        int(req.GetDeployment().FragmentationRedundancy),
		FragmentationSessionIndex:         uint8(req.GetDeployment().FragmentationSessionIndex),
		FragmentationMatrix:               uint8(req.GetDeployment().FragmentationMatrix),
		BlockAckDelay:                     uint8(req.GetDeployment().FragmentationBlockAckDelay),
		UnicastAttemptCount:               int(req.GetDeployment().UnicastAttemptCount),
		RequestFragmentationSessionStatus: fuota.FragmentationSessionStatusRequestType(req.GetDeployment().RequestFragmentationSessionStatus.String()),
	}

	for _, d := range req.GetDeployment().Devices {
		var devEUI lorawan.EUI64
		var mcRootKey lorawan.AES128Key

		copy(devEUI[:], d.DevEui)
		copy(mcRootKey[:], d.McRootKey)

		opts.Devices[devEUI] = fuota.DeviceOptions{
			McRootKey: mcRootKey,
		}
	}

	switch req.GetDeployment().MulticastGroupType {
	case fapi.MulticastGroupType_CLASS_B:
		opts.MulticastGroupType = api.MulticastGroupType_CLASS_B
	case fapi.MulticastGroupType_CLASS_C:
		opts.MulticastGroupType = api.MulticastGroupType_CLASS_C
	}

	copy(opts.Descriptor[:], req.GetDeployment().FragmentationDescriptor)

	unicastTimeout, err := ptypes.Duration(req.GetDeployment().UnicastTimeout)
	if err != nil {
		return nil, err
	}

	opts.UnicastTimeout = unicastTimeout

	depl, err := fuota.NewDeployment(opts)
	if err != nil {
		return nil, err
	}

	go func(depl *fuota.Deployment) {
		if err := depl.Run(context.Background()); err != nil {
			log.WithError(err).WithField("deployment_id", depl.GetID()).Error("api: fuota deployment error")
		}
	}(depl)

	return &fapi.CreateDeploymentResponse{
		Id: depl.GetID().Bytes(),
	}, nil
}

// CreateMulticastDeployment creates the given Multicast deployment.
func (a *FUOTAServerAPI) CreateMulticastDeployment(ctx context.Context, req *fapi.CreateMulticastDeploymentRequest) (*fapi.CreateMulticastDeploymentResponse, error) {
	opts := multicast.DeploymentOptions{
		ApplicationID:            req.GetDeployment().ApplicationId,
		Devices:                  make(map[lorawan.EUI64]multicast.DeviceOptions),
		MulticastDR:              uint8(req.GetDeployment().MulticastDr),
		MulticastFrequency:       req.GetDeployment().MulticastFrequency,
		MulticastGroupID:         uint8(req.GetDeployment().MulticastGroupId),
		MulticastTimeout:         uint8(req.GetDeployment().MulticastTimeout),
		UnicastAttemptCount:      int(req.GetDeployment().UnicastAttemptCount),
		ExistingMulticastGroupID: req.GetDeployment().ExistingMulticastGroupId,
		ExistingDeploymentID:     req.GetDeployment().ExistingDeploymentId,
	}

	for _, d := range req.GetDeployment().Devices {
		var devEUI lorawan.EUI64
		var mcRootKey lorawan.AES128Key

		copy(devEUI[:], d.DevEui)
		copy(mcRootKey[:], d.McRootKey)

		opts.Devices[devEUI] = multicast.DeviceOptions{
			McRootKey: mcRootKey,
		}
	}

	//switch when necessary, we just working with class c devices currently.
	//switch req.GetDeployment().MulticastGroupType {
	//case fapi.MulticastGroupType_CLASS_B:
	//	opts.MulticastGroupType = api.MulticastGroupType_CLASS_B
	//case fapi.MulticastGroupType_CLASS_C:
	//	opts.MulticastGroupType = api.MulticastGroupType_CLASS_C
	//}
	opts.MulticastGroupType = api.MulticastGroupType_CLASS_C

	unicastTimeout, err := ptypes.Duration(req.GetDeployment().UnicastTimeout)
	if err != nil {
		return nil, err
	}

	opts.UnicastTimeout = unicastTimeout

	var depl *multicast.MulticastDeployment
	if len(req.GetDeployment().ExistingDeploymentId) != 0 && req.GetDeployment().ExistingDeploymentId != "" {
		existingDeployment, err := multicast.GetExistingDeployment(req.GetDeployment().ExistingDeploymentId, opts)
		if err != nil {
			return nil, err
		}
		depl = existingDeployment
	} else {
		newDepl, err := multicast.NewDeployment(opts)
		if err != nil {
			return nil, err
		}
		depl = newDepl
	}

	go func(depl *multicast.MulticastDeployment) {
		if err := depl.Run(context.Background()); err != nil {
			log.WithError(err).WithField("deployment_id", depl.GetID()).Error("api: multicast deployment error")
		}
	}(depl)

	var waitFlag = 1

	for waitFlag == 1 {
		var mcGroupID = depl.GetMulticastGroupID()
		if len(mcGroupID) > 0 && mcGroupID != "" {
			waitFlag = 0
			log.WithField("waitFlag", waitFlag).Debug("fuota: waitFlag value has changed to 0")
		}
	}

	var mcGroupID = depl.GetMulticastGroupID()

	return &fapi.CreateMulticastDeploymentResponse{
		Id:               depl.GetID().Bytes(),
		MulticastGroupId: mcGroupID,
	}, nil
}

func (a *FUOTAServerAPI) BulkMulticastDeployment(ctx context.Context, req *fapi.BulkMulticastDeploymentRequest) (*fapi.BulkMulticastDeploymentResponse, error) {
	var devEuiList [][]byte

	var deployment = req.GetDeployment()

	var genAppKey = deployment.McRootKey
	var appId = deployment.ApplicationId

	var deviceCount = len(deployment.GetDevices())
	if deviceCount < 1 {
		return nil, errors.New("empty device list")
	}

	var multicastGroupID = deployment.GetMulticastGroupId()
	var existingMCgroupID = deployment.GetExistingMulticastGroupId()
	var existingDeploymentID = deployment.GetExistingDeploymentId()

	for _, d := range deployment.GetDevices() {
		var devEUI []byte
		devEUI, err := hex.DecodeString(d.DevEui)
		if err != nil {
			return nil, errors.New("devEUI parse error")
		}

		devEuiList = append(devEuiList, devEUI)
	}
	log.Debug(devEuiList)
	//, unicastTimeout int, unicastAttemptCount int, applicationId int, multicastFrequency int, multicastDR int
	var nbOfDevices, createdMulticastGroupId, deploymentId, err = multicast.BulkMulticastDeployment(genAppKey, devEuiList, int64(multicastGroupID), int(deployment.GetUnicastTimeout()), int(deployment.GetUnicastAttemptCount()), int(appId), int(deployment.GetMulticastFrequency()), int(deployment.GetMulticastDr()), existingMCgroupID, existingDeploymentID)
	if err != nil {
		return nil, err
	}

	return &fapi.BulkMulticastDeploymentResponse{
		NumberOfDevices:  uint32(nbOfDevices),
		MulticastGroupId: createdMulticastGroupId,
		DeploymentId:     deploymentId,
	}, nil
}

// GetDeploymentStatus returns the FUOTA deployment status given an ID.
func (a *FUOTAServerAPI) GetDeploymentStatus(ctx context.Context, req *fapi.GetDeploymentStatusRequest) (*fapi.GetDeploymentStatusResponse, error) {
	var id uuid.UUID
	copy(id[:], req.GetId())

	d, err := storage.GetDeployment(ctx, storage.DB(), id)
	if err != nil {
		return nil, err
	}

	var resp fapi.GetDeploymentStatusResponse

	resp.CreatedAt, err = ptypes.TimestampProto(d.CreatedAt)
	if err != nil {
		return nil, err
	}

	resp.UpdatedAt, err = ptypes.TimestampProto(d.UpdatedAt)
	if err != nil {
		return nil, err
	}

	if d.MCGroupSetupCompletedAt != nil {
		resp.McGroupSetupCompletedAt, err = ptypes.TimestampProto(*d.MCGroupSetupCompletedAt)
		if err != nil {
			return nil, err
		}
	}

	if d.MCSessionCompletedAt != nil {
		resp.McSessionCompletedAt, err = ptypes.TimestampProto(*d.MCSessionCompletedAt)
		if err != nil {
			return nil, err
		}
	}

	if d.FragSessionSetupCompletedAt != nil {
		resp.FragSessionSetupCompletedAt, err = ptypes.TimestampProto(*d.FragSessionSetupCompletedAt)
		if err != nil {
			return nil, err
		}
	}

	if d.EnqueueCompletedAt != nil {
		resp.EnqueueCompletedAt, err = ptypes.TimestampProto(*d.EnqueueCompletedAt)
		if err != nil {
			return nil, err
		}
	}

	if d.FragStatusCompletedAt != nil {
		resp.FragStatusCompletedAt, err = ptypes.TimestampProto(*d.FragStatusCompletedAt)
		if err != nil {
			return nil, err
		}
	}

	devices, err := storage.GetDeploymentDevices(ctx, storage.DB(), id)
	if err != nil {
		return nil, err
	}

	for _, device := range devices {
		var dd fapi.DeploymentDeviceStatus
		var err error

		dd.CreatedAt, err = ptypes.TimestampProto(device.CreatedAt)
		if err != nil {
			return nil, err
		}

		dd.UpdatedAt, err = ptypes.TimestampProto(device.UpdatedAt)
		if err != nil {
			return nil, err
		}

		if device.MCGroupSetupCompletedAt != nil {
			dd.McGroupSetupCompletedAt, err = ptypes.TimestampProto(*device.MCGroupSetupCompletedAt)
			if err != nil {
				return nil, err
			}
		}

		if device.MCSessionCompletedAt != nil {
			dd.McSessionCompletedAt, err = ptypes.TimestampProto(*device.MCSessionCompletedAt)
			if err != nil {
				return nil, err
			}
		}

		if device.FragSessionSetupCompletedAt != nil {
			dd.FragSessionSetupCompletedAt, err = ptypes.TimestampProto(*device.FragSessionSetupCompletedAt)
			if err != nil {
				return nil, err
			}
		}

		if device.FragStatusCompletedAt != nil {
			dd.FragStatusCompletedAt, err = ptypes.TimestampProto(*device.FragStatusCompletedAt)
			if err != nil {
				return nil, err
			}
		}

		resp.DeviceStatus = append(resp.DeviceStatus, &dd)
	}

	return &resp, nil
}

// GetDeploymentDeviceLogs returns the FUOTA logs given a deployment ID and DevEUI.
func (a *FUOTAServerAPI) GetDeploymentDeviceLogs(ctx context.Context, req *fapi.GetDeploymentDeviceLogsRequest) (*fapi.GetDeploymentDeviceLogsResponse, error) {
	var deploymentID uuid.UUID
	var devEUI lorawan.EUI64
	var resp fapi.GetDeploymentDeviceLogsResponse

	copy(deploymentID[:], req.GetDeploymentId())
	copy(devEUI[:], req.GetDevEui())

	logs, err := storage.GetDeploymentLogsForDevice(ctx, storage.DB(), deploymentID, devEUI)
	if err != nil {
		return nil, err
	}

	for _, l := range logs {
		dl := fapi.DeploymentDeviceLog{
			FPort:   uint32(l.FPort),
			Command: l.Command,
			Fields:  make(map[string]string),
		}

		dl.CreatedAt, err = ptypes.TimestampProto(l.CreatedAt)
		if err != nil {
			return nil, err
		}

		for k, v := range l.Fields.Map {
			dl.Fields[k] = v.String
		}

		resp.Logs = append(resp.Logs, &dl)
	}

	return &resp, nil
}

// Resets multicast setup of the device
func (a *FUOTAServerAPI) ResetMulticastSetup(ctx context.Context, req *fapi.ResetMulticastSetupRequest) (*fapi.ResetMulticastSetupResponse, error) {
	var resp fapi.ResetMulticastSetupResponse

	byteArray, err := hex.DecodeString(req.DevEui)
	if err != nil {
		return nil, fmt.Errorf("hex.DecodeString DevEui error: %w", err)
	}

	if len(byteArray) != 8 {
		return nil, fmt.Errorf("Hata: Hex string lenght is not valid")
	}

	eui64 := lorawan.EUI64{}
	copy(eui64[:], byteArray)

	parsedUUID, err := uuid.FromString(req.DeploymentId)
	if err != nil {
		return nil, fmt.Errorf("uuid.Parse DeploymentId error: %w", err)
	}

	dd, err := storage.GetDeploymentDevice(ctx, storage.DB(), parsedUUID, eui64)
	if err != nil {
		return nil, fmt.Errorf("get deployment device error: %w", err)
	}

	dd.MCGroupSetupCompletedAt = nil
	if err := storage.UpdateDeploymentDevice(ctx, storage.DB(), &dd); err != nil {
		return nil, fmt.Errorf("update deployment device error: %w", err)
	}

	resp.IsSucceed = true
	return &resp, nil
}
