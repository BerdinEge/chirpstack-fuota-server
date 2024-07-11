package fuota

import (
	"context"
	"crypto/aes"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/gofrs/uuid"
	"github.com/golang/protobuf/ptypes"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq/hstore"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"

	"github.com/brocaar/chirpstack-api/go/v3/as/external/api"
	"github.com/brocaar/chirpstack-api/go/v3/as/integration"
	"github.com/brocaar/chirpstack-api/go/v3/fuota"
	"github.com/brocaar/chirpstack-fuota-server/internal/client/as"
	"github.com/brocaar/chirpstack-fuota-server/internal/eventhandler"
	"github.com/brocaar/chirpstack-fuota-server/internal/storage"
	"github.com/brocaar/lorawan"
	"github.com/brocaar/lorawan/applayer/multicastsetup"

	"github.com/brocaar/chirpstack-fuota-server/internal/config"
)

// Deployments defines the FUOTA deployment struct.
type MulticastDeployment struct {
	opts DeploymentOptions
	id   uuid.UUID

	// this contains the ID from CreateMulticastGroupResponse
	multicastGroupID string

	// McAddr.
	mcAddr lorawan.DevAddr

	// mcKey.
	mcKey lorawan.AES128Key

	// deviceState contains the per device FUOTA state
	deviceState map[lorawan.EUI64]*deviceState

	// channel for completing the multicast-setup
	multicastSetupDone chan struct{}

	// channel for fragmentation-session setup
	fragmentationSessionSetupDone chan struct{}

	// channel multicast session setup
	multicastSessionSetupDone chan struct{}

	// channel for fragmentation-session status
	fragmentationSessionStatusDone chan struct{}

	// session start time
	// this is set by the multicast-session setup function
	sessionStartTime time.Time

	// session end time
	// this is set by the multicast-session setup function
	sessionEndTime time.Time

	// existing mc group id
	existingMulticastGroupID string

	// existing devices in the mc group
	devicesAlreadyInTheMCGroup []lorawan.EUI64
}

// DeploymentOptions defines the options for a FUOTA Deployment.
type DeploymentOptions struct {
	// The application id.
	ApplicationID int64

	// The Devices to include in the update.
	// Note: the devices must be part of the above application id.
	Devices map[lorawan.EUI64]DeviceOptions

	// MulticastGroupType defines the multicast type (B/C)
	MulticastGroupType api.MulticastGroupType

	// Multicast DR defines the multicast data-rate.
	MulticastDR uint8

	// MulticastPingSlotPeriodicity defines the ping-slot periodicity (Class-B).
	// Expected values: 0 -7.
	MulticastPingSlotPeriodicity uint8

	// MulticastFrequency defines the frequency.
	MulticastFrequency uint32

	// MulticastGroupID defines the multicast group ID.
	MulticastGroupID uint8

	// MulticastTimeout defines the timeout of the multicast-session.
	// Please refer to the Remote Multicast Setup specification as this field
	// has a different meaning for Class-B and Class-C groups.
	MulticastTimeout uint8

	// UnicastTimeout.
	// Set this to the value in which you at least expect an uplink frame from
	// the device. The FUOTA server will wait for the given time before
	// attempting a retry or continuing with the next step.
	UnicastTimeout time.Duration

	// UnicastAttemptCount.
	// The number of attempts before considering an unicast command
	// to be failed.
	UnicastAttemptCount int

	// FragSize defines the max size for each payload fragment.
	FragSize int

	// Payload defines the FUOTA payload.
	Payload []byte

	// Redundancy (in number of packets).
	Redundancy int

	// FragmentationSessionIndex.
	FragmentationSessionIndex uint8

	// FragmentationMatrix.
	FragmentationMatrix uint8

	// BlockAckDelay.
	BlockAckDelay uint8

	// Descriptor.
	Descriptor [4]byte

	// Existing mc group id
	ExistingMulticastGroupID string

	// Existing deployment id
	ExistingDeploymentID string
}

// DeviceOptions holds the device options.
type DeviceOptions struct {
	// McRootKey holds the McRootKey.
	// Note: please refer to the Remote Multicast Setup specification for more
	// information (page 10).
	McRootKey lorawan.AES128Key
}

// deviceState contains the FUOTA state of a device
type deviceState struct {
	sync.RWMutex
	multicastSetup             bool
	fragmentationSessionSetup  bool
	multicastSessionSetup      bool
	fragmentationSessionStatus bool
}

func (d *deviceState) getMulticastSetup() bool {
	d.RLock()
	defer d.RUnlock()
	return d.multicastSetup
}

func (d *deviceState) setMulticastSetup(done bool) {
	d.Lock()
	defer d.Unlock()
	d.multicastSetup = done
}

func stringToUUID(inputString string) (uuid.UUID, error) {
	// String'i UUID'ye çevir
	uuidFromString, err := uuid.FromString(inputString)
	if err != nil {
		return uuid.Nil, err
	}

	return uuidFromString, nil
}

func GetExistingDeployment(existingDeploymentID string, existingOpts DeploymentOptions) (*MulticastDeployment, error) {
	// String'i UUID'ye çevir
	existingDeploymentIDUUID, err := stringToUUID(existingDeploymentID)
	if err != nil {
		fmt.Println("UUID oluşturulamadı:", err)
		return nil, err
	}

	d := MulticastDeployment{
		id:          existingDeploymentIDUUID,
		opts:        existingOpts,
		deviceState: make(map[lorawan.EUI64]*deviceState),

		multicastSetupDone:       make(chan struct{}),
		existingMulticastGroupID: existingOpts.ExistingMulticastGroupID,
	}

	return &d, nil
}

// NewDeployment creates a new Deployment.
func NewDeployment(opts DeploymentOptions) (*MulticastDeployment, error) {
	id, err := uuid.NewV4()
	if err != nil {
		return nil, fmt.Errorf("new uuid error: %w", err)
	}

	d := MulticastDeployment{
		id:          id,
		opts:        opts,
		deviceState: make(map[lorawan.EUI64]*deviceState),

		multicastSetupDone:       make(chan struct{}),
		existingMulticastGroupID: opts.ExistingMulticastGroupID,
	}

	if err := storage.Transaction(func(tx sqlx.Ext) error {
		st := storage.Deployment{
			ID: d.id,
		}
		if err := storage.CreateDeployment(context.Background(), tx, &st); err != nil {
			return fmt.Errorf("create deployment error: %w", err)
		}

		//for devEUI, _ := range opts.Devices {
		//	d.deviceState[devEUI] = &deviceState{}
		//
		//	sdd := storage.DeploymentDevice{
		//		DeploymentID: d.id,
		//		DevEUI:       devEUI,
		//	}
		//	if err := storage.CreateDeploymentDevice(context.Background(), tx, &sdd); err != nil {
		//		return fmt.Errorf("create deployment device error: %w", err)
		//	}
		//}

		return nil
	}); err != nil {
		return nil, err
	}

	return &d, nil
}

func BulkMulticastDeployment(genAppKey string, devEuiList [][]byte, groupId int64, unicastTimeout int, unicastAttemptCount int, applicationId int, multicastFrequency int, multicastDR int, existingMCgroupID string, existingDeploymentID string) (int, string, string, error) {

	hexBytes, err := hex.DecodeString(genAppKey)
	if err != nil {
		fmt.Println("Convert error:", err)
		return 0, "", "", nil
	}

	var aesKeyByteArray lorawan.AES128Key
	copy(aesKeyByteArray[:], hexBytes)

	mcRootKey, err := multicastsetup.GetMcRootKeyForGenAppKey(aesKeyByteArray)
	if err != nil {
		log.Fatal(err)
		return 0, "", "", nil
	}

	var devicesToDeploy []*fuota.DeploymentDevice

	for _, devEuiBytes := range devEuiList {
		device := &fuota.DeploymentDevice{
			DevEui:    devEuiBytes,
			McRootKey: mcRootKey[:],
		}

		devicesToDeploy = append(devicesToDeploy, device)
	}

	dialOpts := []grpc.DialOption{
		grpc.WithBlock(),
		grpc.WithInsecure(),
	}

	conn, err := grpc.Dial("localhost:8070", dialOpts...)
	if err != nil {
		panic(err)
	}

	client := fuota.NewFUOTAServerServiceClient(conn)

	resp, err := client.CreateMulticastDeployment(context.Background(), &fuota.CreateMulticastDeploymentRequest{
		Deployment: &fuota.MulticastDeployment{
			ApplicationId:            int64(applicationId),
			Devices:                  devicesToDeploy,
			MulticastDr:              uint32(multicastDR),
			MulticastFrequency:       uint32(multicastFrequency),
			MulticastGroupId:         uint32(groupId),
			MulticastTimeout:         60,
			UnicastTimeout:           ptypes.DurationProto(time.Duration(unicastTimeout) * time.Second),
			UnicastAttemptCount:      uint32(unicastAttemptCount),
			ExistingMulticastGroupId: existingMCgroupID,
			ExistingDeploymentId:     existingDeploymentID,
		},
	})
	if err != nil {
		panic(err)
	}

	var id uuid.UUID
	copy(id[:], resp.GetId())

	fmt.Printf("Multicast Deployment operation. DeploymentId : %s\n", id)

	return len(devEuiList), resp.MulticastGroupId, id.String(), nil
}

// GetID returns the random assigned FUOTA deployment ID.
func (d *MulticastDeployment) GetID() uuid.UUID {
	return d.id
}

// GetMulticastGroupID returns the ID of the created multicast group-if it exists.
func (d *MulticastDeployment) GetMulticastGroupID() string {
	return d.multicastGroupID
}

// Run starts the FUOTA update.
func (d *MulticastDeployment) Run(ctx context.Context) error {
	eventhandler.Get().RegisterUplinkEventFunc(d.GetID(), d.HandleUplinkEvent)
	defer eventhandler.Get().UnregisterUplinkEventFunc(d.GetID())

	steps := []func(context.Context) error{
		d.stepCreateMulticastGroup,
		d.stepAddDevicesToMulticastGroup,
		d.stepMulticastSetup,
		//d.stepMulticastClassBSessionSetup,
		//d.stepMulticastClassCSessionSetup,
	}

	for _, f := range steps {
		if err := f(ctx); err != nil {
			return err
		}
	}

	return nil
}

// HandleUplinkEvent handles the given uplink event.
// In case it does not match one of the FUOTA ports or DevEUI within the
// deployment, the uplink is silently discarded.
func (d *MulticastDeployment) HandleUplinkEvent(ctx context.Context, pl integration.UplinkEvent) error {
	var devEUI lorawan.EUI64
	copy(devEUI[:], pl.DevEui)
	_, found := d.opts.Devices[devEUI]

	if uint8(pl.FPort) == multicastsetup.DefaultFPort && found {
		if err := d.handleMulticastSetupCommand(ctx, devEUI, pl.Data); err != nil {
			return fmt.Errorf("handle multicast setup command error: %w", err)
		}
	} else {
		log.WithFields(log.Fields{
			"deployment_id": d.id,
			"dev_eui":       devEUI,
			"f_port":        pl.FPort,
		}).Debug("fuota: ignoring uplink event")
	}

	return nil
}

// handleMulticastSetupCommand handles an uplink multicast setup command.
func (d *MulticastDeployment) handleMulticastSetupCommand(ctx context.Context, devEUI lorawan.EUI64, b []byte) error {
	var cmd multicastsetup.Command
	if err := cmd.UnmarshalBinary(true, b); err != nil {
		return fmt.Errorf("unmarshal command error: %w", err)
	}

	log.WithFields(log.Fields{
		"deployment_id": d.GetID(),
		"dev_eui":       devEUI,
		"cid":           cmd.CID,
	}).Info("fuota: multicast-setup command received")

	switch cmd.CID {
	case multicastsetup.McGroupSetupAns:
		pl, ok := cmd.Payload.(*multicastsetup.McGroupSetupAnsPayload)
		if !ok {
			return fmt.Errorf("expected *multicastsetup.McGroupSetupAnsPayload, got: %T", cmd.Payload)
		}
		return d.handleMcGroupSetupAns(ctx, devEUI, pl)
	}

	return nil
}

func (d *MulticastDeployment) handleMcGroupSetupAns(ctx context.Context, devEUI lorawan.EUI64, pl *multicastsetup.McGroupSetupAnsPayload) error {
	log.WithFields(log.Fields{
		"deployment_id": d.GetID(),
		"dev_eui":       devEUI,
		"id_error":      pl.McGroupIDHeader.IDError,
		"mc_group_id":   pl.McGroupIDHeader.McGroupID,
	}).Info("fuota: McGroupSetupAns received")

	dl := storage.DeploymentLog{
		DeploymentID: d.GetID(),
		DevEUI:       devEUI,
		FPort:        multicastsetup.DefaultFPort,
		Command:      "McGroupSetupAns",
		Fields: hstore.Hstore{
			Map: map[string]sql.NullString{
				"mc_group_id": sql.NullString{Valid: true, String: fmt.Sprintf("%d", pl.McGroupIDHeader.McGroupID)},
				"id_error":    sql.NullString{Valid: true, String: fmt.Sprintf("%t", pl.McGroupIDHeader.IDError)},
			},
		},
	}
	if err := storage.CreateDeploymentLog(ctx, storage.DB(), &dl); err != nil {
		log.WithError(err).Error("fuota: create deployment log error")
	}

	if pl.McGroupIDHeader.McGroupID == d.opts.MulticastGroupID && !pl.McGroupIDHeader.IDError {
		// update the device state
		if state, ok := d.deviceState[devEUI]; ok {
			state.setMulticastSetup(true)
		}

		dd, err := storage.GetDeploymentDevice(ctx, storage.DB(), d.GetID(), devEUI)
		if err != nil {
			return fmt.Errorf("get deployment device error: %w", err)
		}
		now := time.Now()
		dd.MCGroupSetupCompletedAt = &now
		if err := storage.UpdateDeploymentDevice(ctx, storage.DB(), &dd); err != nil {
			return fmt.Errorf("update deployment device error: %w", err)
		}

		// if all devices have finished the multicast-setup, publish to done chan.
		done := true
		for _, state := range d.deviceState {
			if !state.getMulticastSetup() {
				done = false
			}
		}
		if done {
			d.multicastSetupDone <- struct{}{}
		}
	}

	return nil
}

// create multicast group step
func (d *MulticastDeployment) stepCreateMulticastGroup(ctx context.Context) error {
	log.WithField("deployment_id", d.GetID()).Debug("fuota: stepCreateMulticastGroup funtion called")

	if d.existingMulticastGroupID != "" && len(d.existingMulticastGroupID) > 0 {
		log.WithField("deployment_id", d.GetID()).Debug("fuota: stepCreateMulticastGroup passed couse of an existing multicast group")
		d.multicastGroupID = d.existingMulticastGroupID
		return nil
	}

	// generate randomd devaddr
	if _, err := rand.Read(d.mcAddr[:]); err != nil {
		return fmt.Errorf("read random bytes error: %w", err)
	}

	// generate random McKey
	if _, err := rand.Read(d.mcKey[:]); err != nil {
		return fmt.Errorf("read random bytes error: %w", err)
	}

	// get McAppSKey
	mcAppSKey, err := multicastsetup.GetMcAppSKey(d.mcKey, d.mcAddr)
	if err != nil {
		return fmt.Errorf("get McAppSKey error: %w", err)
	}

	// get McNetSKey
	mcNetSKey, err := multicastsetup.GetMcNetSKey(d.mcKey, d.mcAddr)
	if err != nil {
		return fmt.Errorf("get McNetSKey error: %s", err)
	}

	mg := api.MulticastGroup{
		Name:           fmt.Sprintf("fuota-%s", d.GetID()),
		McAddr:         d.mcAddr.String(),
		McNwkSKey:      mcNetSKey.String(),
		McAppSKey:      mcAppSKey.String(),
		GroupType:      d.opts.MulticastGroupType,
		Dr:             uint32(d.opts.MulticastDR),
		Frequency:      d.opts.MulticastFrequency,
		PingSlotPeriod: uint32(1 << int(5+d.opts.MulticastPingSlotPeriodicity)), // note: period = 2 ^ (5 + periodicity)
		ApplicationId:  d.opts.ApplicationID,
	}

	resp, err := as.MulticastGroupClient().Create(ctx, &api.CreateMulticastGroupRequest{
		MulticastGroup: &mg,
	})
	if err != nil {
		return fmt.Errorf("create multicast-group error: %w", err)
	}

	sd, err := storage.GetDeployment(ctx, storage.DB(), d.GetID())
	if err != nil {
		return fmt.Errorf("get deployment error: %w", err)
	}

	sd.McAddr = d.mcAddr[:]
	sd.McKey = d.mcKey[:]

	if err := storage.UpdateDeployment(ctx, storage.DB(), &sd); err != nil {
		return fmt.Errorf("update deployment error: %w", err)
	}

	d.multicastGroupID = resp.Id

	log.WithFields(log.Fields{
		"deployment_id":      d.GetID(),
		"multicast_group_id": resp.Id,
	}).Info("fuota: multicast-group created")

	return nil
}

// add devices to multicast-group
func (d *MulticastDeployment) stepAddDevicesToMulticastGroup(ctx context.Context) error {
	log.WithField("deployment_id", d.GetID()).Info("fuota: add devices to multicast-group")

	for devEUI := range d.opts.Devices {
		log.WithFields(log.Fields{
			"deployment_id":      d.GetID(),
			"dev_eui":            devEUI,
			"multicast_group_id": d.multicastGroupID,
		}).Info("fuota: add device to multicast-group")

		resp, err := as.MulticastGroupClient().AddDevice(ctx, &api.AddDeviceToMulticastGroupRequest{
			MulticastGroupId: d.multicastGroupID,
			DevEui:           devEUI.String(),
		})
		if err != nil {
			var errCode = status.Code(err)
			if errCode == 6 { //if it already exists in the mc group then add it to a list for avoid sending mcSetup command again.
				d.devicesAlreadyInTheMCGroup = append(d.devicesAlreadyInTheMCGroup, devEUI)
				continue
			} else {
				fmt.Errorf("add device to multicast-group error: %w", err)
			}
		} else if resp == nil {
			fmt.Errorf("add device to multicast-group response nil")
			return nil
		} else {
			if err := storage.Transaction(func(tx sqlx.Ext) error {
				d.deviceState[devEUI] = &deviceState{}

				sdd := storage.DeploymentDevice{
					DeploymentID: d.id,
					DevEUI:       devEUI,
				}
				if err := storage.CreateDeploymentDevice(context.Background(), tx, &sdd); err != nil {
					return fmt.Errorf("create deployment device error: %w", err)
				}
				return nil
			}); err != nil {
				return err
			}
		}
		//if err != nil {
		//	return fmt.Errorf("add device to multicast-group error: %w", err)
		//}
	}

	return nil
}

func (d *MulticastDeployment) stepMulticastSetup(ctx context.Context) error {
	log.WithField("deployment_id", d.GetID()).Info("fuota: starting multicast-setup for devices")

	attempt := 0

devLoop:
	for {
		attempt += 1
		if attempt > d.opts.UnicastAttemptCount {
			log.WithField("deployment_id", d.GetID()).Warning("fuota: multicast-setup reached max. number of attempts, some devices did not complete")
			break
		}

		sd, err := storage.GetDeployment(ctx, storage.DB(), d.GetID())
		if err != nil {
			return fmt.Errorf("get deployment error: %w", err)
		}

		for i := 0; i < len(sd.McAddr); i++ {
			d.mcAddr[i] = sd.McAddr[i]
		}
		//d.mcAddr = sd.McAddr
		for i := 0; i < len(sd.McKey); i++ {
			d.mcKey[i] = sd.McKey[i]
		}
		//d.mcKey = sd.McKey

		for devEUI := range d.opts.Devices {
			//if slices.Contains(d.devicesAlreadyInTheMCGroup, devEUI) {
			//	continue
			//}

			//get deployment device from db and check setupComplete command. If setup completed, continue.
			ddFromDb, err := storage.GetDeploymentDevice(ctx, storage.DB(), d.GetID(), devEUI)
			if err != nil {
				// if there is no dd object, it has not added to the mc group. So throw error.
				return fmt.Errorf("get deployment device error, device dont have an DD record and did not found in mc group. Error: %w", err)
			}

			if d.deviceState[devEUI] == nil {
				d.deviceState[devEUI] = &deviceState{} // init device state object
			}

			if ddFromDb.MCGroupSetupCompletedAt != nil {
				d.deviceState[devEUI].setMulticastSetup(true)
			}

			if d.deviceState[devEUI].getMulticastSetup() {
				continue
			}

			log.WithFields(log.Fields{
				"deployment_id": d.GetID(),
				"dev_eui":       devEUI,
				"attempt":       attempt,
			}).Info("fuota: initiate multicast-setup for device")

			// get the encrypted McKey.
			var mcKeyEncrypted lorawan.AES128Key
			mcKEKey, err := multicastsetup.GetMcKEKey(d.opts.Devices[devEUI].McRootKey)
			if err != nil {
				return fmt.Errorf("GetMcKEKey error: %w", err)
			}
			block, err := aes.NewCipher(mcKEKey[:])
			if err != nil {
				return fmt.Errorf("new cipher error: %w", err)
			}
			block.Decrypt(mcKeyEncrypted[:], d.mcKey[:])

			cmd := multicastsetup.Command{
				CID: multicastsetup.McGroupSetupReq,
				Payload: &multicastsetup.McGroupSetupReqPayload{
					McGroupIDHeader: multicastsetup.McGroupSetupReqPayloadMcGroupIDHeader{
						McGroupID: d.opts.MulticastGroupID,
					},
					McAddr:         d.mcAddr,
					McKeyEncrypted: mcKeyEncrypted,
					MinMcFCnt:      0,
					MaxMcFCnt:      (1 << 32) - 1,
				},
			}

			b, err := cmd.MarshalBinary()
			if err != nil {
				return fmt.Errorf("marshal binary error: %w", err)
			}

			_, err = as.DeviceQueueClient().Enqueue(ctx, &api.EnqueueDeviceQueueItemRequest{
				DeviceQueueItem: &api.DeviceQueueItem{
					DevEui: devEUI.String(),
					FPort:  uint32(multicastsetup.DefaultFPort),
					Data:   b,
				},
			})
			if err != nil {
				log.WithError(err).WithFields(log.Fields{
					"deployment_id": d.GetID(),
					"dev_eui":       devEUI,
				}).Error("fuota: enqueue payload error")
				continue
			}

			dl := storage.DeploymentLog{
				DeploymentID: d.GetID(),
				DevEUI:       devEUI,
				FPort:        uint8(multicastsetup.DefaultFPort),
				Command:      "McGroupSetupReq",
				Fields: hstore.Hstore{
					Map: map[string]sql.NullString{
						"mc_group_id":      sql.NullString{Valid: true, String: fmt.Sprintf("%d", d.opts.MulticastGroupID)},
						"mc_addr":          sql.NullString{Valid: true, String: d.mcAddr.String()},
						"mc_key_encrypted": sql.NullString{Valid: true, String: mcKeyEncrypted.String()},
						"min_mc_fcnt":      sql.NullString{Valid: true, String: fmt.Sprintf("%d", 0)},
						"max_mc_fcnt":      sql.NullString{Valid: true, String: fmt.Sprintf("%d", uint32((1<<32)-1))},
					},
				},
			}
			if err := storage.CreateDeploymentLog(ctx, storage.DB(), &dl); err != nil {
				log.WithError(err).Error("fuota: create deployment log error")
			}

			// Sleep for n seconds before next iteration
			var conf *config.Config
			conf = &config.C
			time.Sleep(time.Duration(conf.FUOTAServer.API.CommanDelay) * time.Second)
		}

		select {
		// sleep until next retry
		case <-time.After(d.opts.UnicastTimeout):
			continue devLoop
		// terminate when all devices have been setup
		case <-d.multicastSetupDone:
			log.WithField("deployment_id", d.GetID()).Info("fuota: multicast-setup completed successful for all devices")
			break devLoop
		}
	}

	sd, err := storage.GetDeployment(ctx, storage.DB(), d.GetID())
	if err != nil {
		return fmt.Errorf("get deployment error: %w", err)
	}
	now := time.Now()
	sd.MCGroupSetupCompletedAt = &now
	if err := storage.UpdateDeployment(ctx, storage.DB(), &sd); err != nil {
		return fmt.Errorf("update deployment error: %w", err)
	}

	return nil
}
