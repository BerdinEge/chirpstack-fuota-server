package fuota

import (
	"context"
	"crypto/aes"
	"crypto/rand"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/gofrs/uuid"
	"github.com/golang/protobuf/ptypes"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq/hstore"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	"github.com/brocaar/chirpstack-api/go/v3/as/external/api"
	"github.com/brocaar/chirpstack-api/go/v3/as/integration"
	"github.com/brocaar/chirpstack-api/go/v3/fuota"
	"github.com/brocaar/chirpstack-fuota-server/internal/client/as"
	"github.com/brocaar/chirpstack-fuota-server/internal/eventhandler"
	"github.com/brocaar/chirpstack-fuota-server/internal/storage"
	"github.com/brocaar/lorawan"
	"github.com/brocaar/lorawan/applayer/multicastsetup"
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

		multicastSetupDone: make(chan struct{}),
		//fragmentationSessionSetupDone:  make(chan struct{}),
		//multicastSessionSetupDone:      make(chan struct{}),
		//fragmentationSessionStatusDone: make(chan struct{}),
	}

	if err := storage.Transaction(func(tx sqlx.Ext) error {
		st := storage.Deployment{
			ID: d.id,
		}
		if err := storage.CreateDeployment(context.Background(), tx, &st); err != nil {
			return fmt.Errorf("create deployment error: %w", err)
		}

		for devEUI, _ := range opts.Devices {
			d.deviceState[devEUI] = &deviceState{}

			sdd := storage.DeploymentDevice{
				DeploymentID: d.id,
				DevEUI:       devEUI,
			}
			if err := storage.CreateDeploymentDevice(context.Background(), tx, &sdd); err != nil {
				return fmt.Errorf("create deployment device error: %w", err)
			}
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return &d, nil
}

func BulkMulticastDeployment(genAppKey []byte, devEuiList [][]byte, groupId int64) int {

	mcRootKey, err := multicastsetup.GetMcRootKeyForGenAppKey(lorawan.AES128Key{0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})
	if err != nil {
		log.Fatal(err)
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
			ApplicationId: 106,
			Devices: []*fuota.DeploymentDevice{
				{
					DevEui:    []byte{9, 0, 0, 0, 0, 0, 0, 0},
					McRootKey: mcRootKey[:],
				},
			},
			MulticastDr:         5,
			MulticastFrequency:  868100000,
			MulticastGroupId:    0,
			MulticastTimeout:    6,
			UnicastTimeout:      ptypes.DurationProto(60 * time.Second),
			UnicastAttemptCount: 1,
		},
	})
	if err != nil {
		panic(err)
	}

	var id uuid.UUID
	copy(id[:], resp.GetId())

	fmt.Printf("deployment created: %s\n", id)

	return len(devEuiList)
}

// GetID returns the random assigned FUOTA deployment ID.
func (d *MulticastDeployment) GetID() uuid.UUID {
	return d.id
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

		_, err := as.MulticastGroupClient().AddDevice(ctx, &api.AddDeviceToMulticastGroupRequest{
			MulticastGroupId: d.multicastGroupID,
			DevEui:           devEUI.String(),
		})
		if err != nil {
			return fmt.Errorf("add device to multicast-group error: %w", err)
		}
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
			log.WithField("deployment_id", d.GetID()).Warning("fuota: multicast-setup reached max. number of attepts, some devices did not complete")
			break
		}

		for devEUI := range d.opts.Devices {
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
