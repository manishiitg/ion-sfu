package sfu

import (
	"strings"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/pion/ion-log"
	"github.com/pion/ion-sfu/pkg/buffer"
	"github.com/pion/rtcp"
	"github.com/pion/transport/packetio"
	"github.com/pion/webrtc/v3"
)

// DownTrackType determines the type of a track
type DownTrackType int

const (
	SimpleDownTrack DownTrackType = iota + 1
	SimulcastDownTrack
)

// DownTrack  implements TrackLocal, is the track used to write packets
// to SFU Subscriber, the track handle the packets for simple, simulcast
// and SVC Publisher.
type DownTrack struct {
	id          string
	peerID      string
	bound       atomicBool
	mime        string
	ssrc        uint32
	streamID    string
	payloadType uint8
	sequencer   *sequencer
	trackType   DownTrackType
	skipFB      int64

	spatialLayer  int32
	temporalLayer int32

	enabled  atomicBool
	reSync   atomicBool
	snOffset uint16
	tsOffset uint32
	lastSSRC uint32
	lastSN   uint16
	lastTS   uint32

	simulcast        simulcastTrackHelpers
	maxSpatialLayer  int64
	maxTemporalLayer int64

	codec          webrtc.RTPCodecCapability
	receiver       Receiver
	transceiver    *webrtc.RTPTransceiver
	writeStream    webrtc.TrackLocalWriter
	onCloseHandler func()
	onBind         func()
	closeOnce      sync.Once

	// Report helpers
	octetCount   uint32
	packetCount  uint32
	maxPacketTs  uint32
	lastPacketMs int64
}

// NewDownTrack returns a DownTrack.
func NewDownTrack(c webrtc.RTPCodecCapability, r Receiver, peerID string) (*DownTrack, error) {
	return &DownTrack{
		id:       r.TrackID(),
		peerID:   peerID,
		streamID: r.StreamID(),
		receiver: r,
		codec:    c,
	}, nil
}

// Bind is called by the PeerConnection after negotiation is complete
// This asserts that the code requested is supported by the remote peer.
// If so it setups all the state (SSRC and PayloadType) to have a call
func (d *DownTrack) Bind(t webrtc.TrackLocalContext) (webrtc.RTPCodecParameters, error) {
	parameters := webrtc.RTPCodecParameters{RTPCodecCapability: d.codec}
	if codec, err := codecParametersFuzzySearch(parameters, t.CodecParameters()); err == nil {
		d.ssrc = uint32(t.SSRC())
		d.payloadType = uint8(codec.PayloadType)
		d.writeStream = t.WriteStream()
		d.mime = strings.ToLower(codec.MimeType)
		d.bound.set(true)
		d.reSync.set(true)
		d.enabled.set(true)
		if rr := bufferFactory.GetOrNew(packetio.RTCPBufferPacket, uint32(t.SSRC())).(*buffer.RTCPReader); rr != nil {
			rr.OnPacket(func(pkt []byte) {
				d.handleRTCP(pkt)
			})
		}
		if strings.HasPrefix(d.codec.MimeType, "video/") {
			d.sequencer = newSequencer()
		}
		d.onBind()
		return codec, nil
	}
	return webrtc.RTPCodecParameters{}, webrtc.ErrUnsupportedCodec
}

// Unbind implements the teardown logic when the track is no longer needed. This happens
// because a track has been stopped.
func (d *DownTrack) Unbind(_ webrtc.TrackLocalContext) error {
	d.bound.set(false)
	return nil
}

// ID is the unique identifier for this Track. This should be unique for the
// stream, but doesn't have to globally unique. A common example would be 'audio' or 'video'
// and StreamID would be 'desktop' or 'webcam'
func (d *DownTrack) ID() string { return d.id }

// Codec returns current track codec capability
func (d *DownTrack) Codec() webrtc.RTPCodecCapability { return d.codec }

// StreamID is the group this track belongs too. This must be unique
func (d *DownTrack) StreamID() string { return d.streamID }

// Kind controls if this TrackLocal is audio or video
func (d *DownTrack) Kind() webrtc.RTPCodecType {
	switch {
	case strings.HasPrefix(d.codec.MimeType, "audio/"):
		return webrtc.RTPCodecTypeAudio
	case strings.HasPrefix(d.codec.MimeType, "video/"):
		return webrtc.RTPCodecTypeVideo
	default:
		return webrtc.RTPCodecType(0)
	}
}

// WriteRTP writes a RTP Packet to the DownTrack
func (d *DownTrack) WriteRTP(p buffer.ExtPacket) error {
	if !d.enabled.get() || !d.bound.get() {
		return nil
	}
	switch d.trackType {
	case SimpleDownTrack:
		return d.writeSimpleRTP(p)
	case SimulcastDownTrack:
		return d.writeSimulcastRTP(p)
	}
	return nil
}

func (d *DownTrack) Mute(val bool) {
	if d.enabled.get() != val {
		return
	}
	d.enabled.set(!val)
	if val {
		d.reSync.set(val)
	}
}

// Close track
func (d *DownTrack) Close() {
	d.closeOnce.Do(func() {
		log.Debugf("Closing sender %s", d.peerID)
		if d.onCloseHandler != nil {
			d.onCloseHandler()
		}
	})
}

func (d *DownTrack) SetInitialLayers(spatialLayer, temporalLayer int64) {
	atomic.StoreInt32(&d.spatialLayer, int32(spatialLayer<<16)|int32(spatialLayer))
	atomic.StoreInt32(&d.temporalLayer, int32(temporalLayer<<16)|int32(temporalLayer))
}

func (d *DownTrack) SwitchSpatialLayer(targetLayer int64, setAsMax bool) {
	if d.trackType == SimulcastDownTrack {
		layer := atomic.LoadInt32(&d.spatialLayer)
		currentLayer := uint16(layer)
		currentTargetLayer := uint16(layer >> 16)

		// Don't switch until previous switch is done or canceled
		if currentLayer != currentTargetLayer ||
			currentLayer == uint16(targetLayer) {
			return
		}
		if err := d.receiver.SubDownTrack(d, int(targetLayer)); err == nil {
			atomic.StoreInt32(&d.spatialLayer, int32(targetLayer<<16)|int32(currentLayer))
			atomic.StoreInt64(&d.skipFB, 4)
			if setAsMax {
				d.maxSpatialLayer = targetLayer
			}
		}
	}
}

func (d *DownTrack) SwitchTemporalLayer(targetLayer int64, setAsMax bool) {
	if d.trackType == SimulcastDownTrack {
		layer := atomic.LoadInt32(&d.temporalLayer)
		currentLayer := uint16(layer)
		currentTargetLayer := uint16(layer >> 16)

		// Don't switch until previous switch is done or canceled
		if currentLayer != currentTargetLayer {
			return
		}
		atomic.StoreInt32(&d.temporalLayer, int32(targetLayer<<16)|int32(currentLayer))
		atomic.StoreInt64(&d.skipFB, 4)
		if setAsMax {
			d.maxTemporalLayer = targetLayer
		}
	}
}

// OnCloseHandler method to be called on remote tracked removed
func (d *DownTrack) OnCloseHandler(fn func()) {
	d.onCloseHandler = fn
}

func (d *DownTrack) OnBind(fn func()) {
	d.onBind = fn
}

func (d *DownTrack) writeSimpleRTP(extPkt buffer.ExtPacket) error {
	if d.reSync.get() {
		if d.Kind() == webrtc.RTPCodecTypeVideo {
			if !extPkt.KeyFrame {
				d.receiver.SendRTCP([]rtcp.Packet{
					&rtcp.PictureLossIndication{SenderSSRC: d.ssrc, MediaSSRC: extPkt.Packet.SSRC},
				})
				return nil
			}
		}
		d.snOffset = extPkt.Packet.SequenceNumber - d.lastSN - 1
		d.tsOffset = extPkt.Packet.Timestamp - d.lastTS - 1
		d.lastSSRC = extPkt.Packet.SSRC
		d.reSync.set(false)
	}

	atomic.AddUint32(&d.octetCount, uint32(len(extPkt.Packet.Payload)))
	atomic.AddUint32(&d.packetCount, 1)

	d.lastSSRC = extPkt.Packet.SSRC
	newSN := extPkt.Packet.SequenceNumber - d.snOffset
	newTS := extPkt.Packet.Timestamp - d.tsOffset
	if d.sequencer != nil {
		d.sequencer.push(extPkt.Packet.SequenceNumber, newSN, newTS, 0, extPkt.Head)
	}
	if (newSN-d.lastSN)&0x8000 == 0 || d.lastSN == 0 {
		d.lastSN = newSN
		atomic.StoreInt64(&d.lastPacketMs, time.Now().UnixNano()/1e6)
		atomic.StoreUint32(&d.lastTS, newTS)
	}
	extPkt.Packet.PayloadType = d.payloadType
	extPkt.Packet.Timestamp = newTS
	extPkt.Packet.SequenceNumber = newSN
	extPkt.Packet.SSRC = d.ssrc

	_, err := d.writeStream.WriteRTP(&extPkt.Packet.Header, extPkt.Packet.Payload)
	if err != nil {
		log.Errorf("Write packet err %v", err)
	}
	return err
}

func (d *DownTrack) writeSimulcastRTP(extPkt buffer.ExtPacket) error {
	// Check if packet SSRC is different from before
	// if true, the video source changed
	if d.lastSSRC != extPkt.Packet.SSRC {
		layer := atomic.LoadInt32(&d.spatialLayer)
		currentLayer := uint16(layer)
		targetLayer := uint16(layer >> 16)
		if currentLayer == targetLayer && d.lastSSRC != 0 {
			return nil
		}
		// Wait for a keyframe to sync new source
		if !extPkt.KeyFrame {
			// Packet is not a keyframe, discard it
			d.receiver.SendRTCP([]rtcp.Packet{
				&rtcp.PictureLossIndication{SenderSSRC: d.ssrc, MediaSSRC: extPkt.Packet.SSRC},
			})
			return nil
		}
		// Switch is done remove sender from previous getLayer
		// and update current getLayer
		if currentLayer != targetLayer {
			go d.receiver.DeleteDownTrack(int(currentLayer), d.peerID)
		}

		atomic.StoreInt32(&d.spatialLayer, int32(targetLayer)<<16|int32(targetLayer))
		if d.simulcast.temporalSupported {
			if d.mime == "video/vp8" {
				if vp8, ok := extPkt.Payload.(buffer.VP8); ok {
					d.simulcast.pRefPicID = d.simulcast.lPicID
					d.simulcast.refPicID = vp8.PictureID
					d.simulcast.pRefTlZIdx = d.simulcast.lTlZIdx
					d.simulcast.refTlZIdx = vp8.TL0PICIDX
				}
			}
		}
	}
	// Compute how much time passed between the old RTP extPkt
	// and the current packet, and fix timestamp on source change
	if !d.simulcast.lTSCalc.IsZero() && d.lastSSRC != extPkt.Packet.SSRC {
		tDiff := time.Now().Sub(d.simulcast.lTSCalc)
		td := uint32((tDiff.Milliseconds() * 90) / 1000)
		if td == 0 {
			td = 1
		}
		d.tsOffset = extPkt.Packet.Timestamp - (d.lastTS + td)
		d.snOffset = extPkt.Packet.SequenceNumber - d.lastSN - 1
	} else if d.simulcast.lTSCalc.IsZero() {
		d.lastTS = extPkt.Packet.Timestamp
		d.lastSN = extPkt.Packet.SequenceNumber
		if d.mime == "video/vp8" {
			if vp8, ok := extPkt.Payload.(buffer.VP8); ok {
				d.simulcast.temporalSupported = vp8.TemporalSupported
			}
		}
	}

	newSN := extPkt.Packet.SequenceNumber - d.snOffset
	newTS := extPkt.Packet.Timestamp - d.tsOffset

	var (
		picID   uint16
		tlz0Idx uint8
	)
	if d.simulcast.temporalSupported {
		if d.mime == "video/vp8" {
			drop := false
			if extPkt.Packet.Payload, picID, tlz0Idx, drop = setVP8TemporalLayer(extPkt, d); drop {
				// Pkt not in temporal getLayer update sequence number offset to avoid gaps
				d.snOffset++
				return nil
			}
		}
	}

	if d.sequencer != nil {
		layer := atomic.LoadInt32(&d.spatialLayer)
		meta := d.sequencer.push(extPkt.Packet.SequenceNumber, newSN, newTS, uint8(layer), extPkt.Head)
		if d.simulcast.temporalSupported && d.mime == "video/vp8" {
			meta.setVP8PayloadMeta(tlz0Idx, picID)
		}
	}

	atomic.AddUint32(&d.octetCount, uint32(len(extPkt.Packet.Payload)))
	atomic.AddUint32(&d.packetCount, 1)

	if (newSN-d.lastSN)&0x8000 == 0 {
		d.lastSN = newSN
		atomic.StoreInt64(&d.lastPacketMs, time.Now().UnixNano()/1e6)
		atomic.StoreUint32(&d.lastTS, newTS)
	}
	// Update base
	d.simulcast.lTSCalc = time.Now()
	d.lastSSRC = extPkt.Packet.SSRC
	// Update extPkt headers
	extPkt.Packet.SequenceNumber = newSN
	extPkt.Packet.Timestamp = newTS
	extPkt.Packet.Header.SSRC = d.ssrc
	extPkt.Packet.Header.PayloadType = d.payloadType

	_, err := d.writeStream.WriteRTP(&extPkt.Packet.Header, extPkt.Packet.Payload)
	if err != nil {
		log.Errorf("Write packet err %v", err)
	}

	if d.simulcast.temporalSupported {
		packetFactory.Put(extPkt.Packet.Payload)
	}

	return err
}

func (d *DownTrack) handleRTCP(bytes []byte) {
	if !d.enabled.get() {
		return
	}

	pkts, err := rtcp.Unmarshal(bytes)
	if err != nil {
		log.Errorf("Unmarshal rtcp receiver packets err: %v", err)
	}

	var fwdPkts []rtcp.Packet
	pliOnce := true
	firOnce := true

	var (
		maxRatePacketLoss  uint8
		expectedMinBitrate uint64
	)

	for _, pkt := range pkts {
		switch p := pkt.(type) {
		case *rtcp.PictureLossIndication:
			if pliOnce {
				p.MediaSSRC = d.lastSSRC
				p.SenderSSRC = d.lastSSRC
				fwdPkts = append(fwdPkts, p)
				pliOnce = false
			}
		case *rtcp.FullIntraRequest:
			if firOnce {
				p.MediaSSRC = d.lastSSRC
				p.SenderSSRC = d.ssrc
				fwdPkts = append(fwdPkts, p)
				firOnce = false
			}
		case *rtcp.ReceiverEstimatedMaximumBitrate:
			if expectedMinBitrate == 0 || expectedMinBitrate > p.Bitrate {
				expectedMinBitrate = p.Bitrate
			}
		case *rtcp.ReceiverReport:
			for _, r := range p.Reports {
				if maxRatePacketLoss == 0 || maxRatePacketLoss < r.FractionLost {
					maxRatePacketLoss = r.FractionLost
				}
			}
		case *rtcp.TransportLayerNack:
			log.Tracef("sender got nack: %+v", p)
			var nackedPackets []packetMeta
			for _, pair := range p.Nacks {
				nackedPackets = append(nackedPackets, d.sequencer.getSeqNoPairs(pair.PacketList())...)
			}
			if err = d.receiver.RetransmitPackets(d, nackedPackets); err != nil {
				return
			}
		}
	}
	if d.trackType == SimulcastDownTrack && maxRatePacketLoss != 0 || expectedMinBitrate != 0 {
	}

	if len(fwdPkts) > 0 {
		d.receiver.SendRTCP(fwdPkts)
	}
}

func (d *DownTrack) handleLayerChange(maxRatePacketLoss uint8, expectedMinBitrate uint64) {
	spatialLayer := atomic.LoadInt32(&d.spatialLayer)
	currentSpatialLayer := int64(spatialLayer & 0x0f)
	targetSpatialLayer := int64(spatialLayer >> 16)

	temporalLayer := atomic.LoadInt32(&d.temporalLayer)
	currentTemporalLayer := int64(temporalLayer & 0x0f)
	targetTemporalLayer := int64(temporalLayer >> 16)

	if targetSpatialLayer == currentSpatialLayer && currentTemporalLayer == targetTemporalLayer {
		skipFB := atomic.LoadInt64(&d.skipFB)
		if skipFB == 0 {
			brs := d.receiver.GetBitrate()
			cbr := brs[currentSpatialLayer]
			mtl := d.receiver.GetMaxTemporalLayer()
			mctl := mtl[currentSpatialLayer]

			if maxRatePacketLoss <= 5 {
				if currentTemporalLayer < mctl && currentTemporalLayer+1 <= d.maxTemporalLayer &&
					expectedMinBitrate >= 3*cbr/4 {
					d.SwitchTemporalLayer(currentTemporalLayer+1, false)
				}
				if currentTemporalLayer >= mctl && expectedMinBitrate >= 3*cbr/2 && currentSpatialLayer+1 <= d.maxSpatialLayer &&
					currentSpatialLayer+1 <= 2 {
					d.SwitchSpatialLayer(currentSpatialLayer+1, false)
					d.SwitchTemporalLayer(0, false)
				}
			}
			if maxRatePacketLoss >= 25 {
				if (expectedMinBitrate <= 5*cbr/8 || currentTemporalLayer == 0) &&
					currentSpatialLayer > 0 &&
					brs[currentSpatialLayer-1] != 0 {
					d.SwitchSpatialLayer(currentSpatialLayer-1, false)
					d.SwitchTemporalLayer(mtl[currentSpatialLayer-1], false)
				} else {
					d.SwitchTemporalLayer(currentTemporalLayer-1, false)
				}
			}
		} else {
			atomic.AddInt64(&d.skipFB, -1)
		}
	}

}

func (d *DownTrack) getSRStats() (octets, packets uint32) {
	octets = atomic.LoadUint32(&d.octetCount)
	packets = atomic.LoadUint32(&d.packetCount)
	return
}
