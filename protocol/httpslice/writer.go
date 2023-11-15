package httpSlice

import (
	"fmt"
	"net/http"
	"time"

	"github.com/gwuhaolin/livego/av"
	"github.com/gwuhaolin/livego/protocol/amf"
	"github.com/gwuhaolin/livego/utils/pio"
	"github.com/gwuhaolin/livego/utils/uid"

	log "github.com/sirupsen/logrus"
)

const (
	headerLen   = 11
	maxQueueNum = 1024
)

type SliceWriter struct {
	Uid string
	av.RWBaser
	app, title, url string
	buf             []byte
	closed          bool
	closedChan      chan struct{}
	ctx             http.ResponseWriter
	packetQueue     chan *av.Packet
}

func NewSliceWriter(app, title, url string, ctx http.ResponseWriter) *SliceWriter {
	ret := &SliceWriter{
		Uid:         uid.NewId(),
		app:         app,
		title:       title,
		url:         url,
		ctx:         ctx,
		RWBaser:     av.NewRWBaser(time.Second * 10),
		closedChan:  make(chan struct{}),
		buf:         make([]byte, headerLen),
		packetQueue: make(chan *av.Packet, maxQueueNum),
	}

	if _, err := ret.ctx.Write([]byte{0x46, 0x4c, 0x56, 0x01, 0x05, 0x00, 0x00, 0x00, 0x09}); err != nil {
		log.Errorf("Error on response writer")
		ret.closed = true
	}
	pio.PutI32BE(ret.buf[:4], 0)
	if _, err := ret.ctx.Write(ret.buf[:4]); err != nil {
		log.Errorf("Error on response writer")
		ret.closed = true
	}
	go func() {
		err := ret.SendPacket()
		if err != nil {
			log.Debug("SendPacket error: ", err)
			ret.closed = true
		}

	}()
	return ret
}

func (SliceWriter *SliceWriter) DropPacket(pktQue chan *av.Packet, info av.Info) {
	log.Warningf("[%v] packet queue max!!!", info)
	for i := 0; i < maxQueueNum-84; i++ {
		tmpPkt, ok := <-pktQue
		if ok && tmpPkt.IsVideo {
			videoPkt, ok := tmpPkt.Header.(av.VideoPacketHeader)
			// dont't drop sps config and dont't drop key frame
			if ok && (videoPkt.IsSeq() || videoPkt.IsKeyFrame()) {
				log.Debug("insert keyframe to queue")
				pktQue <- tmpPkt
			}

			if len(pktQue) > maxQueueNum-10 {
				<-pktQue
			}
			// drop other packet
			<-pktQue
		}
		// try to don't drop audio
		if ok && tmpPkt.IsAudio {
			log.Debug("insert audio to queue")
			pktQue <- tmpPkt
		}
	}
	log.Debug("packet queue len: ", len(pktQue))
}

func (SliceWriter *SliceWriter) Write(p *av.Packet) (err error) {
	err = nil
	if SliceWriter.closed {
		err = fmt.Errorf("Slicewrite source closed")
		return
	}

	defer func() {
		if e := recover(); e != nil {
			err = fmt.Errorf("SliceWriter has already been closed:%v", e)
		}
	}()

	if len(SliceWriter.packetQueue) >= maxQueueNum-24 {
		SliceWriter.DropPacket(SliceWriter.packetQueue, SliceWriter.Info())
	} else {
		SliceWriter.packetQueue <- p
	}

	return
}

func (SliceWriter *SliceWriter) SendPacket() error {
	for {
		p, ok := <-SliceWriter.packetQueue
		if ok {
			SliceWriter.RWBaser.SetPreTime()
			h := SliceWriter.buf[:headerLen]
			typeID := av.TAG_VIDEO
			if !p.IsVideo {
				if p.IsMetadata {
					var err error
					typeID = av.TAG_SCRIPTDATAAMF0
					p.Data, err = amf.MetaDataReform(p.Data, amf.DEL)
					if err != nil {
						return err
					}
				} else if p.IsAudio {
					typeID = av.TAG_AUDIO
				} else {
					typeID = av.TAG_INDEX
				}
			}
			dataLen := len(p.Data)
			timestamp := p.TimeStamp
			timestamp += SliceWriter.BaseTimeStamp()
			SliceWriter.RWBaser.RecTimeStamp(timestamp, uint32(typeID))

			preDataLen := dataLen + headerLen
			timestampbase := timestamp & 0xffffff
			timestampExt := timestamp >> 24 & 0xff

			pio.PutU8(h[0:1], uint8(typeID))
			pio.PutI24BE(h[1:4], int32(dataLen))
			pio.PutI24BE(h[4:7], int32(timestampbase))
			pio.PutU8(h[7:8], uint8(timestampExt))

			if _, err := SliceWriter.ctx.Write(h); err != nil {
				return err
			}

			if _, err := SliceWriter.ctx.Write(p.Data); err != nil {
				return err
			}

			pio.PutI32BE(h[:4], int32(preDataLen))
			if _, err := SliceWriter.ctx.Write(h[:4]); err != nil {
				return err
			}
		} else {
			return fmt.Errorf("closed")
		}

	}
}

func (SliceWriter *SliceWriter) Wait() {
	select {
	case <-SliceWriter.closedChan:
		return
	}
}

func (SliceWriter *SliceWriter) Close(error) {
	log.Debug("http Slice closed")
	if !SliceWriter.closed {
		close(SliceWriter.packetQueue)
		close(SliceWriter.closedChan)
	}
	SliceWriter.closed = true
}

func (SliceWriter *SliceWriter) Info() (ret av.Info) {
	ret.UID = SliceWriter.Uid
	ret.URL = SliceWriter.url
	ret.Key = SliceWriter.app + "/" + SliceWriter.title
	ret.Inter = true
	return
}
