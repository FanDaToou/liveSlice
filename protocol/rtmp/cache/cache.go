package cache

import (
	"bytes"
	"encoding/binary"
	"github.com/gwuhaolin/livego/av"
	"github.com/gwuhaolin/livego/configure"
	log "github.com/sirupsen/logrus"
	"sync"
)

type Cache struct {
	gop      *GopCache
	videoSeq *SpecialCache
	audioSeq *SpecialCache
	metadata *SpecialCache
	last     uint64
	now      uint64
	slice    *sync.Map
}

func NewCache() *Cache {
	return &Cache{
		gop:      NewGopCache(configure.Config.GetInt("gop_num")),
		videoSeq: NewSpecialCache(),
		audioSeq: NewSpecialCache(),
		metadata: NewSpecialCache(),
		slice:    NewSliceCacheMap(),
	}
}

func (cache *Cache) Write(p av.Packet) {
	if p.IsIndex {
		var index uint64
		err := binary.Read(bytes.NewReader(p.Data), binary.BigEndian, &index)
		if err != nil {
			log.Error("binary.Read failed:", err)
			return
		}
		cache.now = index
		if cache.last == 0 {
			cache.last = index
		}
	}

	item, ok := cache.slice.Load(cache.now)
	if !ok {
		log.Debugf("HandleWriter: not found create new info[%d]", cache.now)
		s := SliceCache{}
		cache.slice.Store(cache.now, s)
		s.p = append(s.p, &p)
	} else {
		np := append(item.(SliceCache).p, &p)
		cache.slice.Store(cache.now, np)
	}

	if p.IsMetadata {
		cache.metadata.Write(&p)
		return
	} else {
		if !p.IsVideo {
			ah, ok := p.Header.(av.AudioPacketHeader)
			if ok {
				if ah.SoundFormat() == av.SOUND_AAC &&
					ah.AACPacketType() == av.AAC_SEQHDR {
					cache.audioSeq.Write(&p)
					return
				} else {
					return
				}
			}

		} else {
			vh, ok := p.Header.(av.VideoPacketHeader)
			if ok {
				if vh.IsSeq() {
					cache.videoSeq.Write(&p)
					return
				}
			} else {
				return
			}

		}
	}
	cache.gop.Write(&p)
}

func (cache *Cache) Send(w av.WriteCloser) error {
	if err := cache.metadata.Send(w); err != nil {
		return err
	}

	if err := cache.videoSeq.Send(w); err != nil {
		return err
	}

	if err := cache.audioSeq.Send(w); err != nil {
		return err
	}

	if err := cache.gop.Send(w); err != nil {
		return err
	}

	return nil
}
