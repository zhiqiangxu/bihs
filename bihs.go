package bihs

import (
	"bytes"
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/zhiqiangxu/util/wm"
)

// HotStuff ...
type HotStuff struct {
	view              uint64
	height            uint64
	started           int32
	proposeCh         chan Block
	lockQC            *QC
	commitQC          *QC
	prepareMsg        *Msg
	preCommitMsg      *Msg
	votes1            map[string][]byte
	votes2            map[string][]byte
	p2p               P2P
	store             StateDB
	conf              Config
	ngCount           int
	proposeRelayTimer *time.Timer
	nvInterrupt       *time.Timer
	waiter            *wm.Offset
}

func New(genesis Block, store StateDB, p2p P2P, conf Config) *HotStuff {
	b0 := store.GetBlock(0)
	if b0 == nil {
		err := store.StoreBlock(genesis, nil, nil)
		if err != nil {
			panic(fmt.Sprintf("StateDB.StoreNode failed:%v", err))
		}
	} else {
		if !bytes.Equal(b0.Hash(), genesis.Hash()) {
			panic("stored genesis doesn't match")
		}
	}

	err := conf.validate()
	if err != nil {
		panic(fmt.Sprintf("Config.Validate failed:%v", err))
	}

	return &HotStuff{
		proposeCh: make(chan Block),
		p2p:       p2p,
		store:     store,
		conf:      conf,
		waiter:    wm.NewOffset(),
	}
}

func (hs *HotStuff) Start() (err error) {
	swapped := atomic.CompareAndSwapInt32(&hs.started, 0, 1)
	if !swapped {
		err = fmt.Errorf("already started")
		return
	}

	hs.enterHeightView(hs.store.Height()+1, uint64(hs.store.GetVoted()+1))
	go hs.loop()

	return
}

func (hs *HotStuff) Propose(ctx context.Context, blk Block) (err error) {
	err = blk.Validate()
	if err != nil {
		return
	}

	height := blk.Height()
	if height <= hs.store.Height() {
		err = fmt.Errorf("block for specified height already exists")
		return
	}

	go func() {

		select {
		case hs.proposeCh <- blk:
		case <-ctx.Done():
		}

	}()

	return
}

func (hs *HotStuff) Wait(ctx context.Context, height uint64) error {
	return hs.waiter.Wait(ctx, int64(height))
}

func (hs *HotStuff) Height() uint64 {
	return atomic.LoadUint64(&hs.height)
}

func (hs *HotStuff) View() uint64 {
	return atomic.LoadUint64(&hs.view)
}
