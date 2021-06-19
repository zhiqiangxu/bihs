package bihs

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ontio/ontology/common"
	"github.com/zhiqiangxu/util"
	"github.com/zhiqiangxu/util/wm"
)

// HotStuff ...
type HotStuff struct {
	view              uint64
	height            uint64
	status            int32
	idx               int
	proposeCh         chan Block
	candidateBlk      Block
	lockQC            *QC
	hasVoted          bool
	votes1            map[int][]byte
	votes2            map[int][]byte
	voted             []ID
	p2p               P2P
	store             StateDB
	conf              Config
	proposeRelayTimer *time.Timer
	nvInterrupt       *time.Timer
	waiter            *wm.Offset
	wg                sync.WaitGroup
	waitBlock         func(ID, *Msg)

	createMsg      func(mt MsgType, justify *QC, node *BlockOrHash) *Msg
	sign           func([]byte) []byte
	tcombine       func(msg *Msg, votes map[int][]byte) []byte
	validateQC     func(qc *QC) error
	verify         func(msg *Msg) (voter ID, err error)
	generateBitmap func(votes map[int][]byte) []byte
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

	hs := &HotStuff{
		proposeCh: make(chan Block),
		p2p:       p2p,
		store:     store,
		conf:      conf,
		waiter:    wm.NewOffset(),
	}

	if conf.BlsSigner != nil {
		hs.sign = hs.signBls
		hs.tcombine = hs.tcombineBls
		hs.validateQC = hs.validateQCBls
		hs.verify = hs.verifyBls
		hs.createMsg = hs.createMsgBls
		hs.generateBitmap = hs.generateBitmapBls
	} else {
		hs.sign = hs.signEc
		hs.tcombine = hs.tcombineEc
		hs.validateQC = hs.validateQCEc
		hs.verify = hs.verifyEc
		hs.createMsg = hs.createMsgEc
		hs.generateBitmap = hs.generateBitmapEc
	}

	return hs
}

func (hs *HotStuff) Start() (err error) {

	swapped := atomic.CompareAndSwapInt32(&hs.status, 0, 1)
	if !swapped {
		err = fmt.Errorf("already started")
		return
	}

	err = hs.initConsensusState()
	if err != nil {
		return
	}
	hs.enterHeightView(hs.height, hs.view)
	util.GoFunc(&hs.wg, hs.loop)

	return
}

const consensusFile = "/consensus.dat"

func (hs *HotStuff) initConsensusState() (err error) {
	fullPath := hs.conf.WalPath + consensusFile
	if _, err = os.Stat(fullPath); os.IsNotExist(err) {
		err = nil
		hs.applyState(&ConsensusState{Height: hs.store.Height() + 1})
		return
	}

	data, err := ioutil.ReadFile(fullPath)
	if err != nil {
		hs.conf.Logger.Error("initConsensusState failed:%v", err)
		return
	}
	source := common.NewZeroCopySource(data)
	cs := &ConsensusState{}
	err = cs.Deserialize(source)
	if err != nil {
		hs.conf.Logger.Error("initConsensusState failed:%v", err)
		return
	}

	if cs.Height > hs.store.Height()+1 {
		err = fmt.Errorf("inconsistent state found")
		return
	}
	if cs.Height < hs.store.Height()+1 {
		cs = &ConsensusState{Height: hs.store.Height() + 1}
	}

	hs.applyState(cs)
	return
}

func (hs *HotStuff) applyState(cs *ConsensusState) {
	hs.height = cs.Height
	hs.view = cs.View
	hs.hasVoted = cs.HasVoted
	hs.candidateBlk = cs.CandidateBlk
	hs.lockQC = cs.LockQC
	hs.prepareState(hs.height)
}

func (hs *HotStuff) restoreConsensusState() {
	cs := ConsensusState{Height: hs.height, View: hs.view, CandidateBlk: hs.candidateBlk, LockQC: hs.lockQC, HasVoted: hs.hasVoted}

	sink := common.NewZeroCopySink(nil)
	cs.Serialize(sink)

	util.TryUntilSuccess(func() bool {
		err := ioutil.WriteFile(hs.conf.WalPath+consensusFile, sink.Bytes(), 0777)
		if err != nil {
			hs.conf.Logger.Error("restoreConsensusState failed:%v", err)
			return false
		}
		return true
	}, time.Second)

}

func (hs *HotStuff) Stop() (err error) {
	swapped := atomic.CompareAndSwapInt32(&hs.status, 1, 2)
	if !swapped {
		status := atomic.LoadInt32(&hs.status)
		switch status {
		case 2:
			err = fmt.Errorf("already stopped")
		case 0:
			err = fmt.Errorf("not started")
		default:
			err = fmt.Errorf("unknown error")
		}

		return
	}

	close(hs.proposeCh)
	hs.wg.Wait()
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
