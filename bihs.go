package bihs

import (
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
	ConsensusState
	status         int32
	proposeCh      chan Block
	closeCh        chan struct{}
	heightChangeCh chan struct{}
	p2p            P2P
	store          StateDB
	conf           Config
	relayTimer     *time.Timer
	nvInterrupt    *time.Timer
	waiter         *wm.Offset
	wg             sync.WaitGroup
	waitBlock      func(ID, *Msg)
	lastBlockTime  uint64

	createMsg      func(mt MsgType, justify *QC, node *BlockOrHash) *Msg
	sign           func([]byte) []byte
	tcombine       func(msg *Msg, votes map[int][]byte) []byte
	validateQC     func(qc *QC) error
	verify         func(msg *Msg) (voter ID, err error)
	generateBitmap func(votes map[int][]byte) []byte
}

func New(store StateDB, p2p P2P, conf Config) *HotStuff {

	err := conf.validate()
	if err != nil {
		panic(fmt.Sprintf("Config.Validate failed:%v", err))
	}

	hs := &HotStuff{
		proposeCh:      make(chan Block),
		closeCh:        make(chan struct{}),
		heightChangeCh: make(chan struct{}, 1),
		p2p:            p2p,
		store:          store,
		conf:           conf,
		waiter:         wm.NewOffset(),
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

	hs.store.SubscribeHeightChange(hs)
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

	fullPath := hs.conf.DataDir + consensusFile
	if _, err = os.Stat(fullPath); os.IsNotExist(err) {
		err = nil
		hs.advanceToHeight(hs.store.Height()+1, hs)
		hs.conf.Logger.Infof("initConsensusState height=%d view=0", hs.store.Height()+1)
		return
	}

	data, err := ioutil.ReadFile(fullPath)
	if err != nil {
		hs.conf.Logger.Error("initConsensusState failed:%v", err)
		return
	}

	cs := ConsensusState{}
	err = cs.Deserialize(common.NewZeroCopySource(data))
	if err != nil {
		hs.conf.Logger.Error("initConsensusState cs.Deserialize failed:%v", err)
		return
	}

	if cs.height > hs.store.Height()+1 {
		err = fmt.Errorf("inconsistent state found")
		return
	}
	if cs.height < hs.store.Height()+1 {
		hs.advanceToHeight(hs.store.Height()+1, hs)
		hs.conf.Logger.Infof("initConsensusState height=%d view=0", hs.store.Height()+1)
		return
	}
	hs.ConsensusState = cs
	hs.conf.Logger.Infof("initConsensusState height=%d view=%d", hs.height, hs.view)

	return
}

func (hs *HotStuff) restoreConsensusState() {
	cs := hs.ConsensusState

	sink := common.NewZeroCopySink(nil)
	cs.Serialize(sink)
	csBytes := sink.Bytes()

	util.TryUntilSuccess(func() bool {
		err := ioutil.WriteFile(hs.conf.DataDir+consensusFile, csBytes, 0777)
		if err != nil {
			hs.conf.Logger.Error("restoreConsensusState failed:%v", err)
			return false
		}
		hs.conf.Logger.Infof("consensus state stored at %s #len %d", hs.conf.DataDir+consensusFile, len(csBytes))
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

	close(hs.closeCh)
	hs.store.UnSubscribeHeightChange(hs)
	hs.wg.Wait()
	return
}

func (hs *HotStuff) Propose(ctx context.Context, blk Block) (err error) {
	err = hs.store.Validate(blk)
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
		case <-hs.closeCh:
		}

	}()

	return
}

func (hs *HotStuff) Wait(ctx context.Context, height uint64) error {
	return hs.waiter.Wait(ctx, int64(height))
}

func (hs *HotStuff) ConsensusHeight() uint64 {
	return atomic.LoadUint64(&hs.height)
}

func (hs *HotStuff) ConsensusView() uint64 {
	return atomic.LoadUint64(&hs.view)
}
