package bihs

import (
	"encoding/hex"
	"fmt"
	"math"
	"sync/atomic"
	"time"
)

func (hs *HotStuff) loop() {

	syncTicker := time.NewTicker(hs.conf.SyncCheckInterval)
	defer syncTicker.Stop()

	var proposeRelayTimerCh <-chan time.Time

	for {

		if hs.proposeRelayTimer != nil {
			proposeRelayTimerCh = hs.proposeRelayTimer.C
		} else {
			proposeRelayTimerCh = nil
		}

		select {
		case proposal, ok := <-hs.proposeCh:
			if !ok {
				hs.restoreConsensusState()
				return
			}

			if !hs.isLeader(hs.height, hs.view) {
				hs.conf.Logger.Errorf("proposer(%s) not in turn, height(%d) view(%d)", string(hs.conf.ProposerID), hs.height, hs.view)
				continue
			}
			if hs.view != 0 {
				hs.conf.Logger.Errorf("proposal(%s) Propose not in view 0", string(hs.conf.ProposerID))
				continue
			}
			hs.onProposal(proposal, nil)

		case msg, ok := <-hs.p2p.MsgCh():
			if !ok {
				return
			}

			if msg.Node == nil && msg.Justify == nil {
				// ignore msg with no info
				continue
			}

			sender, err := hs.verify(msg)
			if err != nil {
				continue
			}

			idx := hs.conf.ValidatorIndex(hs.height, sender)
			if idx < 0 {
				continue
			}

			msgHeight := msg.Height
			if msgHeight != hs.height {
				// assume some external fetcher exists to sync
				if hs.conf.Syncer != nil {
					hs.conf.Syncer.SyncWithPeer(sender, msgHeight)
				}
				continue
			}

			switch msg.Type {
			case MTPrepare:
				if hs.isLeader(msgHeight, msg.View) {
					hs.onRecvVote1(sender, idx, msg)
				} else {
					hs.onRecvProposal1(sender, msg)
				}
			case MTCommit:
				if hs.isLeader(msgHeight, msg.View) {
					hs.onRecvVote2(sender, idx, msg)
				} else {
					hs.onRecvProposal2(sender, msg)
				}
			case MTDecide:
				if hs.isLeader(msgHeight, msg.View) {
					hs.conf.Logger.Errorf("got MTCommit msg as leader")
				} else {
					hs.onRecvProposal3(sender, msg)
				}
			case MTNewView:
				if hs.isLeader(msgHeight, msg.View+1) {
					hs.onRecvNewView(sender, msg)
				}
			case MTReqBlock:
				hs.onReqBlock(sender, msg)
			case MTRespBlock:
				if hs.waitBlock != nil {
					hs.waitBlock(sender, msg)
				}
			default:
				hs.conf.Logger.Errorf("invalid msg type:%d, msg:%v", msg.Type, msg)
			}
		case <-hs.nvInterrupt.C:
			hs.onNextSyncView()
			hs.enterHeightView(hs.height, hs.view+1)
		case <-proposeRelayTimerCh:
			hs.relayPropose()
		case <-syncTicker.C:
			if hs.store.Height() >= hs.height {
				hs.enterHeightView(hs.store.Height()+1, 0)
			}
		}

	}
}

func (hs *HotStuff) onNextSyncView() {
	leader := hs.conf.SelectLeader(hs.height, hs.view+1)
	if hs.lockQC != nil {
		hs.p2p.Send(leader, hs.createMsg(MTNewView, hs.lockQC, nil))
	}
}

func (hs *HotStuff) applyBlock(blk Block, commitQC *QC) (err error) {

	err = hs.store.StoreBlock(blk, hs.lockQC, commitQC)
	if err != nil {
		hs.conf.Logger.Errorf("applyBlock failed:%v", err)
		return
	}

	hs.enterHeightView(blk.Height()+1, 0)

	return
}

func (hs *HotStuff) enterHeightView(height, view uint64) {
	hs.conf.Logger.Infof("proposer %s enter height %d view %d", string(hs.conf.ProposerID), height, view)

	if hs.nvInterrupt != nil {
		hs.nvInterrupt.Stop()
	}
	if height > hs.height {
		hs.lockQC = nil
		hs.candidateBlk = nil
		hs.waiter.Done(int64(hs.height))
		hs.prepareState(height)
		hs.hasVoted = false
	} else if view > hs.view {
		hs.hasVoted = false
	}

	atomic.StoreUint64(&hs.height, height)
	atomic.StoreUint64(&hs.view, view)
	timeout := hs.calcViewTimeout()
	hs.nvInterrupt = time.NewTimer(timeout)
	hs.votes1 = make(map[int][]byte)
	hs.votes2 = make(map[int][]byte)
	hs.waitBlock = nil

	if hs.proposeRelayTimer != nil {
		hs.proposeRelayTimer.Stop()
		hs.proposeRelayTimer = nil
	}
	isLeader := hs.isLeader(height, view)
	if view > 0 {
		if isLeader {
			hs.proposeRelayTimer = time.NewTimer(timeout / 5)
		}
	}
}

func (hs *HotStuff) prepareState(height uint64) {
	hs.idx = hs.conf.ValidatorIndex(height, hs.conf.ProposerID)
	count := int(hs.conf.ValidatorCount(height))
	if hs.idx < 0 || hs.idx >= count {
		panic(fmt.Sprintf("invalid index(%d) for proposer:%s", hs.idx, hex.EncodeToString(hs.conf.ProposerID)))
	}
	hs.voted = make([]ID, count)
}

func (hs *HotStuff) relayPropose() {
	if hs.lockQC != nil {
		hs.onProposal(hs.candidateBlk, hs.lockQC)
	} else {
		hs.onProposal(hs.conf.EmptyBlock(hs.height), nil)
		hs.conf.Logger.Infof("proposer %s relayPropose empty", string(hs.conf.ProposerID))
	}
}

func (hs *HotStuff) calcViewTimeout() time.Duration {
	return hs.conf.BlockInterval * time.Duration(math.Pow(2, float64(hs.view)))
}
