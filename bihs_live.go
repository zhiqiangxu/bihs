package bihs

import (
	"fmt"
	"math"
	"time"
)

func (hs *HotStuff) loop() {

	var relayTimerCh <-chan time.Time

	for {

		if hs.relayTimer != nil {
			relayTimerCh = hs.relayTimer.C
		} else {
			relayTimerCh = nil
		}

		select {
		case proposal := <-hs.proposeCh:

			if !hs.isLeader(hs.height, hs.view) {
				hs.conf.Logger.Errorf("proposer(%d) tried to propose when not in turn, height(%d) view(%d)", hs.idx, hs.height, hs.view)
				continue
			}
			if !hs.conf.Promiscuous && hs.view != 0 {
				hs.conf.Logger.Errorf("proposer(%d) tried to propose when not in view 0 in bivalent mode", hs.idx)
				continue
			}
			switch {
			case proposal.Height() < hs.height:
				hs.conf.Logger.Errorf("proposer(%d) proposal height decrease, proposal.height=%d, hs.height=%d", hs.idx, proposal.Height(), hs.height)
				continue
			case proposal.Height() == hs.height:
				if hs.hasVotedPrepare {
					hs.conf.Logger.Errorf("proposer(%d) has voted prepare in the current height(%d) and view(%d), last:%v, current:%v", hs.idx, hs.height, hs.view, hs.candidateBlk.Hash(), proposal.Hash())
					continue
				}
			case proposal.Height() > hs.height:
				hs.enterHeightView(proposal.Height(), 0)
			}

			hs.onProposal(proposal, nil)

		case <-hs.closeCh:
			hs.restoreConsensusState()
			return
		case msg, ok := <-hs.p2p.MsgCh():
			if !ok {
				hs.restoreConsensusState()
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

			idx := hs.store.ValidatorIndex(hs.height, sender)
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
					hs.onRecvPrepareVote(sender, idx, msg)
				} else {
					hs.onRecvPrepare(sender, idx, msg)
				}
			case MTPreCommit:
				if hs.isLeader(msgHeight, msg.View) {
					hs.onRecvPrecommitVote(sender, idx, msg)
				} else {
					hs.onRecvPrecommit(sender, idx, msg)
				}
			case MTCommit:
				if hs.isLeader(msgHeight, msg.View) {
					hs.conf.Logger.Errorf("got MTCommit msg as leader")
				} else {
					hs.onRecvCommit(sender, idx, msg)
				}
			case MTNewView:
				if hs.isLeader(msgHeight, msg.View+1) {
					hs.onRecvNewView(sender, idx, msg)
				}
			case MTReqBlock:
				hs.onReqBlock(sender, idx, msg)
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
		case <-relayTimerCh:
			hs.relayPropose()
		case <-hs.heightChangeCh:
			if hs.store.Height() >= hs.height {
				hs.enterHeightView(hs.store.Height()+1, 0)
			}
		}
	}
}

func (hs *HotStuff) HeightChanged() {
	select {
	case hs.heightChangeCh <- struct{}{}:
	default:
	}
}

func (hs *HotStuff) onNextSyncView() {
	leader := hs.store.SelectLeader(hs.height, hs.view+1)
	if hs.lockQC != nil {
		hs.p2p.Send(leader, hs.createMsg(MTNewView, hs.lockQC, nil))
	}
}

func (hs *HotStuff) applyBlock(blk Block, commitQC *QC) (err error) {

	err = hs.store.StoreBlock(blk, commitQC)
	if err != nil {
		hs.conf.Logger.Errorf("applyBlock failed:%v", err)
		return
	}

	if timeBlk, ok := blk.(BlockWithTime); ok {
		// adjust time skew
		expectedTime := hs.lastBlockTime*uint64(time.Millisecond) + uint64(hs.conf.BlockInterval)
		now := uint64(time.Now().UnixMilli()) * uint64(time.Millisecond)
		if now < expectedTime {
			hs.conf.Logger.Infof("adjust time skew, sleep %v", time.Duration(expectedTime-now))
			time.Sleep(time.Duration(expectedTime - now))
		}
		hs.lastBlockTime = timeBlk.TimeMil()
	}
	hs.enterHeightView(blk.Height()+1, 0)

	return
}

func (hs *HotStuff) enterHeightView(height, view uint64) {
	if !(height > hs.height || height == hs.height && view >= hs.view) {
		panic(fmt.Sprintf("height view invariance is broken, height:%d hs.height:%d view:%d, hs.view:%d", height, hs.height, view, hs.view))
	}
	hs.conf.Logger.Infof("proposer %d enter height %d view %d", hs.idx, height, view)

	if hs.nvInterrupt != nil {
		hs.nvInterrupt.Stop()
	}

	timeout := hs.calcViewTimeout()
	hs.nvInterrupt = time.NewTimer(timeout)
	hs.waitBlock = nil

	if hs.relayTimer != nil {
		hs.relayTimer.Stop()
		hs.relayTimer = nil
	}

	isLeader := hs.isLeader(height, view)

	switch {
	case height > hs.height:
		hs.advanceToHeight(height, hs)
		hs.waiter.Done(int64(height - 1))
	case view > hs.view:
		hs.advanceToView(view)
	default:
		// the same height and view
		// in this case, node has just restarted

		if view > 0 && isLeader {
			hs.relayPropose()
			return
		}
	}

	if isLeader {
		hs.relayTimer = time.NewTimer(timeout / 3)
	}
}

func (hs *HotStuff) relayPropose() {
	if hs.lockQC != nil {
		hs.onProposal(hs.candidateBlk, hs.lockQC)
		hs.conf.Logger.Infof("proposer %d relayPropose locked", hs.idx)
		return
	}

	blk, err := hs.store.MakeBlock(hs.height, !hs.conf.Promiscuous && hs.view > 0)
	if err != nil {
		hs.conf.Logger.Errorf("proposer %d EmptyBlock failed:%v", hs.idx, err)
		return
	}
	hs.onProposal(blk, nil)
	hs.conf.Logger.Infof("proposer %d relayPropose empty", hs.idx)

}

func (hs *HotStuff) calcViewTimeout() time.Duration {
	return hs.conf.BlockInterval * time.Duration(math.Pow(2, float64(hs.view)))
}
