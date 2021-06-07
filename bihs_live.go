package bihs

import (
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

			msgHeight := msg.Height
			if msgHeight != hs.height {
				continue
			}

			sender, err := hs.conf.Verify(msg.Hash(), msg.PartialSig)
			if err != nil {
				continue
			}

			if !hs.conf.IsValidator(msgHeight, sender) {
				continue
			}

			switch msg.Type {
			case MTPrepare:
				if hs.isLeader(msgHeight, msg.View) {
					hs.onRecvVote1(sender, msg)
				} else {
					hs.onRecvProposal1(sender, msg)
				}
			case MTPreCommit:
				if hs.isLeader(msgHeight, msg.View) {
					hs.onRecvVote2(sender, msg)
				} else {
					hs.onRecvProposal2(sender, msg)
				}
			case MTCommit:
				if hs.isLeader(msgHeight, msg.View) {
					hs.conf.Logger.Errorf("got MTCommit msg as leader")
				} else {
					hs.onRecvProposal3(sender, msg)
				}
			case MTNewView:
				if hs.isLeader(msgHeight, msg.View+1) {
					hs.onRecvNewView(sender, msg)
				}
			default:
				hs.conf.Logger.Errorf("invalid msg type:%d, msg:%v", msg.Type, msg)
			}
		case <-hs.nvInterrupt.C:
			hs.onNextSyncView()
			hs.ngCount++
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
	hs.p2p.Send(leader, hs.createMsg(MTNewView, &Node{Justify: hs.lockQC}))
}

func (hs *HotStuff) applyBlock(blk Block, lockQC, commitQC *QC) (err error) {

	err = hs.store.StoreBlock(blk, lockQC, commitQC)
	if err != nil {
		return
	}

	hs.enterHeightView(blk.Height()+1, 0)

	return
}

func (hs *HotStuff) enterHeightView(height, view uint64) {
	if hs.nvInterrupt != nil {
		hs.nvInterrupt.Stop()
	}
	if height > hs.height {
		hs.store.ClearVoted()
		hs.ngCount = 0
		hs.waiter.Done(int64(hs.height))
	}
	timeout := hs.calcViewTimeout()
	hs.nvInterrupt = time.NewTimer(timeout)

	atomic.StoreUint64(&hs.height, height)
	atomic.StoreUint64(&hs.view, view)
	hs.votes1 = make(map[string][]byte)
	hs.votes2 = make(map[string][]byte)
	hs.lockQC = nil
	hs.commitQC = nil
	hs.prepareMsg = nil
	hs.preCommitMsg = nil

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

func (hs *HotStuff) relayPropose() {
	if hs.lockQC != nil {
		hs.onProposal(nil, hs.lockQC)
	} else {
		hs.onProposal(hs.conf.EmptyBlock(hs.height), nil)
		hs.conf.Logger.Infof("proposer %s relayPropose empty", string(hs.conf.ProposerID))
	}
}

func (hs *HotStuff) calcViewTimeout() time.Duration {
	return hs.conf.BlockInterval * time.Duration(math.Pow(2, float64(hs.ngCount)))
}
