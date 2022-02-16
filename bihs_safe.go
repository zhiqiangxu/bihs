package bihs

import (
	"bytes"
	"time"

	"github.com/zhiqiangxu/util"
)

func (hs *HotStuff) onRecvNewView(sender ID, idx int, msg *Msg) {
	qc := msg.Justify
	if qc != nil && (hs.lockQC == nil || qc.View > hs.lockQC.View) && qc.validate(hs) == nil {
		afterGotBlk := func() {
			hs.lockQC = qc
		}

		if hs.candidateBlk != nil && bytes.Equal(hs.candidateBlk.Hash(), qc.BlockHash) {
			afterGotBlk()
			return
		}

		hs.waitBlock = func(_ ID, blkMsg *Msg) {
			if blkMsg.Node.Blk == nil || !bytes.Equal(blkMsg.Node.Blk.Hash(), qc.BlockHash) {
				return
			}
			if err := hs.store.Validate(blkMsg.Node.Blk); err != nil {
				return
			}
			if !(hs.lockQC == nil || qc.View > hs.lockQC.View) {
				return
			}

			hs.candidateBlk = blkMsg.Node.Blk
			afterGotBlk()
		}

		hs.requestBlk(sender, qc.BlockHash)
	}
}

func (hs *HotStuff) onProposal(blk Block, qc *QC) {
	if blk == nil {
		return
	}
	if qc != nil {
		if !bytes.Equal(blk.Hash(), qc.BlockHash) {
			return
		}
	}
	if hs.hasVotedPrepare && !bytes.Equal(hs.candidateBlk.Hash(), blk.Hash()) {
		hs.conf.Logger.Infof("proposer %d tried to propose two different blocks, previous:%s current:%s", hs.idx, hs.candidateBlk.Hash(), blk.Hash())
		blk = hs.candidateBlk
	} else {
		hs.hasVotedPrepare = true
		hs.candidateBlk = blk
	}

	prepareMsg := hs.createMsg(MTPrepare, qc, &BlockOrHash{Blk: blk})
	hs.conf.Logger.Infof("proposer %d sent prepare %s for height:%d view:%d", hs.idx, prepareMsg.Hash(), hs.height, hs.view)
	hs.p2p.Broadcast(prepareMsg)

	hs.onRecvPrepareVote(hs.conf.ProposerID, hs.idx, prepareMsg)
}

func (hs *HotStuff) onRecvPrepare(sender ID, idx int, msg *Msg) {
	hs.conf.Logger.Infof("proposer %d received prepare %s from %d for height:%d view:%d", hs.idx, msg.Hash(), idx, hs.height, hs.view)
	if !hs.matchingMsg(msg, MTPrepare, hs.view) {
		hs.conf.Logger.Errorf("prepare not match, expect (%d, %d) got (%d, %d)", hs.height, hs.view, msg.Height, msg.View)
		return
	}

	if msg.Node.Blk == nil {
		hs.conf.Logger.Error("prepare with no Block")
		return
	}

	if msg.Node.Blk.Height() != msg.Height {
		hs.conf.Logger.Errorf("prepare block height(%d) != msg height(%d)", msg.Node.Blk.Height(), msg.Height)
		return
	}
	if err := hs.store.Validate(msg.Node.Blk); err != nil {
		hs.conf.Logger.Errorf("prepare block Validate failed:%v block hash:%d", err, msg.Node.Blk.Hash())
		return
	}

	switch {
	case msg.View == 0:
		if !msg.Node.Blk.Empty() && !bytes.Equal(hs.store.SelectLeader(msg.Node.Blk.Height(), 0), sender) {
			hs.conf.Logger.Errorf("proposer %s tried to propose when not in turn", sender)
			return
		}
	case msg.View > 0:
		if !msg.Node.Blk.Empty() && msg.Justify == nil {
			hs.conf.Logger.Errorf("proposer %s tried to relay propose with no qc", sender)
			return
		}
		if msg.Justify != nil {
			err := msg.Justify.validate(hs)
			if err != nil {
				hs.conf.Logger.Errorf("prepare Justify invalid:%v", err)
				return
			}
			if !bytes.Equal(msg.Justify.BlockHash, msg.Node.BlockHash()) {
				hs.conf.Logger.Errorf("prepare Justify doesn't match proposal, justify:%s, proposal:%s", msg.Justify.BlockHash, msg.Node.BlockHash())
				return
			}
		}
	}

	if !hs.hasVotedPrepare && hs.safeNode(msg.Node, msg.Justify) {
		hs.candidateBlk = msg.Node.Blk
		hs.hasVotedPrepare = true

		voteMsg := hs.voteMsg(MTPrepare, msg.BlockHash())
		hs.p2p.Send(sender, voteMsg)
	}

	hs.conf.Logger.Infof("proposer %d onRecvPrepare done", hs.idx)
}

func (hs *HotStuff) onRecvPrepareVote(sender ID, idx int, msg *Msg) {
	hs.conf.Logger.Infof("proposer %d received PrepareVote %s from %d for height:%d view:%d", hs.idx, msg.Hash(), idx, hs.height, hs.view)
	if !hs.matchingMsg(msg, MTPrepare, hs.view) {
		hs.conf.Logger.Error("prepare vote not match, expect (%d, %d) got (%d, %d)", hs.height, hs.view, msg.Height, msg.View)
		return
	}

	if !bytes.Equal(msg.BlockHash(), hs.candidateBlk.Hash()) {
		return
	}

	if _, ok := hs.votes1[idx]; ok {
		return
	}

	hs.voted[idx] = sender
	hs.votes1[idx] = msg.PartialSig

	if len(hs.votes1) == int(quorum(hs.store.ValidatorCount(msg.Height))) {
		hs.lockQC = hs.createQC(msg, hs.votes1)
		precommitMsg := hs.createMsg(MTPreCommit, hs.lockQC, nil)
		hs.conf.Logger.Infof("proposer %d sent precommit %s", hs.idx, precommitMsg.Hash())
		hs.p2p.Broadcast(precommitMsg)
		hs.onRecvPrecommitVote(hs.conf.ProposerID, hs.idx, precommitMsg)
	}
}

func (hs *HotStuff) onRecvPrecommit(sender ID, idx int, msg *Msg) {
	hs.conf.Logger.Infof("proposer %d received Precommit %s from %d for height:%d view:%d", hs.idx, msg.Hash(), idx, hs.height, hs.view)
	if !hs.matchingMsg(msg, MTPreCommit, hs.view) {
		hs.conf.Logger.Error("precommit not match, expect (%d, %d) got (%d, %d)", hs.height, hs.view, msg.Height, msg.View)
		return
	}
	if msg.Justify == nil {
		return
	}
	if !hs.matchingQC(msg.Justify, MTPrepare, hs.view) {
		return
	}

	err := msg.Justify.validate(hs)
	if err != nil {
		return
	}

	afterGotBlk := func() {
		voteMsg := hs.voteMsg(MTPreCommit, msg.Justify.BlockHash)
		hs.lockQC = msg.Justify
		hs.p2p.Send(sender, voteMsg)
		hs.conf.Logger.Infof("proposer %d onRecvPrecommit done", hs.idx)
	}
	if hs.candidateBlk == nil || !bytes.Equal(hs.candidateBlk.Hash(), msg.Justify.BlockHash) {
		hs.candidateBlk = nil
		hs.waitBlock = func(leader ID, blkMsg *Msg) {
			if blkMsg.Node.Blk == nil {
				return
			}
			if !bytes.Equal(msg.Justify.BlockHash, blkMsg.Node.Blk.Hash()) {
				return
			}
			if err := hs.store.Validate(blkMsg.Node.Blk); err != nil {
				return
			}
			hs.candidateBlk = blkMsg.Node.Blk
			afterGotBlk()
		}

		hs.requestBlk(sender, msg.Justify.BlockHash)
		return
	}

	afterGotBlk()

}

func (hs *HotStuff) requestBlk(from ID, blkHash []byte) {
	hs.p2p.Send(from, hs.createMsg(MTReqBlock, nil, &BlockOrHash{Hash: blkHash}))
}

func (hs *HotStuff) onReqBlock(sender ID, idx int, msg *Msg) {

	if bytes.Equal(msg.Node.Hash, hs.candidateBlk.Hash()) {
		hs.p2p.Send(sender, hs.createMsg(MTRespBlock, nil, &BlockOrHash{Blk: hs.candidateBlk}))
	}

}

func (hs *HotStuff) onRecvPrecommitVote(sender ID, idx int, msg *Msg) {
	hs.conf.Logger.Infof("proposer %d received PrecommitVote %s from %d for height:%d view:%d", hs.idx, msg.Hash(), idx, hs.height, hs.view)
	if !hs.matchingMsg(msg, MTPreCommit, hs.view) {
		hs.conf.Logger.Error("precommit vote not match, expect (%d, %d) got (%d, %d)", hs.height, hs.view, msg.Height, msg.View)
		return
	}
	if !bytes.Equal(msg.BlockHash(), hs.candidateBlk.Hash()) {
		return
	}

	voter, err := hs.verify(msg)
	if err != nil {
		return
	}
	if _, ok := hs.votes2[idx]; ok {
		return
	}

	hs.voted[idx] = voter
	hs.votes2[idx] = msg.PartialSig

	if len(hs.votes2) == int(quorum(hs.store.ValidatorCount(msg.Height))) {
		commitQC := hs.createQC(msg, hs.votes2)
		commitMsg := hs.createMsg(MTCommit, commitQC, nil)
		hs.conf.Logger.Infof("proposer %d sent commit %s", hs.idx, commitMsg.Hash())
		hs.p2p.Broadcast(commitMsg)

		hs.onRecvCommit(hs.conf.ProposerID, hs.idx, commitMsg)
	}
}

func (hs *HotStuff) onRecvCommit(sender ID, idx int, msg *Msg) {
	hs.conf.Logger.Infof("proposer %d received Commit %s from %d for height:%d view:%d", hs.idx, msg.Hash(), idx, hs.height, hs.view)
	if !hs.matchingMsg(msg, MTCommit, hs.view) {
		hs.conf.Logger.Error("commit not match, expect (%d, %d) got (%d, %d)", hs.height, hs.view, msg.Height, msg.View)
		return
	}
	if msg.Justify == nil {
		return
	}
	if !hs.matchingQC(msg.Justify, MTPreCommit, hs.view) {
		return
	}

	err := msg.Justify.validate(hs)
	if err != nil {
		return
	}

	afterGotBlk := func() {
		util.TryUntilSuccess(func() bool {
			return hs.applyBlock(hs.candidateBlk, msg.Justify) == nil
		}, time.Second)
		hs.conf.Logger.Infof("proposer %d onRecvCommit done", hs.idx)
	}
	if hs.candidateBlk == nil || !bytes.Equal(hs.candidateBlk.Hash(), msg.Justify.BlockHash) {
		hs.candidateBlk = nil
		hs.waitBlock = func(leader ID, blkMsg *Msg) {
			if blkMsg.Node.Blk == nil {
				return
			}
			if !bytes.Equal(msg.Justify.BlockHash, blkMsg.Node.Blk.Hash()) {
				return
			}
			if err := hs.store.Validate(blkMsg.Node.Blk); err != nil {
				return
			}
			hs.candidateBlk = blkMsg.Node.Blk
			afterGotBlk()
		}

		hs.requestBlk(sender, msg.Justify.BlockHash)
		return
	}

	afterGotBlk()
}

func (hs *HotStuff) createQC(msg *Msg, votes map[int][]byte) *QC {
	return &QC{Type: msg.Type, Height: msg.Height, View: msg.View, BlockHash: msg.BlockHash(), Sigs: hs.tcombine(msg, votes), Bitmap: hs.generateBitmap(votes)}
}

func (hs *HotStuff) generateBitmapEc(votes map[int][]byte) []byte {
	return nil
}

func (hs *HotStuff) generateBitmapBls(votes map[int][]byte) []byte {

	bitmapSize := len(hs.voted) / 8
	if len(hs.voted)%8 != 0 {
		bitmapSize++
	}
	bitmap := make([]byte, bitmapSize)
	for i := range votes {
		bitmap[i/8] |= 1 << (uint16(i) % 8)
	}

	return bitmap
}

func (hs *HotStuff) safeNode(node *BlockOrHash, qc *QC) bool {

	if hs.lockQC == nil {
		return true
	}

	if bytes.Equal(hs.lockQC.BlockHash, node.BlockHash()) {
		return true
	}

	if qc == nil {
		return false
	}

	if hs.lockQC.View < qc.View {
		if !bytes.Equal(node.BlockHash(), qc.BlockHash) {
			return false
		}
		return true
	}

	return false
}
