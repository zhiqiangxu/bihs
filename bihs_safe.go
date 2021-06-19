package bihs

import (
	"bytes"
	"time"

	"github.com/zhiqiangxu/util"
)

func (hs *HotStuff) onRecvNewView(sender ID, msg *Msg) {
	qc := msg.Justify
	if qc != nil && (hs.lockQC == nil || qc.View > hs.lockQC.View) && qc.Validate(hs) == nil {
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
	hs.candidateBlk = blk

	prepareMsg := hs.createMsg(MTPrepare, qc, &BlockOrHash{Blk: blk})
	hs.conf.Logger.Infof("proposer %s sent Proposal1 %v", string(hs.conf.ProposerID), prepareMsg)
	hs.p2p.Broadcast(prepareMsg)

	hs.onRecvVote1(hs.conf.ProposerID, hs.idx, prepareMsg)
}

func (hs *HotStuff) onRecvProposal1(sender ID, msg *Msg) {
	hs.conf.Logger.Infof("proposer %s received Proposal1 %v", string(hs.conf.ProposerID), msg)
	if !hs.matchingMsg(msg, MTPrepare, hs.view) {
		hs.conf.Logger.Error("Proposal1 not match")
		return
	}

	if msg.Node.Blk == nil {
		return
	}
	if msg.Justify != nil {
		err := msg.Justify.Validate(hs)
		if err != nil {
			hs.conf.Logger.Errorf("Proposal1 Justify invalid:%v", err)
			return
		}
	}

	err := msg.Node.Blk.Validate()
	if err != nil {
		hs.conf.Logger.Errorf("Proposal1 Node invalid:%v", err)
		return
	}

	if !hs.store.IsBlockProposableBy(msg.Node.Blk, sender) {
		hs.conf.Logger.Errorf("proposer %s not IsBlockProposableBy", string(hs.conf.ProposerID))
		return
	}
	if !hs.hasVoted && hs.safeNode(msg.Node, msg.Justify) {
		hs.candidateBlk = msg.Node.Blk
		hs.hasVoted = true

		voteMsg := hs.voteMsg(MTPrepare, msg.BlockHash())
		hs.p2p.Send(sender, voteMsg)
	}

	hs.conf.Logger.Infof("proposer %v onRecvProposal1 done", string(hs.conf.ProposerID))
}

func (hs *HotStuff) onRecvVote1(sender ID, idx int, msg *Msg) {
	hs.conf.Logger.Infof("proposer %s received Vote1 %v", string(hs.conf.ProposerID), msg)
	if !hs.matchingMsg(msg, MTPrepare, hs.view) {
		return
	}

	if !bytes.Equal(msg.BlockHash(), hs.candidateBlk.Hash()) {
		return
	}

	voter, err := hs.verify(msg)
	if err != nil {
		return
	}
	if _, ok := hs.votes1[idx]; ok {
		return
	}

	hs.voted[idx] = voter
	hs.votes1[idx] = msg.PartialSig

	if len(hs.votes1) == int(quorum(hs.conf.ValidatorCount(msg.Height))) {
		hs.lockQC = hs.createQC(msg, hs.votes1)
		commitMsg := hs.createMsg(MTCommit, hs.lockQC, nil)
		hs.conf.Logger.Infof("proposer %s sent Proposal2 %v", string(hs.conf.ProposerID), commitMsg)
		hs.p2p.Broadcast(commitMsg)
		hs.onRecvVote2(hs.conf.ProposerID, hs.idx, commitMsg)
	}
}

func (hs *HotStuff) onRecvProposal2(sender ID, msg *Msg) {
	hs.conf.Logger.Infof("proposer %s received Proposal2 %v", string(hs.conf.ProposerID), msg)
	if !hs.matchingMsg(msg, MTCommit, hs.view) {
		return
	}
	if msg.Justify == nil {
		return
	}
	if !hs.matchingQC(msg.Justify, MTPrepare, hs.view) {
		return
	}

	err := msg.Justify.Validate(hs)
	if err != nil {
		return
	}

	afterGotBlk := func() {
		voteMsg := hs.voteMsg(MTCommit, msg.Justify.BlockHash)
		hs.lockQC = msg.Justify
		hs.p2p.Send(sender, voteMsg)
		hs.conf.Logger.Infof("proposer %v onRecvProposal2 done", string(hs.conf.ProposerID))
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

func (hs *HotStuff) onReqBlock(sender ID, msg *Msg) {

	if bytes.Equal(msg.Node.Hash, hs.candidateBlk.Hash()) {
		hs.p2p.Send(sender, hs.createMsg(MTRespBlock, nil, &BlockOrHash{Blk: hs.candidateBlk}))
	}

}

func (hs *HotStuff) onRecvVote2(sender ID, idx int, msg *Msg) {
	hs.conf.Logger.Infof("proposer %s received Vote2 %v", string(hs.conf.ProposerID), msg)
	if !hs.matchingMsg(msg, MTCommit, hs.view) {
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

	if len(hs.votes2) == int(quorum(hs.conf.ValidatorCount(msg.Height))) {
		commitQC := hs.createQC(msg, hs.votes2)
		hs.conf.Logger.Infof("proposer %s sent Proposal3 %v", string(hs.conf.ProposerID), commitQC)
		commitMsg := hs.createMsg(MTDecide, commitQC, nil)
		hs.p2p.Broadcast(commitMsg)

		hs.onRecvProposal3(hs.conf.ProposerID, commitMsg)
	}
}

func (hs *HotStuff) onRecvProposal3(sender ID, msg *Msg) {
	hs.conf.Logger.Infof("proposer %s received Proposal3 %v", string(hs.conf.ProposerID), msg)
	if !hs.matchingMsg(msg, MTDecide, hs.view) {
		return
	}
	if msg.Justify == nil {
		return
	}
	if !hs.matchingQC(msg.Justify, MTCommit, hs.view) {
		return
	}

	err := msg.Justify.Validate(hs)
	if err != nil {
		return
	}

	afterGotBlk := func() {
		util.TryUntilSuccess(func() bool {
			return hs.applyBlock(hs.candidateBlk, msg.Justify) == nil
		}, time.Second)
		hs.conf.Logger.Infof("proposer %v onRecvProposal3 done", string(hs.conf.ProposerID))
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
