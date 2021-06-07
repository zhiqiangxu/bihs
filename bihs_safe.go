package bihs

import (
	"bytes"
	"time"

	"github.com/zhiqiangxu/util"
)

func (hs *HotStuff) onRecvNewView(sender ID, msg *Msg) {
	qc := msg.Node.Justify
	if qc != nil && qc.Validate(hs) == nil && (hs.lockQC == nil || qc.View > hs.lockQC.View) {
		hs.lockQC = qc
	}
}

func (hs *HotStuff) onProposal(proposal Block, qc *QC) {
	if (proposal == nil && qc == nil) || (proposal != nil && qc != nil) {
		return
	}

	newNode := &Node{Blk: proposal, Justify: qc}

	hs.prepareMsg = hs.createMsg(MTPrepare, newNode)
	hs.conf.Logger.Infof("proposer %s sent Proposal1 %v", string(hs.conf.ProposerID), hs.prepareMsg)
	hs.p2p.Broadcast(hs.prepareMsg)

	hs.onRecvVote1(hs.conf.ProposerID, hs.prepareMsg)
}

func (hs *HotStuff) onRecvProposal1(sender ID, msg *Msg) {
	hs.conf.Logger.Infof("proposer %s received Proposal1 %v", string(hs.conf.ProposerID), msg)
	if !hs.matchingMsg(msg, MTPrepare, hs.view) {
		hs.conf.Logger.Error("Proposal1 not match")
		return
	}

	newNode := msg.Node
	err := newNode.Validate(hs)
	if err != nil {
		hs.conf.Logger.Errorf("Proposal1 Node invalid:%v", err)
		return
	}
	blk := newNode.Block()
	if !hs.store.IsBlockProposableBy(blk, sender) {
		hs.conf.Logger.Errorf("proposer %s not IsBlockProposableBy", string(hs.conf.ProposerID))
		return
	}

	if int64(msg.View) > hs.store.GetVoted() && hs.safeNode(newNode) {
		util.TryUntilSuccess(func() bool {
			err := hs.store.StoreVoted(msg.View)
			// TODO log error
			if err != nil {
				return false
			}

			voteMsg := hs.voteMsg(MTPrepare, newNode)
			hs.p2p.Send(hs.conf.SelectLeader(msg.Node.Height(), msg.View), voteMsg)
			return true
		}, time.Second)

	}

	hs.conf.Logger.Infof("proposer %v onRecvProposal1 done", string(hs.conf.ProposerID))
}

func (hs *HotStuff) onRecvVote1(sender ID, msg *Msg) {
	hs.conf.Logger.Infof("proposer %s received Vote1 %v", string(hs.conf.ProposerID), msg)
	if !hs.matchingMsg(msg, MTPrepare, hs.view) {
		return
	}

	if !bytes.Equal(msg.Hash(), hs.prepareMsg.Hash()) {
		return
	}

	voter, err := hs.conf.Verify(msg.Hash(), msg.PartialSig)
	if err != nil {
		return
	}
	if _, ok := hs.votes1[string(voter)]; ok {
		return
	}

	hs.votes1[string(voter)] = msg.PartialSig

	if len(hs.votes1) == int(quorum(hs.conf.ValidatorCount(msg.Node.Height()))) {
		hs.lockQC = hs.createQC(msg, hs.votes1)
		hs.preCommitMsg = hs.createMsg(MTPreCommit, &Node{Justify: hs.lockQC})
		hs.conf.Logger.Infof("proposer %s sent Proposal2 %v", string(hs.conf.ProposerID), hs.preCommitMsg)
		hs.p2p.Broadcast(hs.preCommitMsg)
		hs.onRecvVote2(hs.conf.ProposerID, hs.preCommitMsg)
	}
}

func (hs *HotStuff) onRecvProposal2(sender ID, msg *Msg) {
	hs.conf.Logger.Infof("proposer %s received Proposal2 %v", string(hs.conf.ProposerID), msg)
	if !hs.matchingMsg(msg, MTPreCommit, hs.view) {
		return
	}
	if msg.Node.Justify == nil {
		return
	}
	if !hs.matchingQC(msg.Node.Justify, MTPrepare, hs.view) {
		return
	}

	newNode := msg.Node
	if newNode.Justify == nil || newNode.Justify.Type != MTPrepare {
		return
	}
	err := newNode.Justify.Validate(hs)
	if err != nil {
		return
	}
	voteMsg := hs.voteMsg(MTPreCommit, newNode)
	hs.lockQC = newNode.Justify
	hs.p2p.Send(hs.conf.SelectLeader(msg.Node.Height(), msg.View), voteMsg)
}

func (hs *HotStuff) onRecvVote2(sender ID, msg *Msg) {
	hs.conf.Logger.Infof("proposer %s received Vote2 %v", string(hs.conf.ProposerID), msg)
	if !hs.matchingMsg(msg, MTPreCommit, hs.view) {
		return
	}
	if !bytes.Equal(msg.Hash(), hs.preCommitMsg.Hash()) {
		return
	}

	voter, err := hs.conf.Verify(msg.Hash(), msg.PartialSig)
	if err != nil {
		return
	}
	if _, ok := hs.votes2[string(voter)]; ok {
		return
	}

	hs.votes2[string(voter)] = msg.PartialSig

	if len(hs.votes2) == int(quorum(hs.conf.ValidatorCount(msg.Node.Height()))) {
		hs.commitQC = hs.createQC(msg, hs.votes2)
		hs.conf.Logger.Infof("proposer %s sent Proposal3 %v", string(hs.conf.ProposerID), hs.commitQC)
		commitMsg := hs.createMsg(MTCommit, &Node{Justify: hs.commitQC})
		hs.p2p.Broadcast(commitMsg)

		hs.onRecvProposal3(hs.conf.ProposerID, commitMsg)
	}
}

func (hs *HotStuff) onRecvProposal3(sender ID, msg *Msg) {
	hs.conf.Logger.Infof("proposer %s received Proposal3 %v", string(hs.conf.ProposerID), msg)
	if !hs.matchingMsg(msg, MTCommit, hs.view) {
		return
	}
	if msg.Node.Justify == nil {
		return
	}
	if !hs.matchingQC(msg.Node.Justify, MTPreCommit, hs.view) {
		return
	}

	newNode := msg.Node
	if newNode.Justify == nil || newNode.Justify.Type != MTPreCommit {
		return
	}
	err := newNode.Justify.Validate(hs)
	if err != nil {
		return
	}

	util.TryUntilSuccess(func() bool {

		err := hs.applyBlock(msg.Node.Block(), hs.lockQC, msg.Node.Justify)
		// TODO log error
		return err == nil

	}, time.Second)

}

func (hs *HotStuff) createQC(msg *Msg, votes map[string][]byte) *QC {

	return &QC{View: msg.View, Sigs: hs.tcombine(msg, votes), Type: msg.Type, Node: msg.Node}
}

func (hs *HotStuff) safeNode(newNode *Node) bool {

	if hs.lockQC == nil {
		return true
	}

	if bytes.Equal(hs.lockQC.Node.BlockHash(), newNode.BlockHash()) {
		return true
	}

	if newNode.Justify == nil {
		return false
	}

	if hs.lockQC.View < newNode.Justify.View {
		return true
	}
	return false
}
