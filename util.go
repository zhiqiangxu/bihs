package bihs

import (
	"bytes"
)

func (hs *HotStuff) createMsg(mt MsgType, node *Node) *Msg {
	msg := &Msg{Type: mt, Node: node, Height: hs.height, View: hs.view}

	msg.PartialSig = hs.conf.Sign(msg.Hash())
	return msg
}

func (hs *HotStuff) voteMsg(mt MsgType, node *Node) *Msg {
	return hs.createMsg(mt, node)
}

func (hs *HotStuff) matchingMsg(msg *Msg, t MsgType, v uint64) bool {
	return hs.height == msg.Node.Height() && msg.Type == t && msg.View == v
}

func (hs *HotStuff) matchingQC(qc *QC, t MsgType, v uint64) bool {
	return hs.height == qc.Node.Height() && qc.Type == t && qc.View == v
}

func (hs *HotStuff) isLeader(height, view uint64) bool {

	isLeader := bytes.Equal(hs.conf.ProposerID, hs.conf.SelectLeader(height, view))

	return isLeader
}

func quorum(n int32) int32 {
	return 2*n/3 + 1
}

func (hs *HotStuff) tcombine(msg *Msg, votes map[string][]byte) []byte {
	var sigs [][]byte
	for voter := range votes {
		sigs = append(sigs, votes[voter])
	}
	return hs.conf.TCombine(msg.Hash(), sigs)
}
