package bihs

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/bits"
)

func (hs *HotStuff) createMsgEc(mt MsgType, justify *QC, node *BlockOrHash) *Msg {
	msg := &Msg{Type: mt, Justify: justify, Node: node, Height: hs.height, View: hs.view}

	msg.PartialSig = hs.sign(msg.Hash())

	return msg
}

func (hs *HotStuff) createMsgBls(mt MsgType, justify *QC, node *BlockOrHash) *Msg {
	msg := &Msg{Type: mt, Justify: justify, Node: node, Height: hs.height, View: hs.view, ID: hs.conf.ProposerID}

	msg.PartialSig = hs.sign(msg.Hash())

	return msg
}

func (hs *HotStuff) signEc(data []byte) []byte {
	return hs.conf.EcSigner.Sign(data)
}

func (hs *HotStuff) signBls(data []byte) []byte {
	return hs.conf.BlsSigner.Sign(data)
}

func (hs *HotStuff) voteMsg(mt MsgType, blkHash []byte) *Msg {
	return hs.createMsg(mt, nil, &BlockOrHash{Hash: blkHash})
}

func (hs *HotStuff) matchingMsg(msg *Msg, t MsgType, v uint64) bool {
	return hs.height == msg.Height && msg.Type == t && msg.View == v
}

func (hs *HotStuff) matchingQC(qc *QC, t MsgType, v uint64) bool {
	return hs.height == qc.Height && qc.Type == t && qc.View == v
}

func (hs *HotStuff) isLeader(height, view uint64) bool {

	isLeader := bytes.Equal(hs.conf.ProposerID, hs.store.SelectLeader(height, view))

	return isLeader
}

func quorum(n int32) int32 {
	return 2*n/3 + 1
}

func (hs *HotStuff) validateQCEc(qc *QC) error {
	switch qc.Type {
	case MTPrepare:
		fallthrough
	case MTPreCommit:
		ids := hs.store.ValidatorIDs(qc.Height)
		if !hs.conf.EcSigner.TVerify((&Msg{Type: qc.Type, Height: qc.Height, View: qc.View, Node: &BlockOrHash{Hash: qc.BlockHash}}).Hash(), qc.Sigs, ids, quorum(int32(len(ids)))) {
			return fmt.Errorf("TVerify failed, type %d", qc.Type)
		}
	default:
		return fmt.Errorf("invalid type(%d) for qc", qc.Type)
	}

	return nil
}

func (hs *HotStuff) verifyEc(msg *Msg) (voter ID, err error) {
	return hs.conf.EcSigner.Verify(msg.Hash(), msg.PartialSig)
}

func (hs *HotStuff) verifyBls(msg *Msg) (voter ID, err error) {
	err = hs.conf.BlsSigner.Verify(msg.Hash(), msg.ID, msg.PartialSig)
	if err != nil {
		return
	}

	voter = msg.ID
	return
}

func (hs *HotStuff) bitmapEnough(bitmap []byte, q int32) bool {
	ones := 0

	offset := 0
out:
	for {
		remain := len(bitmap) - offset
		switch {
		case remain >= 8:
			ones += bits.OnesCount64(binary.LittleEndian.Uint64(bitmap[offset : offset+8]))
			offset += 8
		case remain >= 4:
			ones += bits.OnesCount32(binary.LittleEndian.Uint32(bitmap[offset : offset+4]))
			offset += 4
		case remain >= 2:
			ones += bits.OnesCount16(binary.LittleEndian.Uint16(bitmap[offset : offset+2]))
			offset += 2
		case remain == 1:
			ones += bits.OnesCount8(bitmap[offset])
			fallthrough
		case remain == 0:
			break out
		}
	}
	return int32(ones) >= q
}

func (hs *HotStuff) validateQCBls(qc *QC) error {
	switch qc.Type {
	case MTPrepare:
		fallthrough
	case MTPreCommit:
		height := qc.Height
		q := quorum(hs.store.ValidatorCount(height))
		if !hs.bitmapEnough(qc.Bitmap, q) {
			return fmt.Errorf("not enough validators in bitmap")
		}
		pks := hs.store.PKs(height, qc.Bitmap)
		if !hs.conf.BlsSigner.TVerify((&Msg{Type: qc.Type, Height: qc.Height, View: qc.View, Node: &BlockOrHash{Hash: qc.BlockHash}}).Hash(), pks, qc.Sigs) {
			return fmt.Errorf("TVerify failed, type %d", qc.Type)
		}
	default:
		return fmt.Errorf("invalid type(%d) for qc", qc.Type)
	}
	return nil
}

func (hs *HotStuff) tcombineEc(msg *Msg, votes map[int][]byte) []byte {
	var sigs [][]byte

	for voter := range votes {
		sigs = append(sigs, votes[voter])
	}
	return hs.conf.EcSigner.TCombine(msg.Hash(), sigs)
}

func (hs *HotStuff) tcombineBls(msg *Msg, votes map[int][]byte) []byte {
	var (
		sigs [][]byte
		pks  []interface{}
	)

	for idx := range votes {
		pk := hs.conf.BlsSigner.PK(hs.voted[idx])
		sigs = append(sigs, votes[idx])
		pks = append(pks, pk)
	}
	return hs.conf.BlsSigner.TCombine(msg.Hash(), pks, sigs)
}
