package bihs

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/ontio/ontology/common"
)

type ID []byte

func (id ID) String() string {
	return hex.EncodeToString(id)
}

type Hash []byte

func (hash Hash) String() string {
	return hex.EncodeToString(hash)
}

type MsgType uint8

const (
	MTPrepare MsgType = iota + 1
	MTPreCommit
	MTCommit
	MTNewView
	MTReqBlock
	MTRespBlock
)

type Serializable interface {
	Serialize(sink *common.ZeroCopySink)
	Deserialize(source *common.ZeroCopySource) error
}

type Msg struct {
	Type       MsgType
	Height     uint64
	View       uint64
	Justify    *QC
	Node       *BlockOrHash
	ID         ID // needed for bls
	PartialSig []byte
}

var _ Serializable = (*Msg)(nil)
var _ Serializable = (*BlockOrHash)(nil)
var _ Serializable = (*QC)(nil)

func (m *Msg) Serialize(sink *common.ZeroCopySink) {
	sink.WriteByte(byte(m.Type))
	sink.WriteUint64(m.Height)
	sink.WriteUint64(m.View)
	sink.WriteBool(m.Justify != nil)

	if m.Justify != nil {
		m.Justify.Serialize(sink)
	}

	sink.WriteBool(m.Node != nil)
	if m.Node != nil {
		m.Node.Serialize(sink)
	}

	sink.WriteVarBytes(m.ID)
	sink.WriteVarBytes(m.PartialSig)
}

func (m *Msg) Deserialize(source *common.ZeroCopySource) (err error) {
	t, eof := source.NextByte()
	if eof {
		err = fmt.Errorf("Msg.Deserialize Type EOF")
		return
	}

	m.Type = MsgType(t)
	m.Height, eof = source.NextUint64()
	if eof {
		err = fmt.Errorf("Msg.Deserialize Height EOF")
		return
	}
	m.View, eof = source.NextUint64()
	if eof {
		err = fmt.Errorf("Msg.Deserialize View EOF")
		return
	}

	isQC, ir, eof := source.NextBool()
	if ir || eof {
		err = fmt.Errorf("Msg.Deserialize isQC EOF")
		return
	}
	if isQC {
		m.Justify = &QC{}
		err = m.Justify.Deserialize(source)
		if err != nil {
			err = fmt.Errorf("Msg.Justify.Deserialize fail:%v", err)
			return
		}
	}

	isNode, ir, eof := source.NextBool()
	if ir || eof {
		err = fmt.Errorf("Msg.Deserialize isNode EOF")
		return
	}

	m.Node = &BlockOrHash{}
	if isNode {
		err = m.Node.Deserialize(source)
		if err != nil {
			err = fmt.Errorf("Msg.Node.Deserialize fail:%v", err)
			return
		}
	}
	if m.Node.Blk == nil && m.Node.Hash == nil && m.Justify != nil {
		m.Node.Hash = m.Justify.BlockHash
	}
	if m.Node.Blk == nil && m.Node.Hash == nil {
		err = fmt.Errorf("m.Node empty")
		return
	}

	m.ID, err = source.ReadVarBytes()
	if err != nil {
		err = fmt.Errorf("Msg.Deserialize ID:%v", err)
		return
	}

	m.PartialSig, err = source.ReadVarBytes()
	if err != nil {
		err = fmt.Errorf("Msg.Deserialize PartialSig:%v", err)
		return
	}

	return
}

func (m *Msg) BlockHash() []byte {
	if m.Justify != nil {
		return m.Justify.BlockHash
	}
	return m.Node.BlockHash()
}

func (m *Msg) Hash() Hash {
	sink := common.NewZeroCopySink(nil)
	sink.WriteByte(byte(m.Type))
	sink.WriteUint64(m.Height)
	sink.WriteUint64(m.View)
	sink.WriteVarBytes(m.BlockHash())

	hash := sha256.Sum256(sink.Bytes())
	return Hash(hash[:])
}

type P2P interface {
	Broadcast(*Msg)
	Send(ID, *Msg)
	MsgCh() <-chan *Msg
}

type BlockWithTime interface {
	Block
	TimeMil() uint64
}

type Block interface {
	Serializable
	Height() uint64
	Hash() Hash
	Empty() bool
}

type QC struct {
	Type      MsgType
	Height    uint64
	View      uint64
	BlockHash Hash
	Sigs      []byte
	Bitmap    []byte // needed for bls
}

func (qc *QC) Serialize(sink *common.ZeroCopySink) {
	sink.WriteByte(byte(qc.Type))
	sink.WriteUint64(qc.Height)
	sink.WriteUint64(qc.View)
	sink.WriteVarBytes(qc.BlockHash)
	sink.WriteVarBytes(qc.Sigs)
	sink.WriteVarBytes(qc.Bitmap)
}

func (qc *QC) Deserialize(source *common.ZeroCopySource) (err error) {
	t, eof := source.NextByte()
	if eof {
		err = fmt.Errorf("QC.Deserialize Type EOF")
		return
	}

	qc.Type = MsgType(t)
	qc.Height, eof = source.NextUint64()
	if eof {
		err = fmt.Errorf("QC.Deserialize Height EOF")
		return
	}
	qc.View, eof = source.NextUint64()
	if eof {
		err = fmt.Errorf("QC.Deserialize View EOF")
		return
	}
	qc.BlockHash, err = source.ReadVarBytes()
	if err != nil {
		err = fmt.Errorf("QC.Deserialize BlockHash invalid:%v", err)
		return
	}
	qc.Sigs, err = source.ReadVarBytes()
	if err != nil {
		err = fmt.Errorf("QC.Deserialize Sigs invalid:%v", err)
		return
	}

	qc.Bitmap, err = source.ReadVarBytes()
	if err != nil {
		err = fmt.Errorf("QC.Deserialize Bitmap invalid:%v", err)
		return
	}

	return
}

func (qc *QC) SerializeForHeader(sink *common.ZeroCopySink) {

	sink.WriteUint64(qc.View)
	sink.WriteVarBytes(qc.Sigs)
	sink.WriteVarBytes(qc.Bitmap)
}

func (qc *QC) DeserializeFromHeader(height uint64, blockHash []byte, source *common.ZeroCopySource) (err error) {
	view, eof := source.NextUint64()
	if eof {
		err = fmt.Errorf("QC.DeserializeFromHeader View EOF")
		return
	}
	qc.View = view

	qc.Sigs, err = source.ReadVarBytes()
	if err != nil {
		err = fmt.Errorf("QC.DeserializeFromHeader Sigs invalid:%v", err)
		return
	}

	qc.Bitmap, err = source.ReadVarBytes()
	if err != nil {
		err = fmt.Errorf("QC.DeserializeFromHeader Bitmap invalid:%v", err)
		return
	}

	qc.Type = MTPreCommit
	qc.Height = height
	qc.BlockHash = blockHash
	return
}

func (qc *QC) VerifyEC(signer EcSigner, ids []ID) bool {
	return signer.TVerify((&Msg{Type: qc.Type, Height: qc.Height, View: qc.View, Node: &BlockOrHash{Hash: qc.BlockHash}}).Hash(), qc.Sigs, ids, quorum(int32(len(ids))))
}

func (qc *QC) validate(hs *HotStuff) error {
	return hs.validateQC(qc)
}

type BlockOrHash struct {
	Blk  Block
	Hash Hash
}

func (boh *BlockOrHash) Serialize(sink *common.ZeroCopySink) {
	sink.WriteBool(boh.Blk != nil)
	if boh.Blk != nil {
		boh.Blk.Serialize(sink)
	} else {
		sink.WriteVarBytes(boh.Hash)
	}
}

var DefaultBlockFunc func() Block

func (boh *BlockOrHash) Deserialize(source *common.ZeroCopySource) error {
	isBlk, ir, eof := source.NextBool()
	if ir || eof {
		return fmt.Errorf("BlockOrHash.Deserialize ir:%v eof:%v", ir, eof)
	}
	if isBlk {
		boh.Blk = DefaultBlockFunc()
		return boh.Blk.Deserialize(source)
	}

	boh.Hash, _, ir, eof = source.NextVarBytes()
	if ir || eof {
		return fmt.Errorf("BlockOrHash.Deserialize ir:%v eof:%v", ir, eof)
	}

	return nil
}

func (boh *BlockOrHash) BlockHash() Hash {
	if boh.Hash != nil {
		return boh.Hash
	}

	return boh.Blk.Hash()
}

type StateDB interface {
	StoreBlock(blk Block, commitQC *QC) error
	Validate(blk Block) error
	Height() uint64
	SubscribeHeightChange(HeightChangeSub)
	UnSubscribeHeightChange(HeightChangeSub)

	ValidatorIndex(height uint64, peer ID) int
	SelectLeader(height, view uint64) ID
	EmptyBlock(height uint64) (Block, error)
	ValidatorCount(height uint64) int32
	ValidatorIDs(height uint64) []ID

	// used together with BlsSigner
	PKs(height uint64, bitmap []byte) interface{}
}

type HeightChangeSub interface {
	HeightChanged()
}

type EcSigner interface {
	Sign(data []byte) []byte
	Verify(data []byte, sig []byte) (ID, error)
	TCombine(data []byte, sigs [][]byte) []byte
	TVerify(data []byte, sigs []byte, ids []ID, quorum int32) bool
}

type BlsSigner interface {
	Sign(data []byte) []byte
	Verify(data []byte, ID, sig []byte) error
	PK(ID) interface{}
	TCombine(data []byte, pks interface{}, sigs [][]byte) []byte
	TVerify(data []byte, pks interface{}, sigs []byte) bool
}

type StateSyncer interface {
	SyncWithPeer(ID, uint64)
}

type Config struct {
	BlockInterval time.Duration
	ProposerID    ID
	DataDir       string

	EcSigner         EcSigner
	BlsSigner        BlsSigner
	Syncer           StateSyncer
	Logger           Logger
	DefaultBlockFunc func() Block
}

func (config *Config) validate() error {
	if config.BlockInterval <= 0 {
		return fmt.Errorf("config.BlockInterval<=0")
	}
	if config.ProposerID == nil {
		return fmt.Errorf("Config.ProposerID is nil")
	}
	if config.EcSigner == nil && config.BlsSigner == nil {
		return fmt.Errorf("both Config.EcSigner and Config.BlsSigner are nil")
	}
	if config.DataDir == "" {
		config.DataDir = "./wal"
	}
	err := os.MkdirAll(config.DataDir, 0777)
	if err != nil {
		return err
	}
	if config.Logger == nil {
		config.Logger = defaultLogger()
	}
	if config.DefaultBlockFunc == nil && DefaultBlockFunc == nil {
		return fmt.Errorf("DefaultBlock missing")
	}
	if config.DefaultBlockFunc != nil {
		DefaultBlockFunc = config.DefaultBlockFunc
	}

	return nil
}

type ConsensusState struct {
	height          uint64
	view            uint64
	candidateBlk    Block
	lockQC          *QC
	hasVotedPrepare bool
	votes1          map[int][]byte
	votes2          map[int][]byte
	voted           []ID
	idx             int
}

func (cs *ConsensusState) Serialize(sink *common.ZeroCopySink) {
	sink.WriteUint64(cs.height)
	sink.WriteUint64(cs.view)
	sink.WriteBool(cs.candidateBlk != nil)
	if cs.candidateBlk != nil {
		cs.candidateBlk.Serialize(sink)
	}
	sink.WriteBool(cs.lockQC != nil)
	if cs.lockQC != nil {
		cs.lockQC.Serialize(sink)
	}
	sink.WriteBool(cs.hasVotedPrepare)
	sink.WriteInt32(int32(len(cs.votes1)))
	for id, vote := range cs.votes1 {
		sink.WriteInt64(int64(id))
		sink.WriteVarBytes(vote)
	}
	sink.WriteInt32(int32(len(cs.votes2)))
	for id, vote := range cs.votes2 {
		sink.WriteInt64(int64(id))
		sink.WriteVarBytes(vote)
	}
	sink.WriteInt32(int32(len(cs.voted)))
	for _, id := range cs.voted {
		sink.WriteVarBytes(id)
	}

	sink.WriteInt64(int64(cs.idx))
}

func (cs *ConsensusState) Deserialize(source *common.ZeroCopySource) (err error) {
	height, eof := source.NextUint64()
	if eof {
		err = fmt.Errorf("ConsensusState.Deserialize Height EOF")
		return
	}
	cs.height = height

	view, eof := source.NextUint64()
	if eof {
		err = fmt.Errorf("ConsensusState.Deserialize View EOF")
		return
	}
	cs.view = view

	candidateBlk, ir, eof := source.NextBool()
	if ir || eof {
		err = fmt.Errorf("ConsensusState.Deserialize candidateBlk bool EOF")
		return
	}
	if candidateBlk {
		block := DefaultBlockFunc()
		err = block.Deserialize(source)
		if err != nil {
			return
		}
		cs.candidateBlk = block
	}

	lockQC, ir, eof := source.NextBool()
	if ir || eof {
		err = fmt.Errorf("ConsensusState.Deserialize lockQC bool EOF")
		return
	}
	if lockQC {
		qc := &QC{}
		err = qc.Deserialize(source)
		if err != nil {
			return
		}
		cs.lockQC = qc
	}

	hasVotedPrepare, ir, eof := source.NextBool()
	if ir || eof {
		err = fmt.Errorf("ConsensusState.Deserialize hasVotedPrepare EOF")
		return
	}
	cs.hasVotedPrepare = hasVotedPrepare

	cs.votes1 = make(map[int][]byte)
	count, eof := source.NextInt32()
	if eof {
		err = fmt.Errorf("ConsensusState.Deserialize #votes1 EOF")
		return
	}
	for i := 0; i < int(count); i++ {
		id, eof := source.NextInt64()
		if eof {
			err = fmt.Errorf("ConsensusState.Deserialize votes1.id EOF")
			return
		}
		vote, _, ir, eof := source.NextVarBytes()
		if ir || eof {
			err = fmt.Errorf("ConsensusState.Deserialize votes1.vote EOF")
			return
		}
		cs.votes1[int(id)] = vote
	}

	cs.votes2 = make(map[int][]byte)
	count, eof = source.NextInt32()
	if eof {
		err = fmt.Errorf("ConsensusState.Deserialize #votes2 EOF")
		return
	}
	for i := 0; i < int(count); i++ {
		id, eof := source.NextInt64()
		if eof {
			err = fmt.Errorf("ConsensusState.Deserialize votes2.id EOF")
			return
		}
		vote, _, ir, eof := source.NextVarBytes()
		if ir || eof {
			err = fmt.Errorf("ConsensusState.Deserialize votes2.vote EOF")
			return
		}
		cs.votes2[int(id)] = vote
	}

	count, eof = source.NextInt32()
	if eof {
		err = fmt.Errorf("ConsensusState.Deserialize #voted EOF")
		return
	}
	cs.voted = make([]ID, count)
	for i := 0; i < int(count); i++ {
		id, _, ir, eof := source.NextVarBytes()
		if ir || eof {
			err = fmt.Errorf("ConsensusState.Deserialize voted.%d EOF #voted:%d", i, count)
			return
		}
		if len(id) > 0 {
			cs.voted[i] = ID(id)
		}
	}

	idx, eof := source.NextInt64()
	if eof {
		err = fmt.Errorf("ConsensusState.Deserialize idx EOF")
		return
	}
	cs.idx = int(idx)
	return
}

func (cs *ConsensusState) advanceToHeight(height uint64, hs *HotStuff) {
	atomic.StoreUint64(&cs.height, height)
	atomic.StoreUint64(&cs.view, 0)
	cs.candidateBlk = nil
	cs.lockQC = nil
	cs.hasVotedPrepare = false

	cs.idx = hs.store.ValidatorIndex(height, hs.conf.ProposerID)
	count := int(hs.store.ValidatorCount(height))
	if hs.idx >= count {
		panic(fmt.Sprintf("invalid index(%d) for proposer:%s", hs.idx, hs.conf.ProposerID))
	}
	cs.resetVote(count)
}

func (cs *ConsensusState) resetVote(count int) {
	cs.votes1 = make(map[int][]byte)
	cs.votes2 = make(map[int][]byte)
	if count == 0 {
		count = len(cs.voted)
	}
	cs.voted = make([]ID, count)
}

func (cs *ConsensusState) advanceToView(view uint64) {
	atomic.StoreUint64(&cs.view, view)
	cs.hasVotedPrepare = false
	cs.resetVote(0)
}

type Logger interface {
	Info(a ...interface{})
	Infof(format string, a ...interface{})
	Debug(a ...interface{})
	Debugf(format string, a ...interface{})
	Fatal(a ...interface{})
	Fatalf(format string, a ...interface{})
	Error(a ...interface{})
	Errorf(format string, a ...interface{})
}
