package bihs

import (
	"crypto/sha256"
	"fmt"
	"os"
	"time"

	"github.com/ontio/ontology/common"
)

type ID []byte

type MsgType uint8

const (
	MTPrepare MsgType = iota + 1
	MTCommit
	MTDecide
	MTNewView
	MTReqBlock
	MTRespBlock
)

type PendingStep uint8

const (
	PSNone PendingStep = iota + 1
	PSPrepare
	PSDecide
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
var _ Serializable = (*ConsensusState)(nil)

func (m *Msg) Serialize(sink *common.ZeroCopySink) {
	sink.WriteByte(byte(m.Type))
	sink.WriteUint64(m.Height)
	sink.WriteUint64(m.View)
	sink.WriteBool(m.Justify != nil)

	if m.Justify != nil {
		m.Justify.Serialize(sink)
	}

	m.Node.Serialize(sink)

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
		m.Node = &BlockOrHash{Hash: m.Justify.BlockHash}
	} else {
		m.Node = &BlockOrHash{}
		err = m.Node.Deserialize(source)
		if err != nil {
			return
		}
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

func (m *Msg) Hash() []byte {
	sink := common.NewZeroCopySink(nil)
	sink.WriteByte(byte(m.Type))
	sink.WriteUint64(m.Height)
	sink.WriteUint64(m.View)
	sink.WriteVarBytes(m.BlockHash())

	hash := sha256.Sum256(sink.Bytes())
	return hash[:]
}

type P2P interface {
	Broadcast(*Msg)
	Send(ID, *Msg)
	MsgCh() <-chan *Msg
}

type Block interface {
	Serializable
	Default() Block
	Validate() error
	Height() uint64
	Hash() []byte
}

type QC struct {
	Type      MsgType
	Height    uint64
	View      uint64
	BlockHash []byte
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

func (qc *QC) Validate(hs *HotStuff) error {
	return hs.validateQC(qc)
}

type BlockOrHash struct {
	Blk  Block
	Hash []byte
}

func (boh *BlockOrHash) Serialize(sink *common.ZeroCopySink) {
	sink.WriteBool(boh.Blk != nil)
	if boh.Blk != nil {
		boh.Blk.Serialize(sink)
	} else {
		sink.WriteVarBytes(boh.Hash)
	}
}

func (boh *BlockOrHash) Deserialize(source *common.ZeroCopySource) error {
	isBlk, ir, eof := source.NextBool()
	if ir || eof {
		return fmt.Errorf("BlockOrHash.Deserialize ir:%v eof:%v", ir, eof)
	}
	if isBlk {
		boh.Blk = boh.Blk.Default()
		return boh.Blk.Deserialize(source)
	}

	boh.Hash, _, ir, eof = source.NextVarBytes()
	if ir || eof {
		return fmt.Errorf("BlockOrHash.Deserialize ir:%v eof:%v", ir, eof)
	}

	return nil
}

func (boh *BlockOrHash) BlockHash() []byte {
	if boh.Hash != nil {
		return boh.Hash
	}

	return boh.Blk.Hash()
}

type StateDB interface {
	GetBlock(n uint64) Block
	StoreBlock(blk Block, lockQc, commitQC *QC) error
	Height() uint64
	IsBlockProposableBy(Block, ID) bool
}

type EcSigner interface {
	Sign(data []byte) []byte
	Verify(data []byte, sig []byte) (ID, error)
	TCombine(data []byte, sigs [][]byte) []byte
	TVerify(data []byte, sigs []byte, quorum int32) bool
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
	BlockInterval     time.Duration
	SyncCheckInterval time.Duration
	ProposerID        ID
	WalPath           string

	ValidatorIndex func(height uint64, peer ID) int
	SelectLeader   func(height, view uint64) ID
	EmptyBlock     func(height uint64) Block
	ValidatorCount func(height uint64) int32

	// used together with BlsSigner
	PKs func(height uint64, bitmap []byte) interface{}

	EcSigner  EcSigner
	BlsSigner BlsSigner
	Syncer    StateSyncer
	Logger    Logger
}

func (config *Config) validate() error {
	if config.ProposerID == nil {
		return fmt.Errorf("Config.ProposerID is nil")
	}
	if config.ValidatorIndex == nil {
		return fmt.Errorf("Config.ValidatorIndex is nil")
	}
	if config.SelectLeader == nil {
		return fmt.Errorf("Config.SelectLeader is nil")
	}
	if config.EmptyBlock == nil {
		return fmt.Errorf("Config.EmptyBlock is nil")
	}
	if config.ValidatorCount == nil {
		return fmt.Errorf("Config.ValidatorCount is nil")
	}
	if config.EcSigner == nil && config.BlsSigner == nil {
		return fmt.Errorf("both Config.EcSigner and Config.BlsSigner are nil")
	}
	if config.WalPath == "" {
		config.WalPath = "./wal"
	}
	err := os.MkdirAll(config.WalPath, 0777)
	if err != nil {
		return err
	}
	if config.Logger == nil {
		config.Logger = defaultLogger()
	}

	return nil
}

type ConsensusState struct {
	Height       uint64
	View         uint64
	CandidateBlk Block
	LockQC       *QC
	HasVoted     bool
}

func (cs *ConsensusState) Serialize(sink *common.ZeroCopySink) {
	sink.WriteUint64(cs.Height)
	sink.WriteUint64(cs.View)
	sink.WriteBool(cs.CandidateBlk != nil)
	if cs.CandidateBlk != nil {
		cs.CandidateBlk.Serialize(sink)
	}
	sink.WriteBool(cs.LockQC != nil)
	if cs.LockQC != nil {
		cs.LockQC.Serialize(sink)
	}
	sink.WriteBool(cs.HasVoted)
}

func (cs *ConsensusState) Deserialize(source *common.ZeroCopySource) (err error) {
	height, eof := source.NextUint64()
	if eof {
		err = fmt.Errorf("ConsensusState.Deserialize EOF at Height")
		return
	}
	view, eof := source.NextUint64()
	if eof {
		err = fmt.Errorf("ConsensusState.Deserialize EOF at View")
		return
	}
	hasBlk, ir, eof := source.NextBool()
	if ir || eof {
		err = fmt.Errorf("ConsensusState.Deserialize EOF at HasBlk")
		return
	}
	if hasBlk {
		cs.CandidateBlk = cs.CandidateBlk.Default()
		err = cs.CandidateBlk.Deserialize(source)
		if err != nil {
			return
		}
	}
	hasQC, ir, eof := source.NextBool()
	if ir || eof {
		err = fmt.Errorf("ConsensusState.Deserialize EOF at HasQC")
		return
	}
	if hasQC {
		cs.LockQC = &QC{}
		err = cs.LockQC.Deserialize(source)
		if err != nil {
			return
		}
	}
	if hasBlk != hasQC {
		err = fmt.Errorf("inconsistent consensus state detected: (hasBlk:%v hasQC:%v)", hasBlk, hasQC)
		return
	}

	hasVoted, ir, eof := source.NextBool()
	if ir || eof {
		err = fmt.Errorf("ConsensusState.Deserialize EOF at HasVoted")
		return
	}
	if hasVoted && cs.CandidateBlk == nil {
		err = fmt.Errorf("inconsistent consensus state detected: (HasVoted:%v CandidateBlk:nil)", hasVoted)
		return
	}

	cs.Height = height
	cs.View = view
	cs.HasVoted = hasVoted
	return
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
