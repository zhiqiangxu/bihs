package bihs

import (
	"crypto/sha256"
	"fmt"
	"time"

	"github.com/ontio/ontology/common"
)

type ID []byte

type MsgType uint8

const (
	MTPrepare MsgType = iota + 1
	MTPreCommit
	MTCommit
	MTNewView
)

type Serializable interface {
	Serialize(sink *common.ZeroCopySink) error
	Deserialize(source *common.ZeroCopySource) error
}

type Msg struct {
	Type       MsgType
	Node       *Node
	Height     uint64
	View       uint64
	PartialSig []byte
}

var _ Serializable = (*Msg)(nil)
var _ Serializable = (*Node)(nil)
var _ Serializable = (*QC)(nil)

func (m *Msg) Serialize(sink *common.ZeroCopySink) (err error) {
	sink.WriteByte(byte(m.Type))
	if m.Node == nil {
		err = fmt.Errorf("Msg.Serialize Node empty")
		return
	}
	m.Node.Serialize(sink)
	sink.WriteUint64(m.View)
	sink.WriteVarBytes(m.PartialSig)
	return
}

func (m *Msg) Deserialize(source *common.ZeroCopySource) (err error) {
	t, eof := source.NextByte()
	if eof {
		err = fmt.Errorf("Msg.Deserialize Type EOF")
		return
	}

	m.Type = MsgType(t)

	m.Node = &Node{}
	err = m.Node.Deserialize(source)
	if err != nil {
		return
	}

	m.View, eof = source.NextUint64()
	if eof {
		err = fmt.Errorf("Msg.Deserialize View EOF")
		return
	}

	m.PartialSig, err = source.ReadVarBytes()
	if err != nil {
		err = fmt.Errorf("Msg.Deserialize PartialSig:%v", err)
	}
	return
}

func (m *Msg) Hash() []byte {
	sink := common.NewZeroCopySink(nil)
	sink.WriteByte(byte(m.Type))
	sink.WriteUint64(m.Height)
	sink.WriteUint64(m.View)
	sink.WriteVarBytes(m.Node.BlockHash())

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
	Type MsgType
	Node *Node
	View uint64
	Sigs []byte
}

func (qc *QC) Serialize(sink *common.ZeroCopySink) (err error) {
	sink.WriteByte(byte(qc.Type))
	if qc.Node == nil {
		err = fmt.Errorf("QC.Serialize Node empty")
		return
	}
	qc.Node.Serialize(sink)
	sink.WriteUint64(qc.View)
	sink.WriteVarBytes(qc.Sigs)
	return
}

func (qc *QC) Deserialize(source *common.ZeroCopySource) (err error) {
	t, eof := source.NextByte()
	if eof {
		err = fmt.Errorf("QC.Deserialize Type EOF")
		return
	}

	qc.Type = MsgType(t)
	qc.Node = &Node{}
	err = qc.Node.Deserialize(source)
	if err != nil {
		err = fmt.Errorf("QC.Deserialize Node invalid:%v", err)
		return
	}

	qc.View, eof = source.NextUint64()
	if eof {
		err = fmt.Errorf("QC.Deserialize View EOF")
		return
	}

	qc.Sigs, err = source.ReadVarBytes()
	if err != nil {
		err = fmt.Errorf("QC.Deserialize Sigs invalid:%v", err)
		return
	}

	return
}

func (qc *QC) Validate(hs *HotStuff) error {
	switch qc.Type {
	case MTPrepare:
		hs.conf.TVerify((&Msg{Type: qc.Type, View: qc.View, Node: qc.Node}).Hash(), qc.Sigs, quorum(hs.conf.ValidatorCount(qc.Node.Height())))
	case MTPreCommit:
		hs.conf.TVerify((&Msg{Type: qc.Type, View: qc.View, Node: qc.Node}).Hash(), qc.Sigs, quorum(hs.conf.ValidatorCount(qc.Node.Height())))
	default:
		return fmt.Errorf("invalid type(%d) for qc", qc.Type)
	}
	return nil
}

// TODO Blk can be empty when embeded in a QC
type BlockPayload struct {
	Blk  Block
	Hash []byte
}

type Node struct {
	Blk     Block
	Justify *QC
}

func (n *Node) Serialize(sink *common.ZeroCopySink) (err error) {
	if n.Blk != nil {
		sink.WriteBool(true)
		err = n.Blk.Serialize(sink)
	} else {
		sink.WriteBool(false)
		if n.Justify == nil {
			sink.WriteBool(false)
			return
		} else {
			sink.WriteBool(true)
		}
		err = n.Justify.Serialize(sink)

	}

	return
}

func (n *Node) Deserialize(source *common.ZeroCopySource) (err error) {
	b, irregular, eof := source.NextBool()
	if irregular || eof {
		err = fmt.Errorf("Node.Deserialize irregular:%v eof:%v", irregular, eof)
		return
	}
	if b {
		n.Blk = n.Blk.Default()
		err = n.Blk.Deserialize(source)
	} else {
		b, irregular, eof = source.NextBool()
		if irregular || eof {
			err = fmt.Errorf("Node.Deserialize irregular:%v eof:%v", irregular, eof)
			return
		}
		if !b {
			return
		}
		n.Justify = &QC{}
		err = n.Justify.Deserialize(source)
	}

	return
}

func (n *Node) Validate(hs *HotStuff) error {
	if n.Blk != nil {
		return n.Blk.Validate()
	}

	if n.Justify == nil {
		return nil
	}

	return n.Justify.Validate(hs)
}

func (n *Node) BlockHash() []byte {
	if n.Blk != nil {
		return n.Blk.Hash()
	}

	if n.Justify == nil {
		return nil
	}
	return n.Justify.Node.BlockHash()
}

func (n *Node) Height() uint64 {
	if n.Blk != nil {
		return n.Blk.Height()
	}

	return n.Justify.Node.Height()
}

func (n *Node) Block() Block {
	if n.Blk != nil {
		return n.Blk
	}

	return n.Justify.Node.Block()
}

type StateDB interface {
	GetBlock(n uint64) Block
	StoreBlock(blk Block, lockQc, commitQC *QC) error
	StoreVoted(voted uint64) error
	GetVoted() int64 // initial(empty) value should be -1
	ClearVoted()
	Height() uint64
	IsBlockProposableBy(Block, ID) bool
}

type Config struct {
	BlockInterval     time.Duration
	SyncCheckInterval time.Duration
	ProposerID        ID

	IsValidator    func(height uint64, peer ID) bool
	SelectLeader   func(height, view uint64) ID
	EmptyBlock     func(height uint64) Block
	ValidatorCount func(height uint64) int32
	Sign           func(data []byte) []byte
	Verify         func(data []byte, sig []byte) (ID, error)
	TCombine       func(data []byte, sigs [][]byte) []byte
	TVerify        func(data []byte, sigs []byte, quorum int32) bool
	Logger         Logger
}

func (config *Config) validate() error {
	if config.ProposerID == nil {
		return fmt.Errorf("Config.ProposerID is nil")
	}
	if config.IsValidator == nil {
		return fmt.Errorf("Config.IsValidator is nil")
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
	if config.Sign == nil {
		return fmt.Errorf("Config.Sign is nil")
	}
	if config.Verify == nil {
		return fmt.Errorf("Config.Verify is nil")
	}
	if config.TCombine == nil {
		return fmt.Errorf("Config.TCombine is nil")
	}
	if config.TVerify == nil {
		return fmt.Errorf("Config.TVerify is nil")
	}
	if config.Logger == nil {
		config.Logger = defaultLogger()
	}

	return nil
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
