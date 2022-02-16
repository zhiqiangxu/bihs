package bihs

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/ontio/ontology/common"
)

type (
	HappyBlock uint64
	HappyDB    struct {
		validators int
		Blocks     []HappyBlock
		voted      int64
		subs       []HeightChangeSub
	}
	HappyP2P struct {
		neibors map[string]*HappyP2P
		msgCh   chan *Msg
	}
)

func (b HappyBlock) Empty() bool {
	return b == EmptyBlock(b.Height())
}

func (b *HappyBlock) Serialize(sink *common.ZeroCopySink) {
	sink.WriteUint64(uint64(*b))
}
func (b *HappyBlock) Deserialize(source *common.ZeroCopySource) error {
	data, eof := source.NextUint64()
	if eof {
		return fmt.Errorf("HappyBlock.Deserialize EOF")
	}
	*b = HappyBlock(data)
	return nil
}

func (b *HappyBlock) Height() uint64 {
	return uint64(*b) & 0xffffffff
}

func (b *HappyBlock) Hash() Hash {
	hash := sha256.Sum256([]byte(fmt.Sprintf("%d", *b)))
	return hash[:]
}

func EmptyBlock(height uint64) HappyBlock {
	height |= (1 << 63)
	hb := HappyBlock(height)
	return hb
}

func NewHappyDB(totalValidators int) *HappyDB {
	return &HappyDB{validators: totalValidators, voted: -1}
}

func (db *HappyDB) getBlock(n uint64) Block {
	if int(n) >= len(db.Blocks) {
		return nil
	}

	return &db.Blocks[n]
}

func (db *HappyDB) SubscribeHeightChange(sub HeightChangeSub) {
	db.subs = append(db.subs, sub)
}

func (db *HappyDB) UnSubscribeHeightChange(sub HeightChangeSub) {
	for i, subed := range db.subs {
		if subed == sub {
			db.subs[len(db.subs)-1], db.subs[i] = db.subs[i], db.subs[len(db.subs)-1]
			db.subs = db.subs[0 : len(db.subs)-1]
			return
		}
	}
}

func (db *HappyDB) StoreBlock(blk Block, commitQC *QC) error {
	hp := blk.(*HappyBlock)
	if hp.Height() != uint64(len(db.Blocks)) {
		return fmt.Errorf("invalid block")
	}

	db.Blocks = append(db.Blocks, *hp)
	for _, sub := range db.subs {
		sub.HeightChanged()
	}
	return nil
}

func (db *HappyDB) Validate(blk Block) error {
	return nil
}

func (db *HappyDB) StoreVoted(voted uint64) error {
	db.voted = int64(voted)
	return nil
}
func (db *HappyDB) GetVoted() int64 {
	return db.voted
}

func (db *HappyDB) ClearVoted() {
	db.voted = -1
}
func (db *HappyDB) Height() uint64 {
	if len(db.Blocks) == 0 {
		panic("HappyDB not initialized")
	}
	return uint64(len(db.Blocks) - 1)
}

func (db *HappyDB) ValidatorIndex(height uint64, peer ID) int {
	id, err := strconv.ParseUint(string(peer), 10, 64)
	if err != nil {
		return -1
	}
	if id < uint64(db.validators) {
		return int(id)
	} else {
		return -1
	}
}
func (db *HappyDB) SelectLeader(height, view uint64) ID {
	return ID([]byte(fmt.Sprintf("%d", (height+view)%uint64(db.validators))))
}
func (db *HappyDB) EmptyBlock(height uint64) (Block, error) {
	b := EmptyBlock(height)
	return &b, nil
}

func (db *HappyDB) ValidatorCount(height uint64) int32 {
	return int32(db.validators)
}

func (db *HappyDB) ValidatorIDs(height uint64) []ID {
	var ids []ID
	for i := uint64(0); i < uint64(db.validators); i++ {
		id := ID([]byte(fmt.Sprintf("%d", i)))
		ids = append(ids, id)
	}
	return ids
}

// used together with BlsSigner
func (db *HappyDB) PKs(height uint64, bitmap []byte) interface{} {
	return nil
}

func NewHappyP2P() *HappyP2P {
	return &HappyP2P{msgCh: make(chan *Msg), neibors: make(map[string]*HappyP2P)}
}
func (p *HappyP2P) Broadcast(msg *Msg) {
	for id := range p.neibors {
		neibor := p.neibors[id]
		go func() {
			neibor.msgCh <- msg
		}()
	}
}

func (p *HappyP2P) Send(id ID, msg *Msg) {
	neibor := p.neibors[string(id)]
	if neibor == nil {
		return
	}

	sink := common.NewZeroCopySink(nil)
	msg.Serialize(sink)

	var decodeMsg Msg
	err := decodeMsg.Deserialize(common.NewZeroCopySource(sink.Bytes()))
	if err != nil {
		panic(fmt.Sprintf("decodeMsg.Deserialize failed:%v", err))
	}

	go func() {
		neibor.msgCh <- msg
	}()
}
func (p *HappyP2P) MsgCh() <-chan *Msg {
	return p.msgCh
}

type ecsigner struct {
	sign     func(data []byte) []byte
	verify   func(data []byte, sig []byte) (ID, error)
	tcombine func(data []byte, sigs [][]byte) []byte
	tVerify  func(data []byte, sigs []byte, ids []ID, quorum int32) bool
}

func (s *ecsigner) Sign(data []byte) []byte {
	return s.sign(data)
}

func (s *ecsigner) Verify(data []byte, sig []byte) (ID, error) {
	return s.verify(data, sig)
}

func (s *ecsigner) TCombine(data []byte, sigs [][]byte) []byte {
	return s.tcombine(data, sigs)
}

func (s *ecsigner) TVerify(data []byte, sigs []byte, ids []ID, quorum int32) bool {
	return s.tVerify(data, sigs, ids, quorum)
}

func TestHappyPath(t *testing.T) {
	var (
		dbs  []*HappyDB
		p2ps []*HappyP2P
		hss  []*HotStuff
	)
	genesis := HappyBlock(0)
	totalValidators := uint64(5)
	for i := uint64(0); i < totalValidators; i++ {
		db := NewHappyDB(int(totalValidators))
		p2p := NewHappyP2P()
		id := ID([]byte(fmt.Sprintf("%d", i)))

		var signer ecsigner
		signer.sign = func(data []byte) []byte {
			clone := make([]byte, len(id))
			copy(clone, id)
			return clone
		}
		signer.verify = func(data []byte, sig []byte) (ID, error) {
			return (ID)(sig), nil
		}
		signer.tcombine = func(data []byte, sigs [][]byte) []byte {
			return []byte(fmt.Sprintf("%d", len(sigs)))
		}
		signer.tVerify = func(data []byte, sigs []byte, ids []ID, quorum int32) bool {
			n, err := strconv.ParseUint(string(sigs), 10, 64)
			if err != nil {
				return false
			}

			return n >= uint64(quorum)
		}
		walPath := fmt.Sprintf("./wal/%d", i)
		defer os.RemoveAll(walPath)

		conf := Config{
			BlockInterval: time.Second * 3,
			ProposerID:    id,
			DataDir:       walPath,
			EcSigner:      &signer,
			DefaultBlockFunc: func() Block {
				hb := HappyBlock(0)
				return &hb
			},
		}
		db.StoreBlock(&genesis, nil)
		hs := New(db, p2p, conf)
		dbs = append(dbs, db)
		p2ps = append(p2ps, p2p)
		hss = append(hss, hs)
	}

	for i := range p2ps {
		p2p := p2ps[i]
		for j := range p2ps {
			if j != i {
				idj := ID([]byte(fmt.Sprintf("%d", j)))
				p2p.neibors[string(idj)] = p2ps[j]
			}
		}
	}

	for _, hs := range hss {
		err := hs.Start()
		if err != nil {
			t.Fatal("hs.Start failed", err)
		}
	}

	eb := uint64(10)
	for h := uint64(1); h < eb; h++ {
		fmt.Println("height", hss[0].ConsensusHeight(), "view", hss[0].ConsensusView())
		if hss[0].ConsensusHeight() == h && hss[0].ConsensusView() == 0 {
			leader := hss[h%totalValidators]
			blk := HappyBlock(h)
			fmt.Println("proposer", string(leader.conf.ProposerID))
			err := leader.Propose(context.Background(), &blk)
			if err != nil {
				t.Fatal("HotStuff.Send failed", err)
			}
		}

		for i, db := range dbs {
			err := hss[i].Wait(context.Background(), uint64(h))
			if err != nil {
				t.Fatal("Wait failed", h)
			}
			if db.Height() != uint64(h) {
				t.Fatal("consensus failed", h)
			}
			if *(db.getBlock(h).(*HappyBlock)) != HappyBlock(h) {
				t.Fatal("consensus failed", h)
			}
		}
		fmt.Printf("height %d passed\n", h)
	}

	for i, db := range dbs {
		err := hss[i].Wait(context.Background(), eb)
		if err != nil {
			t.Fatal("Wait failed", eb)
		}
		if db.Height() != eb {
			t.Fatal("consensus failed", eb)
		}
		if *(db.getBlock(eb).(*HappyBlock)) != EmptyBlock(eb) {
			t.Fatal("consensus failed at block", eb, "got", *(db.getBlock(eb).(*HappyBlock)), "expect empty", EmptyBlock(eb))
		}
	}

	for _, hs := range hss {
		err := hs.Stop()
		if err != nil {
			t.Fatal("hs.Stop failed", err)
		}
	}
	for _, db := range dbs {
		if len(db.subs) != 0 {
			t.Fatal("#db.subs != 0 after hs.Stop")
		}
	}
}
