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
	}
	HappyP2P struct {
		neibors map[string]*HappyP2P
		msgCh   chan *Msg
	}
)

func (b HappyBlock) Default() Block {
	hb := HappyBlock(0)
	return &hb
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
func (b *HappyBlock) Validate() error {
	return nil
}
func (b *HappyBlock) Height() uint64 {
	return uint64(*b) & 0xffffffff
}

func (b *HappyBlock) Hash() []byte {
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

func (db *HappyDB) IsBlockProposableBy(blk Block, id ID) bool {
	realBlk := blk.(*HappyBlock)
	n, err := strconv.ParseUint(string(id), 10, 64)
	if err != nil {
		return false
	}

	return n == blk.Height()%uint64(db.validators) || *realBlk == EmptyBlock(blk.Height())
}

func (db *HappyDB) GetBlock(n uint64) Block {
	if int(n) >= len(db.Blocks) {
		return nil
	}

	return &db.Blocks[n]
}

func (db *HappyDB) StoreBlock(blk Block, lockQc, commitQC *QC) error {
	hp := blk.(*HappyBlock)
	if hp.Height() != uint64(len(db.Blocks)) {
		return fmt.Errorf("invalid block")
	}

	db.Blocks = append(db.Blocks, *hp)
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
	tVerify  func(data []byte, sigs []byte, quorum int32) bool
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

func (s *ecsigner) TVerify(data []byte, sigs []byte, quorum int32) bool {
	return s.tVerify(data, sigs, quorum)
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
		signer.tVerify = func(data []byte, sigs []byte, quorum int32) bool {
			n, err := strconv.ParseUint(string(sigs), 10, 64)
			if err != nil {
				return false
			}

			return n >= uint64(quorum)
		}
		walPath := fmt.Sprintf("./wal/%d", i)
		defer os.RemoveAll(walPath)

		conf := Config{
			BlockInterval:     time.Second * 3,
			SyncCheckInterval: time.Second * 3,
			ProposerID:        id,
			WalPath:           walPath,
			ValidatorIndex: func(height uint64, peer ID) int {
				id, err := strconv.ParseUint(string(peer), 10, 64)
				if err != nil {
					return -1
				}
				if id < totalValidators {
					return int(id)
				} else {
					return -1
				}
			},
			SelectLeader: func(height, view uint64) ID {
				return ID([]byte(fmt.Sprintf("%d", (height+view)%totalValidators)))
			},
			EmptyBlock: func(height uint64) Block {
				b := EmptyBlock(height)
				return &b
			},
			ValidatorCount: func(height uint64) int32 {
				return int32(totalValidators)
			},
			EcSigner: &signer,
		}
		hs := New(&genesis, db, p2p, conf)
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
		fmt.Println("height", hss[0].Height(), "view", hss[0].View())
		if hss[0].Height() == h && hss[0].View() == 0 {
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
			if *(db.GetBlock(h).(*HappyBlock)) != HappyBlock(h) {
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
		if *(db.GetBlock(eb).(*HappyBlock)) != EmptyBlock(eb) {
			t.Fatal("consensus failed at block", eb, "got", *(db.GetBlock(eb).(*HappyBlock)), "expect empty", EmptyBlock(eb))
		}
	}

}
