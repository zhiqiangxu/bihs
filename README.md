# bihs, a bivalent variant for hotstuff protocol

**bihs** differs from hotstuff in two aspects:

1. the votes are splitted into views like pbft
2. the possible block for each height is bivalent, either the one proposed by current leader(which can be calculated after the confirmation of the previous block), or a deterministic empty block.

So **bihs** doesn't have the requirement to know leader for future block, it's as safe as pbft, while the complexity is still `O(n)`.

# Usage

```golang
var (
    genesis Block
    store StateDB
    p2p P2P
    conf Config
)

# all you need to do is to provide the implementations for Block/StateDB/P2P interfaces

genesis = ...
store = ...
p2p = ...

hs := New(genesis , store , p2p , conf)
hs.Start()

# after this, you can propose blocks when it's your turn

hs.Propose(someBlk)

# you can also wait for the confirmation of some block easily

hs.Wait(context.Background(), height)
```

## Demo

[Here](https://github.com/zhiqiangxu/bihs/blob/master/bihs_test.go#L135)