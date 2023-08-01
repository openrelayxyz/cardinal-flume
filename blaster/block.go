package blaster

type BlastBlock struct {
	Number uint64
	Hash [32]byte
	ParentHash [32]byte
	UncleHash [32]byte
	Coinbase [20]byte
	Root [32]byte
	TxRoot [32]byte
	ReceiptRoot [32]byte
	Bloom []byte
	Difficulty uint64
	GasLimit uint64
	GasUsed  uint64
	Time uint64
	Extra []byte
	MixDigest [32]byte
	Nonce uint64
	Uncles []byte
	Size uint64
	Td [32]byte
	BaseFee [32]byte
	WithdrawalsHash [32]byte
}

// number=int64 hash=types.Hash parent hash=types.Hash unclehash=types.Hash
// coinbase=common.Address root=types.Hash tx root=types.Hash receiptRoot=types.Hash
// bloom=[]uint8 difficulty=int64 gaslimit=uint64 gasused=uint64
// time=uint64 extra=[]uint8 mixed=types.Hash nonce=int64
// uncleRLP=[]uint8 size=int td="func() []uint8" basefee=*big.Int whash=*types.Hash

// number      BIGINT PRIMARY KEY,
// hash        varchar(32) UNIQUE,
// parentHash  varchar(32),
// uncleHash   varchar(32),
// coinbase    varchar(20),
// root        varchar(32),
// txRoot      varchar(32),
// receiptRoot varchar(32),
// bloom       blob,
// difficulty  varchar(32),
// gasLimit    BIGINT,
// gasUsed     BIGINT,
// time        BIGINT,
// extra       blob,
// mixDigest   varchar(32),
// nonce       BIGINT,
// uncles      blob,
// size        BIGINT,
// td          varchar(32),
// baseFee varchar(32))

// pb.Number,
// pb.Hash,
// pb.ParentHash,
// header.UncleHash,
// header.Coinbase,
// header.Root,
// header.TxHash,
// header.ReceiptHash,
// compress(header.Bloom[:]),
// header.Difficulty.Int64(),
// header.GasLimit,
// header.GasUsed,
// header.Time,
// header.Extra,
// header.MixDigest,
// int64(binary.BigEndian.Uint64(header.Nonce[:])),
// uncleRLP,
// size,
// td.Bytes(),
// header.BaseFee,
// header.WithdrawalsHash,

// difficulty=int64 gaslimit=uint64 gasused=uint64 time=uint64