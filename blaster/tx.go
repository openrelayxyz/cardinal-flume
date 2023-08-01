package blaster

type blastTx struct {
	Id uint64
	Gas uint64
	GasPrice uint64
	Hash [32]byte
	Input []byte
	Nonce uint64
	Recipient [20]byte
	TransactionIndex uint64
	Value [32]byte
	V uint64
	R [32]byte
	S [32]byte
	Sender [20]byte
	Func [4]byte
	ContractAddress [20]byte
	CumulativeGasUsed uint64
	GasUsed uint64
	LogsBloom []byte
	Status uint64
	Block uint64
	Type uint64
	Access_list []byte
	GasFeeCap [32]byte
    GasTipCap [32]byte
}

// id INTEGER PRIMARY KEY AUTOINCREMENT,
// 		    gas BIGINT,
// 		    gasPrice BIGINT,
// 		    hash varchar(32),
// 		    input blob,
// 		    nonce BIGINT,
// 		    recipient varchar(20),
// 		    transactionIndex MEDIUMINT,
// 		    value varchar(32),
// 		    v SMALLINT,
// 		    r varchar(32),
// 		    s varchar(32),
// 		    sender varchar(20),
// 		    func varchar(4),
// 		    contractAddress varchar(20),
// 		    cumulativeGasUsed BIGINT,
// 		    gasUsed BIGINT,
// 		    logsBloom blob,
// 		    status TINYINT,
// 		    block BIGINT,
// 		    type TINYINT,
// 		    access_list blob,
// 		    gasFeeCap varchar(32),
// 		    gasTipCap varchar(32))