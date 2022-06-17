--ct this file into sqlite3 into the old file cat this | sqlite3 old


ATTACH DATABASE 'blocks.sqlite' AS blocks;
ATTACH DATABASE 'transactions.sqlite' AS transactions;
ATTACH DATABASE  'logs.sqlite' AS logs;

--CREATE TABLE blocks.blocks(number BIGINT PRIMARY KEY);
CREATE TABLE blocks.blocks(number BIGINT PRIMARY KEY, hash varchar(32), parentHash varchar(32), uncleHash varchar(32), coinbase varchar(20), root varchar(32), txRoot varchar(32), receiptRoot varchar(32), bloom blob, difficulty varchar(32), gasLimit BIGINT, gasUsed BIGINT, time BIGINT, extra blob, mixDigest varchar(32), nonce BIGINT, uncles blob, size BIGINT, td varchar(32), baseFee varchar(32));
CREATE TABLE transactions.transactions(id INTEGER PRIMARY KEY AUTOINCREMENT, gas BIGINT, gasPrice BIGINT, hash varchar(32) UNIQUE, input blob, nonce BIGINT, recipient varchar(20), transactionIndex MEDIUMINT, value varchar(32), v SMALLINT, r varchar(32), s varchar(32), sender varchar(20), func varchar(4), contractAddress varchar(20), cumulativeGasUsed BIGINT, gasUsed BIGINT, logsBloom blob, status TINYINT, block BIGINT, type TINYINT, access_list blob, gasFeeCap varchar(32), gasTipCap varchar(32));
CREATE TABLE logs.event_logs(address varchar(20), topic0 varchar(32), topic1 varchar(32), topic2 varchar(32), topic3 varchar(32), data blob, block BIGINT, logIndex MEDIUMINT, transactionHash varchar(32), transactionIndex varchar(32), blockHash varchar(32), PRIMARY KEY (block, logIndex));
--CREATE TABLE logs.event_logs(address varchar(20));
--INSERT INTO blocks.blocks SELECT number FROM blocks;
INSERT INTO blocks.blocks SELECT number, hash, parentHash, uncleHash, coinbase, root, txRoot, receiptRoot, bloom, difficulty, gasLimit, gasUsed, time, extra, mixDigest, nonce, uncles, size, td, baseFee FROM blocks;
INSERT INTO transactions.transactions SELECT id, gas, gasPrice, hash, input, nonce, recipient, transactionIndex, value, v, r, s, sender, func, contractAddress, cumulativeGasUsed, gasUsed, logsBloom, status, block, type, access_list, gasFeeCap, gasTipCap FROM transactions;
INSERT INTO logs.event_logs SELECT address,  topic0, topic1, topic2, topic3, data, block, logIndex, transactionHash, transactionIndex, blockHash FROM event_logs;
--INSERT INTO logs.event_logs SELECT address FROM event_logs;

