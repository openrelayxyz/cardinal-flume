package blaster

// #include "blaster.h"
import "C"

import (
	"unsafe"
	"sync"
	"time"
	"os"
	"compress/gzip"
	
	"github.com/shirou/gopsutil/mem"
	log "github.com/inconshreveable/log15"
)

type BlockBlaster struct {
	DB unsafe.Pointer
	TxQuit chan struct{}
	LogsQuit chan struct{}
	WithdrawalsQuit chan struct{}
	Lock *sync.Mutex
}

type WithdrawalBlaster struct {
	DB unsafe.Pointer
	Lock *sync.Mutex
	Quit chan struct{}
}

type TxBlaster struct {
	DB unsafe.Pointer
	Lock *sync.Mutex
	Quit chan struct{}
	Updates string
	MIFile *gzip.Writer
}

type LogBlaster struct {
	DB unsafe.Pointer
	Lock *sync.Mutex
	Quit chan struct{}
	Updates string
	MIFile *gzip.Writer
}

func NewBlasterBlockIndexer(dataBase string, txQuitChan, logsQuitChan, withdrawalsQuitChan chan struct{}) *BlockBlaster {

	db := C.new_sqlite_block_blaster(C.CString(dataBase))

	b := &BlockBlaster{
		DB: db,
		TxQuit: txQuitChan,
		LogsQuit: logsQuitChan,
		WithdrawalsQuit: withdrawalsQuitChan,
		Lock: new(sync.Mutex),
	}
	log.Info("block blaster initialized")
	return b
}

func (b *BlockBlaster) MonitorMemory() {
	go func() {
		time.Sleep(1 * time.Minute) // I figure the memory might spike when the application starts up so we'll give it a minute to warm up
		for {
			sm, err := mem.SwapMemory()
			if err != nil {
				log.Error("memory swap stat error", "err", err)
			}
			vm, err := mem.VirtualMemory()
			if err != nil {
				log.Error("memory virtual stat error", "err", err)
			}
			if vm.UsedPercent > 97 || sm.UsedPercent > 97 {
				log.Error("flume blaster crossed memory threshold, exiting", "virtual used", vm.UsedPercent, "swap used", sm.UsedPercent)
				b.SendTxQuit()
				b.SendLogsQuit()
				b.SendWtdQuit()
				time.Sleep(500 * time.Millisecond)
				b.Close()
				os.Exit(1)
			} 
			time.Sleep(1 * time.Second)
        }
    }()
}

func (b *BlockBlaster) SendWtdQuit() {
	log.Info("called quit on withdrawals from block blaster")
	b.WithdrawalsQuit <- struct{}{}
}

func (b *BlockBlaster) SendTxQuit() {
	log.Info("called quit on txns from block blaster")
	b.TxQuit <- struct{}{}
}

func (b *BlockBlaster) SendLogsQuit() {
	log.Info("called quit on logs from block blaster")
	b.LogsQuit <- struct{}{}
}

func NewBlasterWithdrawalIndexer(dataBase string, quitChan chan struct{}) *WithdrawalBlaster {
	db := C.new_sqlite_withdrawal_blaster(C.CString(dataBase))

	b := &WithdrawalBlaster{
		DB: db,
		Lock: new(sync.Mutex),
		Quit: quitChan,
	}
	log.Info("withdrawal blaster initialized")
	return b
}

func (b *WithdrawalBlaster) ListenForWtdClose() {
	go func() {
		for {
			select {
			case <- b.Quit:
				log.Info("Caught shutdown signal in withdrawals")
                b.Close()
            }
        }
    }()
}

func NewBlasterTxIndexer(dataBase string, quitChan chan struct{}, updates string) *TxBlaster {

	var writer *gzip.Writer
	currentTime := time.Now()
	timestamp := currentTime.Format("2006-01-02_15-04-05")
	fileName := updates + "tx" + timestamp + ".sql.gz"

	
	file, err := os.Create(fileName)
	if err != nil {
		log.Error("Error opening tx updates file, TxBlaster is nil", "err", err)
		return nil
	}
	writer = gzip.NewWriter(file)
	
	db := C.new_sqlite_tx_blaster(C.CString(dataBase))
	b := &TxBlaster{
		DB: db,
		Lock: new(sync.Mutex),
		Quit: quitChan,
		Updates: updates,
		MIFile: writer,
	}
	log.Info("transaction blaster initialized")
	return b
}

func (b *TxBlaster) ListenForTxClose() {
	go func() {
		for {
			select {
			case <- b.Quit:
				log.Info("Caught shutdown signal in txns")
                b.Close()
            }
        }
    }()
}

func NewBlasterLogIndexer(dataBase string, quitChan chan struct{}, updates string) *LogBlaster {

	var writer *gzip.Writer
	currentTime := time.Now()
	timestamp := currentTime.Format("2006-01-02_15-04-05")
	fileName := updates + "logs" + timestamp + ".sql.gz"

	
	file, err := os.Create(fileName)
	if err != nil {
		log.Error("Error opening lg updates file, LogBlaster is nil", "err", err)
		return nil
	}
	writer = gzip.NewWriter(file)

	db := C.new_sqlite_log_blaster(C.CString(dataBase))

	b := &LogBlaster{
		DB: db,
		Lock: new(sync.Mutex),
		Quit: quitChan,
		Updates: updates,
		MIFile: writer,
	}
	log.Info("log blaster initialized")
	return b
}

func (b *LogBlaster) ListenForLogsClose() {
	go func() {
        for {
            select {
			case <- b.Quit:
				log.Info("Caught shutdown signal in logs")
                b.Close()
            }
        }
    }()
}
 
func (b *BlockBlaster) Close() {
	defer log.Info("close called on blocks blaster")
	b.Lock.Lock()
	C.sqbb_close(b.DB)
}

func (b *WithdrawalBlaster) Close() {
	defer log.Info("close called on withdrawals blaster")
	b.Lock.Lock()
	C.sqwb_close(b.DB)
}

func (b *TxBlaster) Close() {
	defer log.Info("close called on tx blaster")
	b.Lock.Lock()
	b.MIFile.Close()
	C.sqtb_close(b.DB)
}

func (b *LogBlaster) Close() {
	defer log.Info("close called on log blaster")
	b.Lock.Lock()
	b.MIFile.Close()
	C.sqlb_close(b.DB)
}