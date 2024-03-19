package private_channel

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"net"
	log "private_channel/logger"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	PirvatePackageMinBytes     int           = 10
	PrivatePackageSaveTimeout  time.Duration = 800 * time.Millisecond
	PrivatePackageMaxBytes     int           = 1400
	PrivatePackageOnePatchSize int           = 1024
	PrivatePackageLossBatchId                = ^uint16(0)
	PrivateMessageLossRetryMax uint8         = 5
	MaxHearbeatMiss                          = 30
	UdpHearbeatInterval                      = 10 * time.Second
	HeartbeatTid               uint64        = 0
)

type PrivatePackage struct {
	//36:sid； 8:flag(0-可靠；1-流式【如果是流失，去掉解析BatchCount；否则需要解析BatchCount】； tid:20
	Sid        uint64
	Flag       uint64
	Tid        uint64
	BatchId    uint16
	BatchCount uint16
	Content    []byte
}

type PrivateMessage struct {
	Sid                   uint64
	Tid                   uint64
	IsReliable            bool
	LossRetry             uint8
	IsStreaming           bool
	BizInfo               string
	LastTS                time.Time
	ExpectCount           uint32
	RealCount             uint32
	NextBatchId           uint16
	ContentPackageBatches map[uint16]*PrivatePackage
	Content               []byte
	Mu                    sync.RWMutex
}

type PUdpConn struct {
	sid           uint64
	recvSyncPmMap sync.Map
	sendSyncPmMap sync.Map
	conn          *net.UDPConn
	remoteAddr    *net.UDPAddr
	Done          chan struct{}
}

type BizFun func(uint64, bool, string, []byte, *PUdpConn) error

func DecodePrivatePackage(rawBytes []byte) (*PrivatePackage, error) {
	var pkg PrivatePackage
	if len(rawBytes) < PirvatePackageMinBytes {
		log.Errorf("not valid PrivatePackage, length must be at least %d bytes", PirvatePackageMinBytes)
		return nil, fmt.Errorf("not valid PrivatePackage, length must be at least %d bytes", PirvatePackageMinBytes)
	}
	buf := bytes.NewReader(rawBytes)

	var decodeSFT uint64
	if err := binary.Read(buf, binary.BigEndian, &decodeSFT); err != nil {
		return nil, err
	}

	pkg.Sid = (decodeSFT & (0xFFFFFFFFF0000000)) >> 28
	pkg.Flag = (decodeSFT & (0x000000000FF00000)) >> 20
	pkg.Tid = (decodeSFT & (0x00000000000FFFFF))
	if err := binary.Read(buf, binary.BigEndian, &pkg.BatchId); err != nil {
		return nil, err
	}
	if (pkg.Flag & 0b00000010) == 0 { //非流式
		if err := binary.Read(buf, binary.BigEndian, &pkg.BatchCount); err != nil {
			return nil, err
		}
	}
	pkg.Content = make([]byte, buf.Len())
	if err := binary.Read(buf, binary.BigEndian, pkg.Content); err != nil {
		return nil, err
	}

	return &pkg, nil
}

func encodePrivatePackage(pkg *PrivatePackage) ([]byte, error) {
	buf := new(bytes.Buffer)

	var sft uint64 = (pkg.Sid << 28) | ((pkg.Flag) << 20) | (pkg.Tid)
	if err := binary.Write(buf, binary.BigEndian, sft); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, pkg.BatchId); err != nil {
		return nil, err
	}
	if (pkg.Flag & 0b00000010) == 0 { //非流式
		if err := binary.Write(buf, binary.BigEndian, pkg.BatchCount); err != nil {
			return nil, err
		}
	} else {
	}
	if err := binary.Write(buf, binary.BigEndian, pkg.Content); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func createTid() int64 {
	return int64(rand.Intn(math.MaxUint32 >> 12))
}

func NewPudpConn(oneSid uint64, conn *net.UDPConn, addr *net.UDPAddr, bizFn BizFun) *PUdpConn {
	ph := &PUdpConn{
		sid:        oneSid,
		conn:       conn,
		remoteAddr: addr,
		Done:       make(chan struct{}),
	}
	go ph.HandleRecvPM(bizFn)
	go ph.HandleSendPM()
	return ph
}

func findLossBatchIds(pm *PrivateMessage, start, end int) string {
	var lossing string
	for i := start; i <= end; i++ {
		if _, exists := pm.ContentPackageBatches[uint16(i)]; !exists {
			lossing += fmt.Sprintf("%d|", i)
		}
	}
	return lossing
}

func requestLossPackage(pUdpConn *PUdpConn, pm *PrivateMessage, batchLossIdsStr string) error {
	batchLossBytes := []byte(batchLossIdsStr)
	lossPP := constructPrivatePackage(
		pm.Sid,
		0x00,
		pm.Tid,
		PrivatePackageLossBatchId,
		1,
		batchLossBytes,
	)
	return SendPrivatePackage(pUdpConn, lossPP)
}

func handleLossPackage(pUdpConn *PUdpConn, pmId uint64, lossBatchIdsStr string) error {
	actual, ok := pUdpConn.sendSyncPmMap.Load(pmId)
	if !ok {
		log.Errorf("not found pm:%v in sendSyncPmMap", pmId)
		return fmt.Errorf("not found pm:%v in sendSyncPmMap", pmId)
	}
	pm, ok := actual.(*PrivateMessage)
	if !ok {
		log.Errorf("%v Store value is not of type *PrivateMessage", pmId)
		return fmt.Errorf("%v Store value is not of type *PrivateMessage", pmId)
	}

	log.Info("lossBatchIdsStr:", lossBatchIdsStr)
	lossBatchIdsSlice := strings.Split(lossBatchIdsStr, "|")
	log.Info("lossBatchIdsSlice: ", lossBatchIdsSlice)
	for _, batchIdStr := range lossBatchIdsSlice {
		if len(batchIdStr) == 0 {
			continue
		}
		batchId, err := strconv.ParseUint(batchIdStr, 10, 32)
		if err != nil {
			log.Error(err)
			continue
		}
		onePP, ok := pm.ContentPackageBatches[uint16(batchId)]
		if ok {
			err := SendPrivatePackage(pUdpConn, onePP)
			if err != nil {
				log.Error(err)
				continue
			}

		} else {
			log.Warnf("Not found pm sid:%v pm tid: %v, batchId:%v", pm.Sid, pm.Tid, batchId)
		}
	}
	return nil
}
func (pUdpConn *PUdpConn) HandleRecvPM(bizFn BizFun) {
	for {
		select {
		case <-pUdpConn.Done:
			log.Info("HandleRecvPM quit!")
			return
		case <-time.After(time.Millisecond * 100):
			pUdpConn.recvSyncPmMap.Range(func(key, value interface{}) bool {
				pm, ok := value.(*PrivateMessage)
				if ok {
					if pm.IsStreaming { //流式处理
						log.Info("streaming doing")
						oneBatch, exists := pm.ContentPackageBatches[uint16(pm.NextBatchId)]
						log.Infof("pm.NextBatchId: %v, exists: %v", pm.NextBatchId, exists)
						if exists {
							delete(pm.ContentPackageBatches, pm.NextBatchId)
							go bizFn(pm.Sid, false, strconv.FormatUint(oneBatch.Tid, 10), oneBatch.Content, pUdpConn)
							pm.NextBatchId++
						} else {
							err := requestLossPackage(pUdpConn, pm, strconv.FormatUint(uint64(pm.NextBatchId), 10))
							if err != nil {
								log.Error(err)
							}
							if time.Since(pm.LastTS) > PrivatePackageSaveTimeout {
								log.Info("len(pm.ContentPackageBatches):", len(pm.ContentPackageBatches))
								if len(pm.ContentPackageBatches) == 0 {
									log.Warn("pm will be deleted: ", pm.Tid)
									pm.ContentPackageBatches = nil
									pUdpConn.recvSyncPmMap.Delete(key)
								} else {
									pm.NextBatchId++
								}
							}
						}

					} else { //非流式
						if pm.RealCount == pm.ExpectCount {
							pm.Mu.Lock()
							wholePM, bizInfo, pmContent := resamplePPToPM(pm)
							pUdpConn.recvSyncPmMap.Delete(key)
							pm.Mu.Unlock()
							log.Info(bizInfo)
							go bizFn(pm.Sid, wholePM, bizInfo, pmContent, pUdpConn)

						} else {
							if time.Since(pm.LastTS) > PrivatePackageSaveTimeout { //超时时间没有收到数据
								if !pm.IsReliable || pm.LossRetry > PrivateMessageLossRetryMax { //不可靠传输，直接处理掉数据
									log.Info("Deal not reliable")
									pm.Mu.Lock()
									wholePM, bizInfo, pmContent := resamplePPToPM(pm)
									pm.Mu.Unlock()
									pUdpConn.recvSyncPmMap.Delete(key)
									go bizFn(pm.Sid, wholePM, bizInfo, pmContent, pUdpConn)

								} else { //可靠传输
									log.Info("Deal reliable")
									pm.LastTS = time.Now().Add(PrivatePackageSaveTimeout * time.Duration(pm.LossRetry))
									pm.LossRetry++
									if (pm.ExpectCount > 10 && pm.RealCount > uint32(float32(pm.ExpectCount)*0.7)) || (pm.ExpectCount <= 10) {
										batchLossIdsStr := findLossBatchIds(pm, 0, int(pm.ExpectCount-1))
										log.Info("batchLossIdStr: ", batchLossIdsStr)
										err := requestLossPackage(pUdpConn, pm, batchLossIdsStr)
										if err != nil {
											log.Error(err)
										}
									}

								}
							}
						}

					}

				} else {
					log.Warnf("pm:%v is not of type *PrivateMessage", key)
				}
				return true

			})

		}
	}
}
func (pUdpConn *PUdpConn) HandleSendPM() {
	for {
		select {
		case <-pUdpConn.Done:
			log.Info("HandleSendPM quit!")
			return
		case <-time.After(time.Millisecond * 500):
			pUdpConn.sendSyncPmMap.Range(func(key, value interface{}) bool {
				pm, ok := value.(*PrivateMessage)
				if ok {
					if time.Since(pm.LastTS) > 5*PrivatePackageSaveTimeout {
						log.Infof("delete pm:%v from SendPM\n", key)
						pUdpConn.sendSyncPmMap.Delete(key)
					}

				} else {
					log.Warnf("pm:%v is not of type *PrivateMessage", key)
				}
				return true
			})

		}
	}
}

func (pUdpConn *PUdpConn) UdpConnStop() {
	close(pUdpConn.Done)
}

func (pUdpConn *PUdpConn) RecvPrivatePackage(pp *PrivatePackage) error {
	start := time.Now()
	if pp.BatchId == PrivatePackageLossBatchId {
		//del loss batchid
		log.Info("deal loss pp:", string(pp.Content))
		err := handleLossPackage(pUdpConn, pp.Tid, string(pp.Content))
		if err != nil {
			return err
		}

	} else {
		actual, _ := pUdpConn.recvSyncPmMap.LoadOrStore(pp.Tid, &PrivateMessage{
			Sid:                   pp.Sid,
			Tid:                   pp.Tid,
			IsReliable:            (pp.Flag & 0b00000001) > 0,
			IsStreaming:           (pp.Flag & 0b00000010) > 0,
			LastTS:                time.Now(),
			ContentPackageBatches: make(map[uint16]*PrivatePackage),
		})

		pm, ok := actual.(*PrivateMessage)
		if !ok {
			return fmt.Errorf("%v Store value is not of type *PrivateMessage", pp.Tid)
		}
		pm.Mu.Lock()
		defer pm.Mu.Unlock()

		if pm.IsStreaming {
			pm.ContentPackageBatches[(pp.BatchId)] = pp
		} else {
			pm.LastTS = time.Now()
			pm.ExpectCount = uint32(pp.BatchCount)
			if pp.BatchId == 0 {
				pm.BizInfo = string(pp.Content)
			} else {
				pm.ContentPackageBatches[(pp.BatchId)] = pp
			}
			pm.RealCount++

		}

		log.Infof("pp/pm/d: (%v:%v:%v)/(%v,%v,%v)/%v\n",
			pp.Tid, pp.BatchId, pp.BatchCount,
			pm.Tid, pm.RealCount, pp.BatchCount,
			time.Since(start).Milliseconds())

	}

	return nil

}

func resamplePPToPM(pm *PrivateMessage) (bool, string, []byte) {
	keys := make([]int, 0, pm.RealCount)
	for k := range pm.ContentPackageBatches {
		keys = append(keys, int(k))
	}

	sort.Ints(keys)
	pmContent := make([]byte, 0)
	bizInfo := pm.BizInfo
	for _, k := range keys {
		pmContent = append(pmContent, pm.ContentPackageBatches[uint16(k)].Content...)
	}
	wholePM := false
	ppLoss := pm.ExpectCount - pm.RealCount
	log.Infof("PM %v ExpectCount/RealCount %v/%v\n", pm.Tid, pm.ExpectCount, pm.RealCount)
	if ppLoss == 0 {
		wholePM = true
	}

	//
	pm.Tid = 0
	pm.ContentPackageBatches = nil
	pm.RealCount = 0

	return wholePM, bizInfo, pmContent

}
func constructPrivatePackage(sid uint64, flag uint64, tid uint64, batchId uint16, batchCount uint16, content []byte) *PrivatePackage {
	return &PrivatePackage{
		Sid:        sid,
		Tid:        tid,
		Flag:       flag,
		BatchId:    batchId,
		BatchCount: batchCount,
		Content:    content,
	}

}

func SendPrivatePackage(pUdpConn *PUdpConn, pp *PrivatePackage) error {
	log.Infof("remoteAddr:%v,pp.Tid/pp.BatchId:%v/%v/%v", pUdpConn.remoteAddr, pp.Tid, pp.BatchId, len(pp.Content))
	encodePP, err := encodePrivatePackage(pp)
	if err != nil {
		return nil
	}
	encryptEncodePP, err := EncryptAES(encodePP)
	if err != nil {
		return nil
	}
	if pUdpConn.remoteAddr == nil {
		_, err := pUdpConn.conn.Write(encryptEncodePP)
		if err != nil {
			return err
		}

	} else {
		_, err := pUdpConn.conn.WriteToUDP(encryptEncodePP, pUdpConn.remoteAddr)
		if err != nil {
			return nil
		}

	}
	return nil

}

func (pUdpConn *PUdpConn) SendPrivateMessageStream(batchId uint16, pm *PrivateMessage) error {
	if pm == nil {
		log.Error("pm is nil when sending(stream)")
		return errors.New("pm is nil when sending(stream)")
	}
	tid := pm.Tid
	if tid == 0 {
		log.Error("pm.Tid is 0 when sending(stream)")
		return errors.New("pm.Tid is 0 when sending(stream)")
	}

	streamFlag := 0b00000010
	if pm.IsReliable {
		streamFlag = streamFlag | 0b00000001
	}
	streamPP := constructPrivatePackage(pm.Sid, uint64(streamFlag), uint64(tid), batchId, 0, []byte(pm.BizInfo))
	err := SendPrivatePackage(pUdpConn, streamPP)
	if err != nil {
		return err
	}

	if pm.IsReliable {
		//construct sendPM
		actual, _ := pUdpConn.sendSyncPmMap.LoadOrStore(tid, &PrivateMessage{
			Sid:                   pm.Sid,
			Tid:                   tid,
			ContentPackageBatches: make(map[uint16]*PrivatePackage),
			LastTS:                time.Now(),
			IsReliable:            pm.IsReliable,
			IsStreaming:           pm.IsStreaming,
		})

		sendPm, ok := actual.(*PrivateMessage)
		log.Info("sendPm: ", sendPm)
		if !ok {
			log.Errorf("%v Store value is not of type *PrivateMessage", pm.Tid)
			return fmt.Errorf("%v Store value is not of type *PrivateMessage", pm.Tid)
		}

		sendPm.ContentPackageBatches[streamPP.BatchId] = streamPP
	}
	return nil

}

func (pUdpConn *PUdpConn) SendPrivateMessage(pm *PrivateMessage) error {
	if pm == nil {
		log.Error("pm is nil")
		return errors.New("pm is nil")
	}

	tid := createTid()

	sendFlag := 0b00000000
	if pm.IsReliable {
		sendFlag = sendFlag | 0b00000001
	}

	dataBytesLen := len(pm.Content)
	batchCount := int(math.Ceil(float64(dataBytesLen) / float64(PrivatePackageOnePatchSize)))
	log.Info("pm.BizInfo: ", pm.BizInfo)
	firstPP := constructPrivatePackage(pm.Sid, uint64(sendFlag), uint64(tid), 0, uint16(batchCount+1), []byte(pm.BizInfo))

	//construct sendPM
	sendPm := &PrivateMessage{
		Sid:                   firstPP.Sid,
		Tid:                   firstPP.Tid,
		ContentPackageBatches: make(map[uint16]*PrivatePackage),
		LastTS:                time.Now(),
		IsReliable:            pm.IsReliable,
	}
	sendPm.ContentPackageBatches[firstPP.BatchId] = firstPP
	//send firstPP
	err := SendPrivatePackage(pUdpConn, firstPP)
	if err != nil {
		return err
	}

	for i := 0; i < batchCount; i++ {
		start := PrivatePackageOnePatchSize * i
		end := start + PrivatePackageOnePatchSize

		if end > dataBytesLen {
			end = dataBytesLen
		}

		dataPP := constructPrivatePackage(
			pm.Sid,
			uint64(sendFlag),
			uint64(tid),
			uint16(i+1),
			uint16(batchCount+1),
			[]byte(pm.Content[start:end]),
		)
		sendPm.ContentPackageBatches[dataPP.BatchId] = dataPP
		sendPm.LastTS = time.Now()
		err := SendPrivatePackage(pUdpConn, dataPP)
		if err != nil {
			log.Error(err)
		}
		if batchCount > 10 {
			time.Sleep(time.Millisecond * (time.Duration(PrivatePackageMaxBytes / 100)))
		}

	}

	if sendPm.IsReliable {
		log.Infof("store pm sid:%v tid:%v to sendSyncPMMap", sendPm.Sid, sendPm.Tid)
		pUdpConn.sendSyncPmMap.Store(sendPm.Tid, sendPm)

	}

	return nil
}
