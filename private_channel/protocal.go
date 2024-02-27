package private_channel

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"net"
	log "private_channel/logger"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	PrivatePackageMagicNumber  uint8         = 0x88
	PirvatePackageMinBytes     int           = 28
	PrivatePackageSaveTimeout  time.Duration = 800 * time.Millisecond
	PrivatePackageMaxBytes     int           = 1024
	PrivatePackageOnePatchSize int           = 256
	PrivatePackageLossBatchId                = ^uint32(0)
	PrivateMessageLossRetryMax uint8         = 5
)

type PrivatePackage struct {
	MagicNumber     uint8
	Tid             uint64
	BatchId         uint32
	BatchCount      uint32
	Length          uint32
	DataTotalLength uint32
	Timestamp       int64
	IsReliable      uint8
	Content         []byte
}

type PrivateMessage struct {
	Tid                   uint64
	BizInfo               string
	LastTS                time.Time
	ExpectCount           uint32
	RealCount             uint32
	ContentPackageBatches map[uint32]*PrivatePackage
	Content               []byte
	Mu                    sync.RWMutex
	IsReliable            uint8
}

type PUdpConn struct {
	recvSyncPmMap sync.Map
	sendSyncPmMap sync.Map
	conn          *net.UDPConn
	remoteAddr    *net.UDPAddr
	Done          chan struct{}
}

type BizFun func(bool, string, []byte, *PUdpConn) error

func DecodePrivatePackage(rawBytes []byte) (*PrivatePackage, error) {
	var pkg PrivatePackage
	if len(rawBytes) < PirvatePackageMinBytes {
		return nil, fmt.Errorf("not valid PrivatePackage, length must be at least %d bytes", PirvatePackageMinBytes)
	}
	buf := bytes.NewReader(rawBytes)
	if err := binary.Read(buf, binary.BigEndian, &pkg.MagicNumber); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.BigEndian, &pkg.Tid); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.BigEndian, &pkg.BatchId); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.BigEndian, &pkg.BatchCount); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.BigEndian, &pkg.Length); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.BigEndian, &pkg.DataTotalLength); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.BigEndian, &pkg.Timestamp); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.BigEndian, &pkg.IsReliable); err != nil {
		return nil, err
	}
	if pkg.Length > uint32(len(rawBytes)) {
		return nil, fmt.Errorf("pkg.Length %d is out of bound", pkg.Length)
	}
	if pkg.Length > uint32(PrivatePackageMaxBytes) {
		return nil, fmt.Errorf("pkg.Length %d exceed the max bytes %d", pkg.Length, PrivatePackageMaxBytes)
	}
	pkg.Content = make([]byte, pkg.Length)
	if err := binary.Read(buf, binary.BigEndian, &pkg.Content); err != nil {
		return nil, err
	}

	return &pkg, nil
}

func EncodePrivatePackage(pkg *PrivatePackage) ([]byte, error) {
	buf := new(bytes.Buffer)

	if err := binary.Write(buf, binary.BigEndian, pkg.MagicNumber); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, pkg.Tid); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, pkg.BatchId); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, pkg.BatchCount); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, pkg.Length); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, pkg.DataTotalLength); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, pkg.Timestamp); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, pkg.IsReliable); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, pkg.Content); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func createTid() int64 {
	now := time.Now()
	const format = "20060102150405.999"
	formattedTime := now.Format(format)
	formattedTime = strings.ReplaceAll(formattedTime, ".", "")
	tid, _ := strconv.ParseInt(formattedTime, 10, 64)
	return tid

}

func NewPudpConn(conn *net.UDPConn, addr *net.UDPAddr, bizFn BizFun) *PUdpConn {
	ph := &PUdpConn{
		conn:       conn,
		remoteAddr: addr,
	}
	go ph.HandleRecvPM(bizFn)
	go ph.HandleSendPM()
	return ph
}

func findLossBatchIds(pm *PrivateMessage, start, end int) string {
	var lossing string
	for i := start; i <= end; i++ {
		if _, exists := pm.ContentPackageBatches[uint32(i)]; !exists {
			lossing += fmt.Sprintf("%d|", i)
		}
	}
	return lossing
}

func requestLossPackage(pUdpConn *PUdpConn, pm *PrivateMessage, batchLossIdsStr string) error {
	batchLossBytes := []byte(batchLossIdsStr)
	lossPP := ConstructPrivatePackage(
		pm.Tid,
		PrivatePackageLossBatchId,
		1,
		uint32(len(batchLossBytes)),
		uint32(len(batchLossBytes)),
		0,
		batchLossBytes,
	)
	return SendPrivatePackage(pUdpConn, lossPP)
}

func handleLossPackage(pUdpConn *PUdpConn, pmId uint64, lossBatchIdsStr string) error {
	actual, ok := pUdpConn.sendSyncPmMap.Load(pmId)
	if !ok {
		return fmt.Errorf("not found pm:%v in sendSyncPmMap", pmId)
	}
	pm, ok := actual.(*PrivateMessage)
	if !ok {
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
		onePP, ok := pm.ContentPackageBatches[uint32(batchId)]
		if ok {
			err := SendPrivatePackage(pUdpConn, onePP)
			if err != nil {
				log.Error(err)
				continue
			}

		} else {
			log.Warnf("Not found pm:%v,batchId:%v", pm.Tid, batchId)
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
					if pm.RealCount == pm.ExpectCount {
						pm.Mu.Lock()
						wholePM, bizInfo, pmContent := resamplePPToPM(pm)
						pm.Mu.Unlock()
						bizFn(wholePM, bizInfo, pmContent, pUdpConn)
						pUdpConn.recvSyncPmMap.Delete(key)

					} else {
						if time.Since(pm.LastTS) > PrivatePackageSaveTimeout {
							if pm.IsReliable == 0 || pm.IsReliable > PrivateMessageLossRetryMax {
								log.Info("Deal not reliable")
								pm.Mu.Lock()
								wholePM, bizInfo, pmContent := resamplePPToPM(pm)
								pm.Mu.Unlock()
								bizFn(wholePM, bizInfo, pmContent, pUdpConn)
								pUdpConn.recvSyncPmMap.Delete(key)

							} else {
								log.Info("Deal reliable")
								pm.IsReliable++ // flag have deal
								pm.LastTS = time.Now()
								if (pm.ExpectCount > 10 && pm.RealCount > uint32(float32(pm.ExpectCount)*0.7)) || (pm.ExpectCount <= 10) {
									batchLossIdsStr := findLossBatchIds(pm, 1, int(pm.ExpectCount-1))
									log.Info("batchLossIdStr: ", batchLossIdsStr)
									err := requestLossPackage(pUdpConn, pm, batchLossIdsStr)
									if err != nil {
										log.Error(err)
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

func (pUdpConn *PUdpConn) RecvPrivatePackage(rawBytes []byte) error {
	start := time.Now()

	if rawBytes == nil {
		return errors.New("rawBytes is nil")
	}

	if len(rawBytes) == 0 {
		return errors.New("len(rawBytes) is 0")
	}

	pp, err := DecodePrivatePackage(rawBytes)
	if err != nil {
		return err
	}

	actual, _ := pUdpConn.recvSyncPmMap.LoadOrStore(pp.Tid, &PrivateMessage{
		Tid:                   pp.Tid,
		LastTS:                time.Now(),
		ExpectCount:           pp.BatchCount,
		ContentPackageBatches: make(map[uint32]*PrivatePackage),
		IsReliable:            pp.IsReliable,
	})
	pm, ok := actual.(*PrivateMessage)
	if !ok {
		return fmt.Errorf("%v Store value is not of type *PrivateMessage", pp.Tid)
	}

	if pp.BatchId == PrivatePackageLossBatchId {
		//del loss batchid
		log.Info("deal loss pp:", string(pp.Content))
		err := handleLossPackage(pUdpConn, pp.Tid, string(pp.Content))
		if err != nil {
			return err
		}

	} else {
		pm.Mu.Lock()
		defer pm.Mu.Unlock()

		pm.LastTS = time.Now()

		if pp.BatchId == 0 {
			pm.BizInfo = string(pp.Content)
		} else {
			pm.ContentPackageBatches[pp.BatchId] = pp
		}
		pm.RealCount++

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
		pmContent = append(pmContent, pm.ContentPackageBatches[uint32(k)].Content...)
	}
	wholePM := false
	ppLoss := pm.ExpectCount - pm.RealCount
	log.Infof("PM ExpectCount/RealCount %v/%v\n", pm.ExpectCount, pm.RealCount)
	if ppLoss == 0 {
		wholePM = true
	}

	//
	pm.Tid = 0
	pm.ContentPackageBatches = nil
	pm.RealCount = 0

	return wholePM, bizInfo, pmContent

}
func ConstructPrivatePackage(tid uint64, batchId uint32, batchCount uint32, length uint32, dataTotalLength uint32, isReliable uint8, content []byte) *PrivatePackage {
	return &PrivatePackage{
		MagicNumber:     PrivatePackageMagicNumber,
		Tid:             tid,
		BatchId:         batchId,
		BatchCount:      batchCount,
		Length:          length,
		DataTotalLength: dataTotalLength,
		Timestamp:       time.Now().Unix(),
		IsReliable:      isReliable,
		Content:         content,
	}

}

func SendPrivatePackage(pUdpConn *PUdpConn, pp *PrivatePackage) error {
	log.Infof("pp.Tid/pp.BatchId:%v/%v", pp.Tid, pp.BatchId)
	encodePP, err := EncodePrivatePackage(pp)
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

func (pUdpConn *PUdpConn) SendPrivateMessage(pm *PrivateMessage) error {
	if pm == nil {
		return errors.New("pm is nil")
	}

	dataBytesLen := len(pm.Content)
	batchCount := int(math.Ceil(float64(dataBytesLen) / float64(PrivatePackageOnePatchSize)))
	tid := createTid()

	firstPP := ConstructPrivatePackage(uint64(tid), 0, uint32(batchCount+1), uint32(len(pm.BizInfo)), uint32(dataBytesLen), pm.IsReliable, []byte(pm.BizInfo))

	//construct sendPM
	sendPm := &PrivateMessage{
		Tid:                   firstPP.Tid,
		ContentPackageBatches: make(map[uint32]*PrivatePackage),
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

		dataPP := ConstructPrivatePackage(
			uint64(tid),
			uint32(i+1),
			uint32(batchCount+1),
			uint32(end-start),
			uint32(dataBytesLen),
			pm.IsReliable,
			[]byte(pm.Content[start:end]),
		)
		sendPm.ContentPackageBatches[dataPP.BatchId] = dataPP
		sendPm.LastTS = time.Now()
		err := SendPrivatePackage(pUdpConn, dataPP)
		if err != nil {
			log.Error(err)
		}
		if batchCount > 10 {
			time.Sleep(time.Millisecond * (time.Duration(PrivatePackageMaxBytes / 10)))
		}

	}

	if sendPm.IsReliable > 0 {
		log.Infof("store pm:%v to sendSyncPMMap", sendPm.Tid)
		pUdpConn.sendSyncPmMap.Store(sendPm.Tid, sendPm)

	}

	return nil
}
