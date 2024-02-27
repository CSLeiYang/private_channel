package private_channel

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	log "private_channel/logger"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	PrivatePackageMagicNumber  uint8 = 0x88
	PirvatePackageMinBytes     int   = 28
	PrivatePackageSaveMaxMS    int   = 800
	PrivatePackageMaxBytes     int   = 1024
	PrivatePackageOnePatchSize int   = 256
)

type PrivatePackage struct {
	MagicNumber     uint8
	Tid             uint64
	BatchId         uint32
	BatchCount      uint32
	Length          uint32
	DataTotalLength uint32
	Timestamp       int64
	Content         []byte
}

type PrivateMessage struct {
	Tid                   uint64
	BizInfo               string
	LastTS                int64
	ExpectCount           uint32
	RealCount             uint32
	ContentPackageBatches map[uint32]*PrivatePackage
	Content               []byte
}

type PrivateMessageHandler struct {
	privateMessages []*PrivateMessage
	lock            sync.RWMutex
	mapTidToPostion map[int64]int
	Done            chan struct{}
}

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

func findPostion(pp *PrivatePackage, pmHandler *PrivateMessageHandler) (int, bool) {
	currentUnixTime := time.Now().UnixMilli()
	pmHandler.lock.RLock()
	defer pmHandler.lock.RUnlock()

	for i, pm := range pmHandler.privateMessages {
		if pm != nil && pp.Tid == pm.Tid {
			return i, true
		}
	}
	for i, pm := range pmHandler.privateMessages {
		if pm == nil {
			pmHandler.privateMessages[i] = &PrivateMessage{}
			return i, true
		}
	}
	for i, pm := range pmHandler.privateMessages {
		if pm.Tid == 0 {
			return i, true
		}
	}
	for i, pm := range pmHandler.privateMessages {
		if pm.LastTS-currentUnixTime > int64(PrivatePackageSaveMaxMS) {
			return i, true
		}
	}
	return -1, false
}

func NewPrivateMessageHandler(count int) *PrivateMessageHandler {
	ph := &PrivateMessageHandler{
		privateMessages: make([]*PrivateMessage, count),
		mapTidToPostion: make(map[int64]int, count),
	}
	go ph.CronTaskStart()
	return ph
}

func (hander *PrivateMessageHandler) CronTaskStart() {
	for {
		select {
		case <-hander.Done:
			log.Info("cronTask quit!")
			return
		case <-time.After(time.Millisecond * 200):
			currentTime := time.Now().UnixMilli()
			for _, pm := range hander.privateMessages {
				if pm != nil && pm.Tid > 0 && pm.LastTS != 0 && (currentTime-pm.LastTS) > int64(PrivatePackageSaveMaxMS) {
					log.Infof("Deal pm.Tid: %v, e: %v\n", pm.Tid, (currentTime - pm.LastTS))
					wholePM, bizInfo, pmContent := resamplePPToPM(pm)
					HandlePEvent(wholePM, bizInfo, pmContent)
				}
			}
		}
	}
}

func (hander *PrivateMessageHandler) CronTaskStop() {
	close(hander.Done)
}

func (handler *PrivateMessageHandler) HandlePrivatePackage(rawBytes []byte) (bool, string, []byte, error) {
	start := time.Now()

	if rawBytes == nil {
		return false, "", nil, errors.New("rawBytes is nil")
	}

	if len(rawBytes) == 0 {
		return false, "", nil, errors.New("len(rawBytes) is 0")
	}

	pp, err := DecodePrivatePackage(rawBytes)
	if err != nil {
		return false, "", nil, err
	}
	pmPostion := -1

	if postion, exits := handler.mapTidToPostion[int64(pp.Tid)]; exits {
		pmPostion = postion
	} else {

		pmPostion, _ = findPostion(pp, handler)
		handler.mapTidToPostion[int64(pp.Tid)] = pmPostion
	}

	if pmPostion == -1 {
		return false, "", nil, fmt.Errorf("can not found postion for %v", pp)
	}

	handler.lock.Lock()
	defer handler.lock.Unlock()
	pm := handler.privateMessages[pmPostion]

	pm.Tid = pp.Tid
	pm.LastTS = time.Now().UnixMilli()

	if pm.ContentPackageBatches == nil {
		pm.ContentPackageBatches = make(map[uint32]*PrivatePackage)
	}

	if pp.BatchId == 0 {
		pm.BizInfo = string(pp.Content)

	} else {
		pm.ContentPackageBatches[pp.BatchId] = pp
		pm.ExpectCount = pp.BatchCount
	}
	pm.RealCount++
	log.Infof("pp/pm/d: (%v:%v:%v)/(%v,%v,%v)/%v\n",
		pp.Tid, pp.BatchId, pp.BatchCount,
		pm.Tid, pm.RealCount, pp.BatchCount,
		time.Since(start).Milliseconds())

	if uint32(pm.Tid) == uint32(pp.Tid) && pm.RealCount == pp.BatchCount {
		wholePM, bizInfo, pmContent := resamplePPToPM(pm)
		return wholePM, bizInfo, pmContent, nil

	} else {
		return false, "", nil, nil
	}
}
func resamplePPToPM(pm *PrivateMessage) (bool, string, []byte) {
	keys := make([]int, 0, pm.RealCount)
	for k := range pm.ContentPackageBatches {
		keys = append(keys, int(k))
	}

	sort.Ints(keys)
	pmContent := make([]byte, 0)
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

	return wholePM, pm.BizInfo, pmContent

}
func ConstructPrivatePackage(tid uint64, batchId uint32, batchCount uint32, length uint32, dataTotalLength uint32, content []byte) *PrivatePackage {
	return &PrivatePackage{
		MagicNumber:     PrivatePackageMagicNumber,
		Tid:             tid,
		BatchId:         batchId,
		BatchCount:      batchCount,
		Length:          length,
		DataTotalLength: dataTotalLength,
		Timestamp:       time.Now().Unix(),
		Content:         content,
	}

}

func (handler *PrivateMessageHandler) PrivateMessageToPrivatePackage(pm *PrivateMessage) ([]*PrivatePackage, error) {
	if pm == nil {
		return nil, errors.New("pm is nil")
	}

	dataBytesLen := len(pm.Content)
	batchCount := int(math.Ceil(float64(dataBytesLen) / float64(PrivatePackageOnePatchSize)))
	tid := createTid()
	ppSlice := make([]*PrivatePackage, 0, batchCount)

	firstPP := ConstructPrivatePackage(uint64(tid), 0, uint32(batchCount+1), uint32(len(pm.BizInfo)), uint32(dataBytesLen), []byte(pm.BizInfo))
	ppSlice = append(ppSlice, firstPP)

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
			[]byte(pm.Content[start:end]),
		)
		ppSlice = append(ppSlice, dataPP)

	}
	return ppSlice, nil

}
