package protocal

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
	PrivatePackageSaveMaxTime  int   = 5
	PrivatePackageMaxBytes     int   = 1024
	PrivatePackageOnePatchSize int   = 512
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
	IsDeal                bool
	RealCount             uint32
	ContentPackageBatches map[uint32]PrivatePackage
	Content               []byte
}

type PrivateMessageHandler struct {
	privateMessages []PrivateMessage
	sync.Mutex
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

func findPostion(pp *PrivatePackage, pms []PrivateMessage) (int, bool) {
	currentUnixTime := time.Now().Unix()
	for i, pm := range pms {
		emptyOrExpired := pm.Tid == 0 || pm.IsDeal || (pm.LastTS > 0 && currentUnixTime-pm.LastTS > int64(PrivatePackageSaveMaxTime))
		if (pm.Tid == pp.Tid && !pm.IsDeal) || emptyOrExpired {
			return i, emptyOrExpired
		}
	}

	return -1, false
}

func NewPrivateMessageHandler(count int) *PrivateMessageHandler {
	return &PrivateMessageHandler{
		privateMessages: make([]PrivateMessage, count),
	}
}

func (handler *PrivateMessageHandler) HandlePrivatePackage(rawBytes []byte) (bool, string, []byte, error) {

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
	log.Infof("Decode private package: %v/%v/%v\n", pp.Tid, pp.BatchId, pp.BatchCount)
	position, _ := findPostion(pp, handler.privateMessages)
	if position == -1 {
		return false, "", nil, fmt.Errorf("can not found postion for %v", pp)
	}
	log.Infof("Find Postion: %v\n", position)

	handler.Lock()
	defer handler.Unlock()

	pm := &handler.privateMessages[position]

	pm.Tid = pp.Tid
	pm.IsDeal = false
	pm.LastTS = time.Now().Unix()

	if pm.ContentPackageBatches == nil {
		pm.ContentPackageBatches = make(map[uint32]PrivatePackage)
	}

	if pp.BatchId == 0 {
		pm.BizInfo = string(pp.Content)
		pm.RealCount = 0
	} else {
		pm.ContentPackageBatches[pp.BatchId] = *pp
	}
	pm.RealCount++

	log.Infof("pm.RealCount/pp.BatchCount/pm.IsDeal: %v/%v/%v\n", pm.RealCount, pp.BatchCount, pm.IsDeal)

	if pm.RealCount == pp.BatchCount && !pm.IsDeal {
		pm.IsDeal = true
		keys := make([]int, 0, pm.RealCount)
		for k := range pm.ContentPackageBatches {
			keys = append(keys, int(k))
		}

		sort.Ints(keys)
		content := make([]byte, 0)
		for _, k := range keys {
			content = append(content, pm.ContentPackageBatches[uint32(k)].Content...)
		}
		pm.ContentPackageBatches = nil
		pm.Content = content
		pm.IsDeal = true
		pm.RealCount = 0

		return true, pm.BizInfo, pm.Content, nil

	} else {
		return false, "", nil, nil
	}
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
