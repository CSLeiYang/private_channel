package private_channel

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	log "private_channel/logger"
	"strconv"
	"time"

	"github.com/faiface/beep"
	"github.com/faiface/beep/mp3"
	"github.com/faiface/beep/speaker"
)

type PCommand struct {
	SID    string `json:"sid"`
	Cmd    string `json:"cmd"`
	Params string `json:"params"`
}

type PEvent struct {
	SID          string `json:"sid"`
	EventType    string `json:"event_type"`
	EventContent string `json:"event_content"`
}

func HandlePCommand(userId uint64, wholePM bool, bizInfo string, content []byte, pUdpConn *PUdpConn) error {

	log.Infof("HandlePCommand comming, bizInfo: %v", bizInfo)
	var (
		pCmd   PCommand
		pm     *PrivateMessage
		pEvent PEvent
	)

	err := json.Unmarshal([]byte(bizInfo), &pCmd)
	if err != nil {
		log.Error(err.Error())
		return err
	}
	log.Info("PCommand: ", pCmd)
	switch pCmd.Cmd {
	case "CHAT":
		sid_uint64, _ := strconv.ParseUint(pCmd.SID, 10, 64)
		tid := uint64(createTid())
		streamPm := &PrivateMessage{
			Sid:         sid_uint64,
			Tid:         tid,
			IsStreaming: true,
			IsReliable:  true,
			BizInfo:     (fmt.Sprintf("%s %d", content, 10)),
		}
		pUdpConn.SendPrivateMessageStream(uint16(3), streamPm)
		pUdpConn.SendPrivateMessageStream(uint16(0), streamPm)
		pUdpConn.SendPrivateMessageStream(uint16(5), streamPm)
		pUdpConn.SendPrivateMessageStream(uint16(100), streamPm)

		return nil
	case "ASR":
		fileName := pCmd.Params
		err := os.WriteFile(fileName, content, 0666)
		if err != nil {
			return err
		}
		pEvent = PEvent{
			SID:          pCmd.SID,
			EventType:    pCmd.Cmd,
			EventContent: fmt.Sprintf("fileName: %s save success", fileName),
		}
	case "TTS":
		ttsContent, _ := os.ReadFile("TTS.mp3")
		pEvent = PEvent{
			SID:          pCmd.SID,
			EventType:    pCmd.Cmd,
			EventContent: "TTS.mp3",
		}
		pEventStr, err := json.Marshal(&pEvent)
		if err != nil {
			return err
		}

		pm = &PrivateMessage{
			BizInfo: string(pEventStr),
			Content: ttsContent,
		}
		pUdpConn.SendPrivateMessage(pm)
		return nil

	default:
		pEvent = PEvent{
			SID:          pCmd.SID,
			EventType:    pCmd.Cmd,
			EventContent: fmt.Sprintf("Invalid command: %s", pCmd),
		}
	}

	pEventStr, err := json.Marshal(&pEvent)
	if err != nil {
		return err
	}

	pm = &PrivateMessage{
		BizInfo: string(pEventStr),
	}
	pUdpConn.SendPrivateMessage(pm)
	return nil

}

func HandlePEvent(userId uint64, wholePM bool, bizInfo string, content []byte, pUdpCon *PUdpConn) error {
	log.Infof("HandlePEvent, bizInfo: %s, content:%v", bizInfo, len(content))
	if len(bizInfo) == 0 {
		log.Error("bizInfo is empty")
		return errors.New("bizInfo is empty")
	}

	var pEvent PEvent
	json.Unmarshal([]byte(bizInfo), &pEvent)
	switch pEvent.EventType {
	case "TTS":
		log.Info("TTS Text: ", pEvent.EventContent)
		// 将[]byte转换为io.Reader
		go func() {
			mp3Reader := bytes.NewReader(content)

			// 将io.Reader转换为ReadCloser
			mp3ReadCloser := io.NopCloser(mp3Reader)
			streamer, format, err := mp3.Decode(mp3ReadCloser)
			if err != nil {
				log.Error(err)
				return
			}
			defer streamer.Close()

			err = speaker.Init(format.SampleRate, format.SampleRate.N(time.Second/10))
			if err != nil {
				log.Error(err)
				return
			}

			done := make(chan bool)
			speaker.Play(beep.Seq(streamer, beep.Callback(func() {
				done <- true
			})))
			time.Sleep(time.Millisecond * 100)
			streamer.Close()
			<-done

		}()

	case "CHAT":
		log.Info("AI: ", string(pEvent.EventContent))
	}
	return nil

}
