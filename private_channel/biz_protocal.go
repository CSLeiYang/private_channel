package private_channel

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	log "private_channel/logger"
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

func HandlePCommand(wholePM, bizInfo string, content []byte, pUdpConn *PUdpConn) error {
	var (
		pCmd   PCommand
		pm     *PrivateMessage
		pEvent PEvent
	)

	err := json.Unmarshal([]byte(bizInfo), &pCmd)
	if err != nil {
		return err
	}
	log.Info("PCommand: ", pCmd)
	switch pCmd.Cmd {
	case "CHAT":
		pEvent = PEvent{
			SID:          pCmd.SID,
			EventType:    pCmd.Cmd,
			EventContent: string(content),
		}
	case "IMAGE", "AUDIO":
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

func HandlePEvent(wholePM bool, bizInfo string, content []byte, pUdpCon *PUdpConn) error {
	if len(bizInfo) == 0 {
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
		log.Info("AI: ", string(content))
	}
	return nil

}
