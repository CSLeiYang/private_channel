package protocal

import (
	"encoding/json"
	"fmt"
	"os"
	log "private_channel/logger"
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

func HandlePCommand(bizInfo string, content []byte) (*PrivateMessage, error) {
	var (
		pCmd   PCommand
		pm     *PrivateMessage
		pEvent PEvent
	)

	err := json.Unmarshal([]byte(bizInfo), &pCmd)
	if err != nil {
		return pm, err
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
			return pm, err
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
		return pm, err
	}

	pm = &PrivateMessage{
		BizInfo: string(pEventStr),
	}
	return pm, nil

}
