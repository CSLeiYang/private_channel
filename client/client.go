package main

import (
	"bufio"
	"encoding/json"
	"net"
	"os"
	log "private_channel/logger"
	private_channel "private_channel/private_channel"
	"strings"
	"time"

	"github.com/google/uuid"
)

func main() {
	sid := uuid.New().String()
	if len(os.Args) > 1 {
		sid = os.Args[1]
	}
	reader := bufio.NewReader(os.Stdin)
	log.Info("UDP Started")
	serverAddress := "165.227.25.123:8096"

	serverAddr, err := net.ResolveUDPAddr("udp", serverAddress)
	if err != nil {
		log.Error("Can not resolve the server addr: ", serverAddress)
		return
	}

	conn, err := net.DialUDP("udp", nil, serverAddr)
	if err != nil {
		log.Error("Can not connect the server: ", err)
		return
	}
	defer conn.Close()

	handler := private_channel.NewPrivateMessageHandler(5)
	go func() {
		for {
			buffer := make([]byte, 4096)
			n, _, err := conn.ReadFromUDP(buffer)
			if err != nil {
				log.Error(err)
				continue
			}
			dencryptedBytes, _ := private_channel.DescryptAES(buffer[:n])

			if dencryptedBytes[0] == private_channel.PrivatePackageMagicNumber {
				havePM, bizInfo, content, err := handler.HandlePrivatePackage(dencryptedBytes)
				if err != nil {
					log.Error(err)
					continue
				}
				private_channel.HandlePEvent(havePM, bizInfo, content)

			} else {
				dencryptedBytesStr := string(dencryptedBytes)
				if !strings.Contains(dencryptedBytesStr, "Heartbeat") {
					log.Info("Recv: ", dencryptedBytesStr)
				}
			}

		}

	}()

	for {
		log.Info("Pls Input (CHAT(default)|ASR|TTS): ")
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)

		if strings.EqualFold(input, "exit") {
			log.Info("exit client")
			handler.CronTaskStop()
			return
		}

		switch input {
		case "ASR":
			log.Info("FilePath: ")
			filePath, _ := reader.ReadString('\n')
			filePath = strings.TrimSpace(filePath)
			// fileName := filepath.Base(filePath)

			pCmd := &private_channel.PCommand{
				SID:    sid,
				Cmd:    "ASR",
				Params: "",
			}

			bizInfo, err := json.Marshal(&pCmd)
			if err != nil {
				log.Warn(err)
				continue
			}

			data, err := os.ReadFile(filePath)
			if err != nil {
				log.Warn(err)
				continue
			}

			ppSlice, err := handler.PrivateMessageToPrivatePackage(&private_channel.PrivateMessage{BizInfo: string(bizInfo), Content: []byte(data)})
			if err != nil {
				log.Warn(err)
				continue
			}

			for _, p := range ppSlice {
				encodedPP, _ := private_channel.EncodePrivatePackage(p)
				encryptedPPBytes, _ := private_channel.EncryptAES(encodedPP)
				_, err := conn.Write(encryptedPPBytes)
				if err != nil {
					log.Warn(err)
				}
				if len(ppSlice) > 10 {
					time.Sleep(time.Microsecond * 100)
				}
			}
		case "TTS":
			log.Info("Input TTS: ")
			chatContent, _ := reader.ReadString('\n')
			chatContent = strings.TrimSpace(chatContent)

			pCmd := &private_channel.PCommand{
				SID:    sid,
				Cmd:    "TTS",
				Params: "",
			}

			bizInfo, err := json.Marshal(&pCmd)
			if err != nil {
				log.Warn(err)
				continue
			}

			ppSlice, err := handler.PrivateMessageToPrivatePackage(&private_channel.PrivateMessage{BizInfo: string(bizInfo), Content: []byte(chatContent)})
			if err != nil {
				log.Warn(err)
				continue
			}

			for _, p := range ppSlice {
				encodedPP, _ := private_channel.EncodePrivatePackage(p)
				log.Info("send pp: ", encodedPP)
				encryptedPPBytes, _ := private_channel.EncryptAES(encodedPP)
				_, err := conn.Write(encryptedPPBytes)
				if err != nil {
					log.Warn(err)
				}
				if len(ppSlice) > 10 {
					time.Sleep(time.Microsecond * 100)
				}

			}

		default:
			chatContent := input

			pCmd := &private_channel.PCommand{
				SID:    sid,
				Cmd:    "CHAT",
				Params: "",
			}

			bizInfo, err := json.Marshal(&pCmd)
			if err != nil {
				log.Warn(err)
				continue
			}

			ppSlice, err := handler.PrivateMessageToPrivatePackage(&private_channel.PrivateMessage{BizInfo: string(bizInfo), Content: []byte(chatContent)})
			if err != nil {
				log.Warn(err)
				continue
			}

			for _, p := range ppSlice {
				encodedPP, _ := private_channel.EncodePrivatePackage(p)
				encryptedPPBytes, _ := private_channel.EncryptAES(encodedPP)
				_, err := conn.Write(encryptedPPBytes)
				if err != nil {
					log.Warn(err)
				}
				time.Sleep(time.Microsecond * 100)
			}
		}

	}

}
