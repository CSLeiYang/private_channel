package main

import (
	"bufio"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	log "private_channel/logger"
	p_protocal "private_channel/protocal"
	"private_channel/server"
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
	serverAddress := "127.0.0.1:8096"

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

	handler := p_protocal.NewPrivateMessageHandler(5)
	go func() {
		for {
			buffer := make([]byte, 2048)
			n, _, err := conn.ReadFromUDP(buffer)
			if err != nil {
				log.Error(err)
				return
			}
			dencryptedBytes, _ := server.DescryptAES(buffer[:n])

			if dencryptedBytes[0] == p_protocal.PrivatePackageMagicNumber {
				havePM, bizInfo, content, err := handler.HandlePrivatePackage(dencryptedBytes)
				if err != nil {
					log.Error(err)
					return
				}
				if havePM {
					log.Infof("HavePM bizInfo:%v, content:%v", bizInfo, content)
				}

			} else {
				dencryptedBytesStr := string(dencryptedBytes)
				if !strings.Contains(dencryptedBytesStr, "Heartbeat") {
					log.Info("Recv: ", dencryptedBytesStr)
				}
			}

		}

	}()

	for {
		log.Info("Pls input (exit): ")
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)

		if strings.EqualFold(input, "exit") {
			log.Info("exit client")
			return
		}

		switch input {
		case "A":
			log.Info("FilePath: ")
			filePath, _ := reader.ReadString('\n')
			filePath = strings.TrimSpace(filePath)
			fileName := filepath.Base(filePath)

			pCmd := &p_protocal.PCommand{
				SID:    sid,
				Cmd:    "AUDIO",
				Params: fileName,
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

			ppSlice, err := handler.PrivateMessageToPrivatePackage(&p_protocal.PrivateMessage{BizInfo: string(bizInfo), Content: []byte(data)})
			if err != nil {
				log.Warn(err)
				continue
			}

			for _, p := range ppSlice {
				encodedPP, _ := p_protocal.EncodePrivatePackage(p)
				encryptedPPBytes, _ := server.EncryptAES(encodedPP)
				_, err := conn.Write(encryptedPPBytes)
				if err != nil {
					log.Warn(err)
				}
				if len(ppSlice) > 10 {
					time.Sleep(time.Microsecond * 100)
				}
			}
		case "C":
			log.Info("Chat Input: ")
			chatContent, _ := reader.ReadString('\n')
			chatContent = strings.TrimSpace(chatContent)

			pCmd := &p_protocal.PCommand{
				SID:    sid,
				Cmd:    "CHAT",
				Params: "",
			}

			bizInfo, err := json.Marshal(&pCmd)
			if err != nil {
				log.Warn(err)
				continue
			}

			ppSlice, err := handler.PrivateMessageToPrivatePackage(&p_protocal.PrivateMessage{BizInfo: string(bizInfo), Content: []byte(chatContent)})
			if err != nil {
				log.Warn(err)
				continue
			}

			for _, p := range ppSlice {
				encodedPP, _ := p_protocal.EncodePrivatePackage(p)
				log.Info("send pp: ", encodedPP)
				encryptedPPBytes, _ := server.EncryptAES(encodedPP)
				_, err := conn.Write(encryptedPPBytes)
				if err != nil {
					log.Warn(err)
				}
				time.Sleep(time.Microsecond * 100)
			}

		default:
			encryptedBytes, _ := server.EncryptAES([]byte(input))
			_, err := conn.Write(encryptedBytes)
			if err != nil {
				log.Error(err)
				continue
			}
		}

	}

}
