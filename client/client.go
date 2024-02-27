package main

import (
	"bufio"
	"encoding/json"
	"net"
	"os"
	log "private_channel/logger"
	private_channel "private_channel/private_channel"
	"strings"

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

	onePupConn := private_channel.NewPudpConn(conn, nil, private_channel.HandlePEvent)
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
				err := onePupConn.RecvPrivatePackage(dencryptedBytes)
				if err != nil {
					log.Error(err)
					continue
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
		log.Info("Pls Input (CHAT(default)|ASR|TTS): ")
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)

		if strings.EqualFold(input, "exit") {
			log.Info("exit client")
			onePupConn.UdpConnStop()
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

			err = onePupConn.SendPrivateMessage(&private_channel.PrivateMessage{BizInfo: string(bizInfo), Content: []byte(data)})
			if err != nil {
				log.Warn(err)
				continue
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

			err = onePupConn.SendPrivateMessage(&private_channel.PrivateMessage{BizInfo: string(bizInfo), Content: []byte(chatContent)})
			if err != nil {
				log.Warn(err)
				continue
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

			err = onePupConn.SendPrivateMessage(&private_channel.PrivateMessage{BizInfo: string(bizInfo), Content: []byte(chatContent)})
			if err != nil {
				log.Warn(err)
				continue
			}
		}

	}

}
