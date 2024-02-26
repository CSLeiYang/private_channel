package main

import (
	"bufio"
	// "bytes"
	"encoding/json"
	// "io"
	"net"
	"os"
	log "private_channel/logger"
	p_protocal "private_channel/protocal"
	"private_channel/server"
	"strings"
	"time"

	// "github.com/faiface/beep"
	// "github.com/faiface/beep/mp3"
	// "github.com/faiface/beep/speaker"
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

	handler := p_protocal.NewPrivateMessageHandler(5)
	go func() {
		for {
			buffer := make([]byte, 4096)
			n, _, err := conn.ReadFromUDP(buffer)
			if err != nil {
				log.Error(err)
				continue
			}
			dencryptedBytes, _ := server.DescryptAES(buffer[:n])

			if dencryptedBytes[0] == p_protocal.PrivatePackageMagicNumber {
				start := time.Now()
				havePM, bizInfo, content, err := handler.HandlePrivatePackage(dencryptedBytes)
				elapsed :=time.Since(start)
				log.Infof("HandlePrivatePackage: %v",elapsed.Milliseconds())
				if err != nil {
					log.Error(err)
					continue
				}
				if havePM {
					log.Infof("HavePM bizInfo:%v, len(content):%v", bizInfo, len(content))
					var pEvent p_protocal.PEvent
					json.Unmarshal([]byte(bizInfo), &pEvent)
					switch pEvent.EventType {
					case "TTS":
						log.Info("TTS Text: ", pEvent.EventContent)
						// 将[]byte转换为io.Reader
						// go func() {
						// 	mp3Reader := bytes.NewReader(content)

						// 	// 将io.Reader转换为ReadCloser
						// 	mp3ReadCloser := io.NopCloser(mp3Reader)
						// 	streamer, format, err := mp3.Decode(mp3ReadCloser)
						// 	if err != nil {
						// 		log.Error(err)
						// 		return
						// 	}
						// 	defer streamer.Close()

						// 	err = speaker.Init(format.SampleRate, format.SampleRate.N(time.Second/10))
						// 	if err != nil {
						// 		log.Error(err)
						// 		return
						// 	}

						// 	done := make(chan bool)
						// 	speaker.Play(beep.Seq(streamer, beep.Callback(func() {
						// 		done <- true
						// 	})))
						// 	time.Sleep(time.Millisecond * 100)
						// 	streamer.Close()
						// 	<-done

						// }()

					case "CHAT":
						log.Info("AI: ", string(content))
					}
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
			return
		}

		switch input {
		case "ASR":
			log.Info("FilePath: ")
			filePath, _ := reader.ReadString('\n')
			filePath = strings.TrimSpace(filePath)
			// fileName := filepath.Base(filePath)

			pCmd := &p_protocal.PCommand{
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
		case "TTS":
			log.Info("Input TTS: ")
			chatContent, _ := reader.ReadString('\n')
			chatContent = strings.TrimSpace(chatContent)

			pCmd := &p_protocal.PCommand{
				SID:    sid,
				Cmd:    "TTS",
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
				if len(ppSlice) > 10 {
					time.Sleep(time.Microsecond * 100)
				}

			}

		default:
			chatContent := input

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
		}

	}

}
