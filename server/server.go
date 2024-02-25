package server

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"io"
	"net"
	log "private_channel/logger"
	p_protocal "private_channel/protocal"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

const (
	udpAddr             = ":8096"
	maxHearbeatMiss     = 3
	udpHearbeatInterval = 30 * time.Second
)

type udpClient struct {
	conn          *net.UDPConn
	addr          *net.UDPAddr
	lastHeartBeat time.Time
	handler       *p_protocal.PrivateMessageHandler
}

var (
	udpClients     = make(map[string]*udpClient)
	udpClientsLock sync.Mutex
)

func splitToBatches(s string, maxBytes int) [][]byte {
	var batches [][]byte
	var batch []byte
	for len(s) > 0 {
		_, size := utf8.DecodeRuneInString(s)
		if len(batch)+size > maxBytes {
			batches = append(batches, batch)
			batch = []byte{}
		}
		batch = append(batch, s[:size]...)
		s = s[size:]
	}
	if len(batch) > 0 {
		batches = append(batches, batch)
	}
	return batches
}

func StartUDPServer() {
	addr, err := net.ResolveUDPAddr("udp", udpAddr)
	if err != nil {
		log.Errorf("Error resolving UDP address: %v", err)
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Errorf("Error starting UDP server: %v", err)
	}
	defer conn.Close()
	log.Infof("UDP server listening on %s", udpAddr)

	go sendUDPHeartbeats(conn)

	for {
		buffer := make([]byte,1024)
		n, clientAddr, err := conn.ReadFromUDP(buffer)
		if err != nil {
			log.Warnf("Error reading UDP data: %s", err)
			continue
		}
		log.Infof("ReadFromUdp %s:%v", clientAddr, n)

		data, err := DescryptAES(buffer[:n])
		if err != nil {
			log.Infof("Failed to decrypt UDP message: %s", err)
			continue
		}
		if len(data) == 0 {
			log.Warn("Data length is 0")
			continue
		}
		dataCopy := make([]byte, len(data))
		copy(dataCopy, data)
		oneUdpClient := updateUDPClient(conn, clientAddr)

		go func(recvData []byte, oneUdpClient *udpClient) {
			if recvData[0] == p_protocal.PrivatePackageMagicNumber {
				log.Info("recvData: ", recvData)
				havePM, bizInfo, content, err := oneUdpClient.handler.HandlePrivatePackage(recvData)
				if err != nil {
					log.Warn(err)
					return
				} else {
					if havePM {
						pm, err := p_protocal.HandlePCommand(bizInfo, content)
						if err != nil {
							log.Warn(err)
							return
						}
						ppSlice, err := oneUdpClient.handler.PrivateMessageToPrivatePackage(pm)
						if err != nil {
							log.Warn(err)
							return
						}
						log.Infof("ppSlice: %v", ppSlice)

						for _, pp := range ppSlice {
							encodedPP, err := p_protocal.EncodePrivatePackage(pp)
							if err != nil {
								log.Warn(err)
								return
							}
							log.Info("encodedPP: ", encodedPP)
							encryptedPPBytes, _ := EncryptAES(encodedPP)
							_, err = oneUdpClient.conn.WriteToUDP(encryptedPPBytes, oneUdpClient.addr)
							if err != nil {
								log.Warn(err)
								return
							}
							time.Sleep(time.Microsecond * 100)
						}
						log.Info("Send end")

					} else {
						log.Info("need more pacakge ")
					}
				}

			} else {
				recvMsg := string(data)
				log.Infof("recvMsg: %v", recvMsg)
				if strings.Contains(recvMsg, ">") {
					parts := strings.Split(recvMsg, ">")
					reqId := parts[0]
					msg := parts[1]
					returnMsg := ""
					log.Info(reqId, msg)
					// returnMsg, err := service.CreateChatDialogReturn(reqId, msg)
					// if err != nil {
					// 	log.Warnf("CreateChatDialogReturn err: %s", err)
					// 	return
					// }
					log.Infof("Send %s to %s", returnMsg, clientAddr)

					batches := splitToBatches(returnMsg, 1024)
					for _, batch := range batches {
						encryptReturnMsg, _ := EncryptAES([]byte(batch))
						n, err = conn.WriteToUDP([]byte(encryptReturnMsg), clientAddr)
						if err != nil {
							log.Warnf("WriteToUdp error: %s", err)
						}
					}
				}
			}

		}(dataCopy, oneUdpClient)

	}
}

func updateUDPClient(udpCon *net.UDPConn, clientAddr *net.UDPAddr) *udpClient {
	clientKey := clientAddr.String()
	udpClientsLock.Lock()
	defer udpClientsLock.Unlock()
	if client, exits := udpClients[clientKey]; exits {
		client.lastHeartBeat = time.Now()
	} else {
		udpClients[clientKey] = &udpClient{conn: udpCon, addr: clientAddr, lastHeartBeat: time.Now(), handler: p_protocal.NewPrivateMessageHandler(5)}
		log.Infof("New UDP client: %s\n", clientKey)
	}
	return udpClients[clientKey]
}

func sendUDPHeartbeats(conn *net.UDPConn) {
	for {
		time.Sleep(udpHearbeatInterval)
		udpClientsLock.Lock()
		for key, client := range udpClients {
			if time.Since(client.lastHeartBeat) > 3*udpHearbeatInterval {
				log.Infof("UDP client %s timed out, removing from list. \n", key)
				delete(udpClients, key)
				continue
			}

			heartbeatMsg := "UDP Heartbeat"
			encryptHeartbeatMsg, _ := EncryptAES([]byte(heartbeatMsg))
			_, err := conn.WriteToUDP([]byte(encryptHeartbeatMsg), client.addr)
			if err != nil {
				log.Infof("Failed to send UDP heartbeat to %s: %v\n", client.addr, err)
				continue
			}
		}
		udpClientsLock.Unlock()
	}
}

func EncryptAES(plaintext []byte) ([]byte, error) {
	aesKey := strings.Repeat(time.Now().UTC().Format("20060102"), 4)
	block, err := aes.NewCipher([]byte(aesKey))
	if err != nil {
		return nil, err
	}

	padding := block.BlockSize() - len(plaintext)%block.BlockSize()
	padtext := bytes.Repeat([]byte{byte(padding)}, padding)
	plaintext = append(plaintext, padtext...)
	ciphertext := make([]byte, aes.BlockSize+len(plaintext))
	iv := ciphertext[:aes.BlockSize]
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return nil, err
	}

	mode := cipher.NewCBCEncrypter(block, iv)
	mode.CryptBlocks(ciphertext[aes.BlockSize:], plaintext)

	return ciphertext, nil
}

func DescryptAES(ciphertext []byte) ([]byte, error) {
	aesKey := strings.Repeat(time.Now().UTC().Format("20060102"), 4)
	block, err := aes.NewCipher([]byte(aesKey))
	if err != nil {
		return nil, err
	}

	if len(ciphertext) < aes.BlockSize {
		return nil, errors.New("ciphertext too short")
	}

	iv := ciphertext[:aes.BlockSize]
	ciphertext = ciphertext[aes.BlockSize:]
	if len(ciphertext)%block.BlockSize() != 0 {
		return nil, errors.New("ciphertext is not a multiple of the block size")

	}
	mode := cipher.NewCBCDecrypter(block, iv)
	mode.CryptBlocks(ciphertext, ciphertext)
	padding := int(ciphertext[len(ciphertext)-1])
	if padding > block.BlockSize() || padding == 0 {
		return nil, err
	}

	for i := len(ciphertext) - padding; i < len(ciphertext); i++ {
		if ciphertext[i] != byte(padding) {
			return nil, err
		}
	}

	return ciphertext[:len(ciphertext)-padding], nil
}