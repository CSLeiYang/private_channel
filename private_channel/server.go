package private_channel

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"io"
	"net"
	log "private_channel/logger"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	udpAddr = ":8096"
)

type udpClient struct {
	remoteAddr    *net.UDPAddr
	conn          *net.UDPConn
	sid           string
	lastHeartBeat time.Time
	pudpConn      *PUdpConn
}

var (
	udpClients     = make(map[string]*udpClient)
	udpClientsLock sync.Mutex
)

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

	//start hearbeats
	go sendUDPHeartbeats()

	for {
		buffer := make([]byte, 4096)
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

		go func(recvData []byte, oneClientAddr *net.UDPAddr) {
			log.Infof("recvData:%v, remoteAddr: %v", recvData, oneClientAddr)
			//
			pp, err := DecodePrivatePackage(recvData)
			if err != nil {
				log.Error(err)
				return
			}
			//update addr or new client
			oneUdpClient := updateUDPClient(pp.Sid, conn, oneClientAddr)

			if pp.Tid == HeartbeatTid {

				log.Infof("pp:%v is hearbeat,so ignore.", pp.Sid)
			} else {
				err = oneUdpClient.pudpConn.RecvPrivatePackage(pp)
				if err != nil {
					log.Warn(err)
					return
				}
			}

		}(dataCopy, clientAddr)

	}
}

func updateUDPClient(sid uint64, udpCon *net.UDPConn, clientAddr *net.UDPAddr) *udpClient {
	clientKey := strconv.FormatUint(sid, 10)
	udpClientsLock.Lock()
	defer udpClientsLock.Unlock()
	if client, exits := udpClients[clientKey]; exits {
		client.lastHeartBeat = time.Now()
		oldAddr := client.pudpConn.remoteAddr
		if !(oldAddr.IP.Equal(clientAddr.IP) && oldAddr.Port == clientAddr.Port) {
			log.Infof("client: %s addr changed, from %s to %s", clientKey, oldAddr, clientAddr)
			client.remoteAddr = clientAddr
			client.pudpConn.remoteAddr = clientAddr
		}
	} else {
		udpClients[clientKey] = &udpClient{sid: clientKey, conn: udpCon, remoteAddr: clientAddr, lastHeartBeat: time.Now(), pudpConn: NewPudpConn(sid, udpCon, clientAddr, HandlePCommand)}
		log.Infof("New UDP client: %s\n", clientKey)
	}
	return udpClients[clientKey]
}

func sendUDPHeartbeats() error {
	for {
		time.Sleep(UdpHearbeatInterval)
		udpClientsLock.Lock()
		for clientKey, oneUdpClient := range udpClients {
			if time.Since(oneUdpClient.lastHeartBeat) > MaxHearbeatMiss*UdpHearbeatInterval {
				log.Infof("UDP client %v timed out, removing from list. \n", clientKey)
				oneUdpClient.pudpConn.UdpConnStop()
				delete(udpClients, clientKey)
			}

			//send heartbeat
			heartbeatMsg := []byte("UDP Heartbeat")
			uint64_sid, _ := strconv.ParseUint(clientKey, 10, 64)
			heartbeatPP := constructPrivatePackage(
				uint64_sid,
				0x00,
				HeartbeatTid,
				0,
				1,
				heartbeatMsg,
			)

			err := SendPrivatePackage(oneUdpClient.pudpConn, heartbeatPP)
			if err != nil {
				udpClientsLock.Unlock()
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
