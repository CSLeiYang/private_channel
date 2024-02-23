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
	tcpAddr             = ":8080"
	udpAddr             = ":8096"
	heartbeatInteval    = 5 * time.Second
	connectionTimeout   = 30 * time.Second
	maxHearbeatMiss     = 3
	udpHearbeatInterval = 10 * time.Second
)

type client struct {
	conn          net.Conn
	lastHeartBeat time.Time
	heartBeatMiss int
	mu            sync.Mutex
}

func newClient(conn net.Conn) *client {
	return &client{
		conn:          conn,
		lastHeartBeat: time.Now(),
		heartBeatMiss: 0,
	}
}

func (c *client) resetHeartBeat() {
	c.mu.Lock()
	c.lastHeartBeat = time.Now()
	c.heartBeatMiss = 0
	c.mu.Unlock()
}

func (c *client) missedHeartbeat() {
	c.mu.Lock()
	c.heartBeatMiss++
	c.mu.Unlock()
}

func (c *client) disconnectIfTimeout() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if time.Since(c.lastHeartBeat) > connectionTimeout || c.heartBeatMiss >= maxHearbeatMiss {
		log.Infof("Client %s timed out or missed too many heartbeats. Disconnectiong...\n", c.conn.RemoteAddr())
		c.conn.Close()
	}
}

func StartTcpServer() {
	tcpserver, err := net.Listen("tcp", tcpAddr)
	if err != nil {
		log.Errorf("Error starting TCP server: %v", err)
	}
	defer tcpserver.Close()
	log.Infof("TCP server listening on %s", tcpAddr)
	for {
		conn, err := tcpserver.Accept()
		if err != nil {
			log.Infof("Error accepting TCP connection: %s", err)
			continue
		}
		go handleTcpConnection(newClient(conn))
	}
}

func handleTcpConnection(c *client) {
	defer c.conn.Close()

	buffer := make([]byte, aes.BlockSize+256)
	for {
		err := c.conn.SetReadDeadline(time.Now().Add(connectionTimeout))
		if err != nil {
			log.Errorf("Error setting read deadline: %v", err)
			return
		}

		n, err := c.conn.Read(buffer)
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				log.Infof("Connection closed by client: %s\n", c.conn.RemoteAddr())

			} else if err != io.EOF {
				log.Infof("Error reading from client %s: %v\n", c.conn.RemoteAddr(), err)
			}
			break
		}

		data, err := DescryptAES(buffer[:n])
		if err != nil {
			log.Infof("Failed to decrypt message from %s: %v\n", c.conn.RemoteAddr(), err)
			continue
		}

		c.resetHeartBeat()
		log.Infof("Received from %s: %s\n", c.conn.RemoteAddr(), string(data))

		go sendHeartbeat(c)
	}
}

func sendHeartbeat(c *client) {
	for {
		time.Sleep(heartbeatInteval)
		heartbeatMsg := "Heartbeat"

		encryptedMsg, err := EncryptAES([]byte(heartbeatMsg))
		if err != nil {
			log.Infof("Failed to encrypt heartbeat message: %v\n", err)
			return
		}

		_, err = c.conn.Write(encryptedMsg)
		if err != nil {
			log.Infof("Failed to send heartbeat to %s: %v\n", c.conn.RemoteAddr(), err)
			c.missedHeartbeat()
			c.disconnectIfTimeout()
			return
		}
		c.resetHeartBeat()

	}

}

type udpClient struct {
	conn           *net.UDPConn
	addr           *net.UDPAddr
	lastHeartBeat  time.Time
	handleIncoming *p_protocal.PrivateMessageHandler
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

	buffer := make([]byte, 1024)
	for {
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

		oneUdpClient := updateUDPClient(conn, clientAddr)

		go func(recvData []byte, oneUdpClient *udpClient) {
			if n > 0 {
				checkPackage := data[0]
				if checkPackage == p_protocal.PrivatePackageMagicNumber {
					bizInfo, content, err := oneUdpClient.handleIncoming.HandleRecvBytes(recvData)
					if err != nil {
						log.Warn(err)
					} else {
						log.Infof("bizInfo: %v\n, content: %v\n", bizInfo, content)
						p_protocal.SendPrivateMessage(oneUdpClient.conn, &p_protocal.PrivateMessage{BizInfo: "OKKK", Content: []byte("11111")}, oneUdpClient.addr)
					}

				} else {
					recvMsg := string(data)
					if strings.Contains(recvMsg, ">") {
						parts := strings.Split(recvMsg, ">")
						reqId := parts[0]
						msg := parts[1]
						// returnMsg, err := service.CreateChatDialogReturn(reqId, msg)
						if err != nil {
							log.Warnf("CreateChatDialogReturn err: %s", err)
							return
						}
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

			}

		}(data, oneUdpClient)

	}
}

func updateUDPClient(udpCon *net.UDPConn, clientAddr *net.UDPAddr) *udpClient {
	clientKey := clientAddr.String()
	udpClientsLock.Lock()
	defer udpClientsLock.Unlock()
	if client, exits := udpClients[clientKey]; exits {
		client.lastHeartBeat = time.Now()
	} else {
		udpClients[clientKey] = &udpClient{conn: udpCon, addr: clientAddr, lastHeartBeat: time.Now(), handleIncoming: NewPrivateMessageHandler(5)}
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
	log.Infof("Encrypt AesKey:%s", aesKey)
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
	log.Infof("Descrypt AesKey:%s", aesKey)
	block, err := aes.NewCipher([]byte(aesKey))
	if err != nil {
		return nil, err
	}

	if len(ciphertext) < aes.BlockSize {
		return nil, errors.New("ciphertext too short")
	}

	iv := ciphertext[:aes.BlockSize]
	ciphertext = ciphertext[aes.BlockSize:]

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
