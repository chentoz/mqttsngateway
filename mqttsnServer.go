package main

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"runtime"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

func random(min, max int) int {
	return rand.Intn(max-min) + min
}

type m2a struct {
	mac  string
	addr *net.UDPAddr
}

type processDAT struct {
	payload []byte
	addr    *net.UDPAddr
}

type processQueue struct {
	id int
}

var (
	rwm         sync.RWMutex
	rwm2        sync.RWMutex
	queue       []int
	macStr2Addr map[string]*net.UDPAddr = make(map[string]*net.UDPAddr)
	appName     string                  = "MQTTSN-Gateway-Golang"
	appVersion  string
	inport      *string
	outport     *string
	workers     *int
)

func get(key string) *net.UDPAddr {
	rwm.RLock()
	defer rwm.RUnlock()
	return macStr2Addr[key]
}

func set(key string, value *net.UDPAddr) {
	rwm.Lock()
	defer rwm.Unlock()
	macStr2Addr[key] = value
}

func addQueue(id int) {
	rwm2.Lock()
	defer rwm2.Unlock()
	log.Printf("add  %v \n", queue)
	queue = append(queue, id)
}

func frontQueue() int {
	rwm2.RLock()
	defer rwm2.RUnlock()
	return queue[0]
}

func updateMac2AddrMap(update chan *m2a) {
	for {
		ud := <-update
		set(ud.mac, ud.addr)
	}
}

func handleMqttSnMessage(message []byte, rinfo *net.UDPAddr, client *mqtt.Client, update chan *m2a) error {

	var topic string
	n := len(message)

	if n < 8 {
		return errors.New("Insufficient data length for a mqttsn messager")
	}

	if message[3] == 0x42 {
		topic = "BLELocation"
	} else if message[3] == 0x47 {
		topic = "GPSLocation"
	} else if message[3] == 0x48 {
		topic = "HeartBeat"
	} else {
		return errors.New("Unrecognized topic")
	}

	if strings.TrimSpace(string(message)) == "STOP" {
		log.Printf("Exiting %v via STOP command\n", appName)
		return errors.New("Exiting via STOp command")
	}
	mqttsnMessage := message[7:n]

	macstring := strings.ToUpper(hex.EncodeToString(mqttsnMessage[2 : 2+6]))
	update <- &m2a{macstring, rinfo}

	log.Printf("%v redirecting : %v\n", appName, macstring)
	token := (*client).Publish(topic, 0, false, mqttsnMessage)
	if token.Error() != nil {
		log.Printf("%v : error sending message from %v : %v \n", appName, macstring, token.Error())
		return token.Error()
	}
	return nil
}

func hdlcDecode(message []byte, rinfo *net.UDPAddr, client *mqtt.Client, update chan *m2a) {
	state := 0
	targetIdx := 0
	msgLen := len(message)
	frame := make([]byte, msgLen)
	totalPacket := 0

	for i := 0; i < msgLen; i++ {
		if message[i] == 0x7e {
			if state == 0 {
				state = 1
				continue
			} else if state == 1 {
				totalPacket++
				err := handleMqttSnMessage(frame[0:targetIdx], rinfo, client, update)
				if err == nil {
					log.Printf("Handling frame : %v ", frame)
				}
				frame = make([]byte, msgLen)
				state = 0
				targetIdx = 0
			}
		} else if message[i] == 0x7d && i+1 < msgLen {
			if message[i+1] == 0x5e {
				frame[targetIdx], targetIdx = 0x7e, targetIdx+1
				i = i + 1
			} else if message[i+1] == 0x5d {
				frame[targetIdx], targetIdx = 0x7d, targetIdx+1
				i = i + 1
			} else {
				frame[targetIdx], targetIdx = message[i], targetIdx+1
			}
		} else {
			frame[targetIdx], targetIdx = message[i], targetIdx+1
		}
		log.Printf("************************************\n Total Packets : %v\n ************************************ \n", totalPacket)
	}
}

func hdlcEncode(message []byte) []byte {
	msgLength := len(message)
	encodedFrame := make([]byte, msgLength*2)
	encodedFrame[0] = 0x7e
	targetIdx := 1
	for srcIdx := 0; srcIdx < msgLength; srcIdx++ {
		if message[srcIdx] == 0x7e {
			encodedFrame[targetIdx] = 0x7d
			targetIdx++
			encodedFrame[targetIdx] = 0x5e
		} else if message[srcIdx] == 0x7d {
			encodedFrame[targetIdx] = 0x7d
			targetIdx++
			encodedFrame[targetIdx] = 0x5d
		} else {
			encodedFrame[targetIdx] = message[srcIdx]
		}
		targetIdx++
	}
	encodedFrame[targetIdx] = 0x7e
	targetIdx++
	return encodedFrame[0:targetIdx]
}

func dispatchUDPPackets(connection *net.UDPConn, dataChannels []chan *processDAT) {

	buffer := make([]byte, 4096)

	for {
		n, remoteAddr, err := connection.ReadFromUDP(buffer)
		if err == nil {
			dataChannels[frontQueue()] <- &processDAT{buffer[0:n], remoteAddr}
		} else {
			log.Printf("%v reading udp packets error : %v", appName, err.Error())
		}
	}
}

func processUDPPackets(connection *net.UDPConn, in chan *processDAT, workerID int, update chan *m2a) {

	var hdrHeartBeatAck mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {

		msgTypeByte := byte(0x0c)
		flagByte := byte(0x62)
		tidBytes := make([]byte, 2)
		tidBytes[0] = byte('H')
		tidBytes[1] = byte('B')
		midBytes := make([]byte, 2)
		binary.BigEndian.PutUint16(midBytes, 0x0000)
		lenByte := byte(1 + 1 + 1 + 2 + 2 + binary.Size(msg.Payload()))

		packet := make([]byte, lenByte)
		(packet)[0] = lenByte
		(packet)[1] = msgTypeByte
		(packet)[2] = flagByte
		copy((packet)[3:5], tidBytes)
		copy((packet)[5:7], midBytes)
		copy((packet)[7:], msg.Payload())

		macstring := strings.ToUpper(hex.EncodeToString(msg.Payload()[2 : 2+6]))
		udpAddr := get(macstring)

		log.Printf("%v receive HeartBeat Ack from : %v \n", appName, macstring)
		frame := hdlcEncode(packet)

		if udpAddr == nil {
			log.Printf("mac string not found : %v \n", macstring)
		} else {
			_, err := connection.WriteToUDP(frame, udpAddr)
			if err != nil {
				log.Printf("Error when re-sending : %v \n", err.Error())
			}
		}
	}

	opts := mqtt.NewClientOptions().
		AddBroker("tcp://localhost:" + *outport).
		SetClientID(fmt.Sprintf("%v-%v-%v", appName, time.Now().Format(time.RFC3339Nano), "")).
		SetCleanSession(true).
		SetAutoReconnect(true).
		SetOnConnectHandler(func(client mqtt.Client) {
			if token := client.Subscribe("HeartBeatAck", 0, hdrHeartBeatAck); token.Wait() && token.Error() != nil {
				log.Println(token.Error())
			} else {
				log.Println("Subscribe topic HeartBeatAck success")
			}
		}).
		SetConnectionLostHandler(func(client mqtt.Client, reason error) {
			log.Printf("CLIENT %v lost connection to the broker: %v. Will reconnect...\n", appName, reason.Error())
		})

	client := mqtt.NewClient(opts)

	token := client.Connect()
	token.Wait()
	if token.Error() != nil {
		log.Printf("CLIENT %v had error connecting to the broker: %v\n", appName, token.Error())
		return
	}

	head := make([]byte, 1)
	head[0] = 0x7e

	select {
	case inADT := <-in:
		if bytes.Contains(inADT.payload, head) {
			hdlcDecode(inADT.payload, inADT.addr, &client, update)
		} else { // compatible with old devices without HDLC
			//handleMqttSnMessage(inADT.payload, inADT.addr, &client, update)
			log.Printf("HDLC header not detected \n")
		}
		addQueue(workerID)
	}
}

func main() {
	log.Printf("\n***************************\n %v version : %v \n***************************\n ", appName, appVersion)

	maxCores := runtime.GOMAXPROCS(runtime.NumCPU())

	inport = flag.String("inport", "31337", "MQTTSN packet source port")
	outport = flag.String("outport", "43518", "MQTT packet destination port")
	workers = flag.Int("workers", maxCores, "Total Works")

	flag.Parse()

	s, err := net.ResolveUDPAddr("udp4", ":"+*inport)
	if err != nil {
		log.Println(err)
		return
	}

	connection, err := net.ListenUDP("udp4", s)
	if err != nil {
		log.Println(err)
		return
	}

	defer connection.Close()

	update := make(chan *m2a)
	go updateMac2AddrMap(update)

	actualWorkers := 1
	if *workers > runtime.NumCPU() {
		actualWorkers = runtime.NumCPU()
	} else {
		actualWorkers = *workers
	}

	channels := make([]chan *processDAT, actualWorkers)

	queue = make([]int, actualWorkers)
	for i := range queue {
		queue[i] = i
	}

	for i := 0; i < actualWorkers; i++ {
		channels[i] = make(chan *processDAT)
	}

	go dispatchUDPPackets(connection, channels)

	for i := 0; i < actualWorkers; i++ {
		go processUDPPackets(connection, channels[i], i, update)
	}

	quit := make(chan struct{})
	<-quit
}
