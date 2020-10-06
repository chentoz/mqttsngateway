package main

import (
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

type m2a struct{
	mac string
	addr *net.UDPAddr
}

var (
	rwm     sync.RWMutex
	macStr2Addr map[string]*net.UDPAddr = make(map[string]*net.UDPAddr)
	appName string = "MQTTSN-Gateway-Golang"
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

func updateMacMap(update chan *m2a) {
	for {
		ud := <- update
		set(ud.mac, ud.addr)
	}
}

func handleMqttSnMessage(message []byte , rinfo *net.UDPAddr, client *mqtt.Client, update chan *m2a) error {

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
		log.Println("Exiting UDP Server")
		//quit <- struct{}{} // quit
		return errors.New("Exiting Server")
	}
	mqttsnMessage := message[7:n]

	macstring := strings.ToUpper(hex.EncodeToString(mqttsnMessage[2 : 2+6]))
	update <- &m2a{macstring, rinfo}

	log.Printf("%v sending : %v\n",appName, macstring)
	token := (*client).Publish(topic, 0, false, mqttsnMessage)
	if token.Error() != nil {
		log.Printf("%v : error sending message : %v \n", appName, token.Error())
		return token.Error()
	}
	return nil
}

func hdlcDecode(message []byte, rinfo *net.UDPAddr, client *mqtt.Client, update chan *m2a){
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
		} else if message[i] == 0x7d && i + 1 < msgLen {
			if message[i + 1] == 0x5e {
				frame[targetIdx], targetIdx = 0x7e, targetIdx + 1
				i = i + 1
			} else if message[i + 1] == 0x5d {
				frame[targetIdx], targetIdx = 0x7d, targetIdx + 1
				i = i + 1
			} else {
				frame[targetIdx], targetIdx = message[i], targetIdx + 1
			}
		} else {
			frame[targetIdx], targetIdx = message[i], targetIdx + 1
		}
		log.Printf("************************************\n Total Packets : %v\n ************************************ \n", totalPacket)
	}
}

func hdlcEncode(message []byte) []byte{
	msgLength := len(message)
	encodedFrame := make([]byte, msgLength)
	encodedFrame[0] = 0x7e
	targetIdx := 1
	for srcIdx := 0; srcIdx < msgLength; srcIdx++ {
		if message[srcIdx] == 0x7e {
			encodedFrame[targetIdx] = 0x7d
			targetIdx, encodedFrame[targetIdx] = targetIdx + 1, 0x5e
			targetIdx++
		} else if message[srcIdx] == 0x7d {
			encodedFrame[targetIdx] = 0x7d
			targetIdx, encodedFrame[targetIdx] = targetIdx +1, 0x5d
			targetIdx++
		} else {
			encodedFrame[targetIdx], targetIdx = message[srcIdx], targetIdx + 1
		}
	}
	encodedFrame[targetIdx], targetIdx = 0x7e, targetIdx +1
	return encodedFrame[0: targetIdx]
}

func processUDPPackets(connection *net.UDPConn, quit chan struct{}, update chan *m2a, outport *string) {

	n, remoteAddr, err := 0, new(net.UDPAddr), error(nil)

	var hdrHeartBeatAck mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {

		msgTypeByte := byte(0x0c)
		flagByte := byte(0x62)
		tidBytes := make([]byte, 2)
		tidBytes[0] = byte('H')
		tidBytes[1] = byte('B')
		midBytes := make([]byte, 2)
		binary.BigEndian.PutUint16(midBytes, 0x0b00)
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

		frame := hdlcEncode(msg.Payload())

		if udpAddr == nil {
			log.Printf("mac string not found : %v", macstring)
		} else {
			_, err := connection.WriteToUDP(frame, udpAddr)
			if err != nil {
				log.Printf("Error when re-sending : %v \n", err.Error())
				//quit <- struct{}{}
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
				quit <- struct{}{}
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
		quit <- struct{}{}
	}

	buffer := make([]byte, 4096)
	for err == nil {
		n, remoteAddr, err = connection.ReadFromUDP(buffer)

		hdlcDecode(buffer[0:n], remoteAddr, &client, update)
	}
}

func main() {
	maxCores := runtime.GOMAXPROCS(runtime.NumCPU())

	var (
		inport   = flag.String("inport", "31337", "MQTTSN packet source port")
		outport   = flag.String("outport", "43518", "MQTT packet destination port")
		workers   = flag.Int("workers",  maxCores , "Total Works")
	)
	flag.Parse()

	s, err := net.ResolveUDPAddr("udp4", ":" + *inport)
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

	go updateMacMap(update)

	quit := make(chan struct{})

	actualWorkers := 1
	if *workers > runtime.NumCPU() {
		actualWorkers = runtime.NumCPU()
	} else {
		actualWorkers = *workers
	}
	for i := 0; i < actualWorkers; i++ {
		go processUDPPackets(connection, quit, update, outport)
	}

	<-quit
}
