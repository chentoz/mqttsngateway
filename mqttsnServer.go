package main

import (
	"encoding/binary"
	"encoding/hex"
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

func handleMqttSNPacket(connection *net.UDPConn, quit chan struct{}, update chan *m2a, outport *string) {

	buffer := make([]byte, 1024)
	n, remoteAddr, err := 0, new(net.UDPAddr), error(nil)

	var hdrHeartBeat mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {

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

		if udpAddr == nil {
			log.Printf("mac string not found : %v", macstring)
		} else {
			_, err = connection.WriteToUDP(buffer[0:n], udpAddr)
			if err != nil {
				log.Printf("Error when re-sending : %v \n", err.Error())
				//quit <- struct{}{}
			}
		}
	}

	opts := mqtt.NewClientOptions().
		AddBroker("tcp://localhost:" + *outport).
		SetClientID(fmt.Sprintf("mqtt-benchmark-%v-%v", time.Now().Format(time.RFC3339Nano), "MQTTSN-Gateway-golang")).
		SetCleanSession(true).
		SetAutoReconnect(true).
		SetOnConnectHandler(func(client mqtt.Client) {
			if token := client.Subscribe("HeartBeatAck", 0, hdrHeartBeat); token.Wait() && token.Error() != nil {
				log.Println(token.Error())
				quit <- struct{}{}
			} else {
				log.Println("Subscribe topic HeartBeatAck success")
			}

		}).
		SetConnectionLostHandler(func(client mqtt.Client, reason error) {
			log.Printf("CLIENT %v lost connection to the broker: %v. Will reconnect...\n", "MQTTSN-Gateway-golang", reason.Error())
		})
	client := mqtt.NewClient(opts)

	token := client.Connect()
	token.Wait()
	if token.Error() != nil {
		log.Printf("CLIENT %v had error connecting to the broker: %v\n", "MQTTSN-Gateway", token.Error())
		quit <- struct{}{}
	}

	for err == nil {

		n, remoteAddr, err = connection.ReadFromUDP(buffer)

		var topic string

		if n < 8 {
			continue
		}

		if buffer[3] == 0x42 {
			topic = "BLELocation"
		} else if buffer[3] == 0x47 {
			topic = "GPSLocation"
		} else if buffer[3] == 0x48 {
			topic = "HeartBeat"
		} else {
			log.Println("Unrecognized topic")
			continue
		}

		if strings.TrimSpace(string(buffer[0:n])) == "STOP" {
			log.Println("Exiting UDP Server")
			quit <- struct{}{} // quit
		}

		mqttsnMessage := buffer[7:n]
		macstring := strings.ToUpper(hex.EncodeToString(mqttsnMessage[2 : 2+6]))

		update <- &m2a{macstring, remoteAddr}

		log.Printf("Sending : %v\n", macstring)
		token := client.Publish(topic, 0, false, mqttsnMessage)
		if token.Error() != nil {
			log.Println("CLIENT Error sending message")
		}
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
		go handleMqttSNPacket(connection, quit, update, outport)
	}

	<-quit
}
