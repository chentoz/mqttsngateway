package main

import (
	"encoding/hex"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"runtime"
	"strings"
	"time"

	HB "github.com/chentoz/mqttsngateway/HeartBeat"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"google.golang.org/protobuf/proto"
)

func random(min, max int) int {
	return rand.Intn(max-min) + min
}

func handleMqttSNPacket(connection *net.UDPConn, quit chan struct{}) {

	buffer := make([]byte, 1024)
	n, remoteAddr, err := 0, new(net.UDPAddr), error(nil)
	MacStr2Addr := make(map[string]net.UDPAddr)

	// mqtt client for workers
	var hdrHeartBeat mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {

		hb := &HB.HeartBeat{}
		proto.Unmarshal(msg.Payload(), bleLocation)

		length := len(bleLocation.Id)
		fmt.Printf("Get : %v \n", string(bleLocation.Id[:length]))

		_, err = connection.WriteToUDP(buffer[0:n], remoteAddr)
		if err != nil {
			fmt.Printf("Error when re-sending : %v \n", strings(err))
			quit <- struct{}{}
		}
	}

	opts := mqtt.NewClientOptions().
		AddBroker("tcp://localhost:31337").
		SetClientID(fmt.Sprintf("mqtt-benchmark-%v-%v", time.Now().Format(time.RFC3339Nano), "MQTTSN-Gateway-worker")).
		SetCleanSession(true).
		SetAutoReconnect(true).
		SetOnConnectHandler(func(client mqtt.Client) {
			if token := client.Subscribe("HeartBeatAck", 0, hdrHeartBeat); token.Wait() && token.Error() != nil {
				fmt.Println(token.Error())
				quit <- struct{}{}
			} else {
				fmt.Print("Subscribe topic " + "HeartBeat" + " success\n")
			}

		}).
		SetConnectionLostHandler(func(client mqtt.Client, reason error) {
			log.Printf("CLIENT %v lost connection to the broker: %v. Will reconnect...\n", "MQTTSN-Gateway", reason.Error())
		})
	client := mqtt.NewClient(opts)

	token := client.Connect()
	token.Wait()
	if token.Error() != nil {
		log.Printf("CLIENT %v had error connecting to the broker: %v\n", "MQTTSN-Gateway", token.Error())
		quit <- struct{}{}
	}

	for err == nil {
		n, remoteAddr, err := connection.ReadFromUDP(buffer)

		var topic string
		if buffer[3] == 0x42 {
			topic = "BLELocation"
		} else if buffer[3] == 0x47 {
			topic = "GPSLocation"
		} else if buffer[3] == 0x48 {
			topic = "HeartBeat"
		} else {
			fmt.Println("Unrecognized topic")
			continue
		}

		if strings.TrimSpace(string(buffer[0:n])) == "STOP" {
			fmt.Println("Exiting UDP Server")
			quit <- struct{}{} // quit
		}

		mqttsnMessage := buffer[7:n]
		macstring := hex.EncodeToString(mqttsnMessage[2 : 2+6])
		MacStr2Addr[macstring] = remoteAddr

		token := client.Publish(topic, 0, false, mqttsnMessage)
		if token.Error() != nil {
			fmt.Println("CLIENT Error sending message")
		}
	}
}

func main() {
	arguments := os.Args
	runtime.GOMAXPROCS(runtime.NumCPU())

	if len(arguments) == 1 {
		fmt.Println("Please provide a port number")
		return
	}
	PORT := ":" + arguments[1]

	s, err := net.ResolveUDPAddr("udp4", PORT)
	if err != nil {
		fmt.Println(err)
		return
	}

	connection, err := net.ListenUDP("udp4", s)
	if err != nil {
		fmt.Println(err)
		return
	}

	defer connection.Close()

	quit := make(chan struct{})
	for i := 0; i < runtime.NumCPU(); i++ {
		go handleMqttSNPacket(connection, quit)
	}

	<-quit
}
