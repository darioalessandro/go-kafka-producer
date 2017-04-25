package main

import (
	"github.com/Shopify/sarama"
	"log"
	"time"
	"sync"
	"fmt"
)

var wg sync.WaitGroup

func main() {
	kafkaWriter := make(chan string, 1000)
	wg.Add(1)
	go kafkaMessageProducer(kafkaWriter)
	go supervisor(kafkaWriter)
	fmt.Println("Waiting for supervisor to die")
	wg.Wait()
	fmt.Println("Bye")
}

func kafkaMessageProducer(kafkaWriter chan string) {
	defer func() {
		log.Println("kafkaMessageProducer: closing kafka message producer")
	}()
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case t := <-ticker.C:
			var message = t.String()
			log.Printf("\nkafkaMessageProducer: sending %s", message)
			kafkaWriter <- message
		}
	}
}


func supervisor(kafkaWriter chan string) {
	deadletters := make(chan string,10)
	connected := make(chan bool, 10)

	defer func() {
		wg.Done()
	}()

	go kafkaRoutine(deadletters, connected, kafkaWriter)

	for {
		select {
		case deadLetter := <-deadletters:
			log.Printf("\nSupervisor: got dead letter from kafka, rescheduling %s\n", deadLetter)
			time.Sleep(2 * time.Second)
			go kafkaRoutine(deadletters, connected, kafkaWriter)

		case c := <- connected:
			log.Printf("Supervisor: got connected: %d from kafka", c)
		}
	}
}


func kafkaRoutine(	deadletters chan string,
			connected chan bool,
			kafkaWriter chan string) {

	const kafkanodename = "localhost:9092"
	const topic = "ava_notifications"

	log.Printf("\nKafkaRoutine attempting to connect: %s %s", kafkanodename, topic)

	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Sending message to reschedule kafka connection", r)
		}
		connected <- false
		deadletters <- "error"
	}()
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	producer, kafka_connectError := sarama.NewSyncProducer([]string{kafkanodename}, config)

	if kafka_connectError != nil {
		log.Panicf("kafka_connectError %s", kafka_connectError)
	}

	connected <- true

	msg := &sarama.ProducerMessage{Topic: topic, Value: sarama.StringEncoder("Connected to kafka")}
	_,_, kafka_helloErr := producer.SendMessage(msg)
	if kafka_helloErr != nil {
		log.Panicf("error sending msg to kafka %s", kafka_helloErr)
	}
	log.Printf("\nProducer: %s",producer)

	for {
		select {
		case message := <-kafkaWriter:
				log.Printf("\nkafkaRoutine: receiving %s", message)
				msg := &sarama.ProducerMessage{Topic: topic, Value: sarama.StringEncoder(message)}
				_,_, kafka_helloErr := producer.SendMessage(msg)
				if kafka_helloErr != nil {
					log.Panicf("error connecting to kafka %s", kafka_helloErr)
				}
		}
	}



}