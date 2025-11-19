package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

// KAFKA PORT USED
var (
	brokerPort  string = "kafka:9092"
	metricsChan        = make(chan WeatherMessage)
)

// Structure that holds all writer instances for different topics
// The writers handles all connections, partition selection, batching, and retries automatically
type KafkaWriters struct {
	TempWriter     *kafka.Writer
	HumidityWriter *kafka.Writer
	WindWriter     *kafka.Writer
	CloudWriter    *kafka.Writer
}

// Holds all metrics for a given ZIP-Date key
//type CachedWeather struct {
//	Temperature float64
//	FeelsLike   float64
//	Humidity    float64
//	WindSpeed   float64
//	WindDegree  float64
//	Cloud       float64
//}

// Structure that holds the consumer data that will be sent to Prometheus
type WeatherMessage struct {
	Topic       string
	Zip         string
	Date        string
	Temperature float64 `json:"Temp"`
	FeelsLike   float64 `json:"FeelsLike"`
	Humidity    float64 `json:"Humidity"`
	WindSpeed   float64 `json:"Speed"`
	WindDegree  float64 `json:"Degree"`
	Cloud       float64 `json:"CloudPercent"`
}

// ALL PAYLOADS FOR EACH WRITER
// The basis for each payload requires a location and a time

// Temperature Payload
type TemperaturePayload struct {
	Location  string
	Date      string
	Temp      float64
	FeelsLike float64
}

// Humidity Payload
type HumidityPayload struct {
	Location string
	Date     string
	Humidity float64
}

// Wind Payload
type WindPayload struct {
	Location string
	Date     string
	Speed    float64
	Degree   float64
}

// Cloud Payload
type CloudPayload struct {
	Location     string
	Date         string
	CloudPercent float64
}

// Waits for Kafka to be set up
func waitForKafka() {
	retryDelay := 2 * time.Second

	// Once Kafka is officially setup and this connection is successful, the function will finish
	for {
		conn, err := kafka.Dial("tcp", brokerPort)

		if err == nil {
			conn.Close()
			return
		}
		fmt.Println("Kafka is not ready yet. Retrying...")
		time.Sleep(retryDelay)
	}
}

// Ensures a Kafka topic exists
// If doesn't, will be created
func ensureKafkaTopic(topic string) {

	// Connect to the Kafka broker
	conn, err := kafka.Dial("tcp", brokerPort)
	check(err)
	defer conn.Close()

	// Check if the topic already exists by reading its partitions
	partitions, err := conn.ReadPartitions(topic)

	// If partitions are returned, that means the topic exists so the program can end
	if err == nil && len(partitions) > 0 {
		return
	}

	// If program reached here, that means the topic does not exist, so we need to create it

	// First, find the Kafka controller (responsible for topic creation)
	// In Kafka, only the controller broker can create topics
	controller, err := conn.Controller()
	check(err)

	// Connect to the Kafka controller
	controllerConn, err := kafka.Dial("tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	check(err)
	defer controllerConn.Close()

	// Define topic configuration: 1 partition, 1 replica
	topicConfigs := []kafka.TopicConfig{
		{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
	}

	// Send request to Kafka controller to create the topic
	err = controllerConn.CreateTopics(topicConfigs...)
	check(err)
}

// Initializes all of the Kafka Writers
func initKafkaWriters() *KafkaWriters {

	waitForKafka()

	// Writer for the temperature topic
	tWriter := kafka.NewWriter(kafka.WriterConfig{
		// Broker allows applications to communicate asynchronously by exchanging messages
		Brokers:      []string{brokerPort},
		Topic:        "temperature",
		BatchTimeout: 10 * time.Millisecond,
		BatchSize:    1,
	})

	// Writer for the humidity topic
	hWriter := kafka.NewWriter(kafka.WriterConfig{
		// Broker allows applications to communicate asynchronously by exchanging messages
		Brokers:      []string{brokerPort},
		Topic:        "humidity",
		BatchTimeout: 10 * time.Millisecond,
		BatchSize:    1,
	})

	// Writer for the wind topic
	wWriter := kafka.NewWriter(kafka.WriterConfig{
		// Broker allows applications to communicate asynchronously by exchanging messages
		Brokers:      []string{brokerPort},
		Topic:        "wind",
		BatchTimeout: 10 * time.Millisecond,
		BatchSize:    1,
	})

	// Writer for the cloud topic
	cWriter := kafka.NewWriter(kafka.WriterConfig{
		// Broker allows applications to communicate asynchronously by exchanging messages
		Brokers:      []string{brokerPort},
		Topic:        "cloud",
		BatchTimeout: 10 * time.Millisecond,
		BatchSize:    1,
	})

	return &KafkaWriters{TempWriter: tWriter, HumidityWriter: hWriter, WindWriter: wWriter, CloudWriter: cWriter}
}

// Reads messages that come through topics
func consumeKafkaTopic(ctx context.Context, topic string) {

	// Creates a new Kafka reader to read data coming from this topic
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{brokerPort},
		Topic:       topic,
		StartOffset: kafka.FirstOffset,
		MaxWait:     100 * time.Millisecond,
	})
	defer reader.Close()

	for {
		// If program is still running, read incoming messages
		m, err := reader.ReadMessage(ctx)

		// When program is over, stop reading messages
		// This context will get cancelled at the end of the program
		if errors.Is(err, context.Canceled) {
			return
		}

		// Unmarshal the JSON string into the WeatherMessage structure
		var msg WeatherMessage
		err = json.Unmarshal(m.Value, &msg)
		check(err)

		// Break up key into ZIP code and Date
		keyParts := strings.SplitN(string(m.Key), "-", 2)
		msg.Zip = keyParts[0]
		msg.Date = keyParts[1]

		// Track which topic the message came from
		msg.Topic = topic

		// Adds message to the metrics channel
		metricsChan <- msg
	}
}

// Closes all of the Writers at the end of this program
func (w *KafkaWriters) closeKafkaWriters() {
	// Creates a slice of all writers for this program
	writers := []*kafka.Writer{w.TempWriter, w.HumidityWriter, w.WindWriter, w.CloudWriter}

	// Waitgroup to close these channels concurrently
	var wg sync.WaitGroup

	// Closes each one concurrently
	for _, writer := range writers {
		w := writer
		wg.Go(func() {
			err := w.Close()
			check(err)
		})
	}

	// Wait for programs to close
	wg.Wait()
}
