package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Define Prometheus metrics
var (
	// line-delimited JSON (used as TSDB)
	metricsFilePath  = "/data/metrics.jsonl"
	metricsFileMutex sync.Mutex

	// Alerts
	tempLow, tempHigh         float64
	humidityLow, humidityHigh float64
	windHigh                  float64

	// Help description
	tempHelp       = "Temperature in Fahrenheit"
	humidityHelp   = "Humidity Percentage"
	windSpeedHelp  = "Wind Speed in MPH"
	windDegreeHelp = "Wind Direction in Degrees"
	cloudHelp      = "Cloud cover percentage"

	// PROMETHEUS GAUGES FOR EACH TOPIC
	tempGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "temperature",
			Help: tempHelp,
		},
		[]string{"location", "date"},
	)
	feelsLikeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "feelslike",
			Help: tempHelp,
		},
		[]string{"location", "date"},
	)
	humidityGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "humidity",
			Help: humidityHelp,
		},
		[]string{"location", "date"},
	)
	windSpeedGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "wind_speed",
			Help: windSpeedHelp,
		},
		[]string{"location", "date"},
	)
	windDegreeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "wind_degree",
			Help: windDegreeHelp,
		},
		[]string{"location", "date"},
	)
	cloudGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cloud_percent",
			Help: cloudHelp,
		},
		[]string{"location", "date"},
	)

	// ALERTS
	alertTempHigh = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "alert_temperature_high",
			Help: "1 if temperature is above TEMP_HIGH, else 0",
		},
		[]string{"location", "date"},
	)
	alertTempLow = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "alert_temperature_low",
			Help: "1 if temperature is below TEMP_LOW, else 0",
		},
		[]string{"location", "date"},
	)
	alertHumidityHigh = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "alert_humidity_high",
			Help: "1 if humidity is above HUMIDITY_HIGH, else 0",
		},
		[]string{"location", "date"},
	)
	alertHumidityLow = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "alert_humidity_low",
			Help: "1 if humidity is below HUMIDITY_LOW, else 0",
		},
		[]string{"location", "date"},
	)
	alertWindHigh = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "alert_wind_high",
			Help: "1 if wind speed is above WIND_SPEED_HIGH, else 0",
		},
		[]string{"location", "date"},
	)
)

// Stores all registered metrics for this program
var registeredMetrics = make(map[string]struct{})

// Checks the map to make sure Prometheus doesn't register maps more than once
func safeRegister(c prometheus.Collector, name string) {
	if _, exists := registeredMetrics[name]; !exists {
		prometheus.MustRegister(c)
		registeredMetrics[name] = struct{}{}
	}
}

// Ran before main()
func init() {
	// Register metrics with the default registry safely
	safeRegister(tempGauge, "temperature")
	safeRegister(feelsLikeGauge, "feelslike")
	safeRegister(humidityGauge, "humidity")
	safeRegister(windSpeedGauge, "wind_speed")
	safeRegister(windDegreeGauge, "wind_degree")
	safeRegister(cloudGauge, "cloud_percent")

	safeRegister(alertTempHigh, "alert_temperature_high")
	safeRegister(alertTempLow, "alert_temperature_low")
	safeRegister(alertHumidityHigh, "alert_humidity_high")
	safeRegister(alertHumidityLow, "alert_humidity_low")
	safeRegister(alertWindHigh, "alert_wind_high")

	// Make sure alert values set up in docker-compose.yml are valid
	// If they are not valid, use default values
	var err error
	tempLow, err = strconv.ParseFloat(os.Getenv("TEMP_LOW"), 64)
	if err != nil {
		tempLow = 32
	}
	tempHigh, err = strconv.ParseFloat(os.Getenv("TEMP_HIGH"), 64)
	if err != nil {
		tempHigh = 90
	}
	humidityLow, err = strconv.ParseFloat(os.Getenv("HUMIDITY_LOW"), 64)
	if err != nil {
		humidityLow = 30
	}
	humidityHigh, err = strconv.ParseFloat(os.Getenv("HUMIDITY_HIGH"), 64)
	if err != nil {
		humidityHigh = 70
	}
	windHigh, err = strconv.ParseFloat(os.Getenv("WIND_SPEED_HIGH"), 64)
	if err != nil {
		windHigh = 40
	}
}

// Starts the HTTP server for Prometheus (avaliable at localhost:8080/metrics)
func startMetrics() {
	http.Handle("/metrics", promhttp.Handler())
	if err := http.ListenAndServe(":8080", nil); err != nil {
		fmt.Println("Prometheus HTTP server failed:", err)
		os.Exit(1)
	}
}

// Updates metrics for Prometheus by reading Kafka log data
// This function will be called when a metric is found in the metricChan
func updateMetrics(msg WeatherMessage) {

	// Update Gauges with metric data from Kafka for EACH topic
	// Also sets alert gauges if necessary
	switch msg.Topic {
	case "temperature":
		tempGauge.WithLabelValues(msg.Zip, msg.Date).Set(msg.Temperature)
		feelsLikeGauge.WithLabelValues(msg.Zip, msg.Date).Set(msg.FeelsLike)

		// Set alert gauge to 1 or 0 depending on temperature
		if msg.Temperature > tempHigh {
			alertTempHigh.WithLabelValues(msg.Zip, msg.Date).Set(1)
		} else {
			alertTempHigh.WithLabelValues(msg.Zip, msg.Date).Set(0)
		}

		if msg.Temperature < tempLow {
			alertTempLow.WithLabelValues(msg.Zip, msg.Date).Set(1)
		} else {
			alertTempLow.WithLabelValues(msg.Zip, msg.Date).Set(0)
		}
	case "humidity":
		humidityGauge.WithLabelValues(msg.Zip, msg.Date).Set(msg.Humidity)

		// Set alert gauge to 1 or 0 depending on humidity
		if msg.Humidity > humidityHigh {
			alertHumidityHigh.WithLabelValues(msg.Zip, msg.Date).Set(1)
		} else {
			alertHumidityHigh.WithLabelValues(msg.Zip, msg.Date).Set(0)
		}

		if msg.Humidity < humidityLow {
			alertHumidityLow.WithLabelValues(msg.Zip, msg.Date).Set(1)
		} else {
			alertHumidityLow.WithLabelValues(msg.Zip, msg.Date).Set(0)
		}

	case "wind":
		windSpeedGauge.WithLabelValues(msg.Zip, msg.Date).Set(msg.WindSpeed)
		windDegreeGauge.WithLabelValues(msg.Zip, msg.Date).Set(msg.WindDegree)

		// Set alert gauge to 1 or 0 depending on wind speed
		if msg.WindSpeed > windHigh {
			alertWindHigh.WithLabelValues(msg.Zip, msg.Date).Set(1)
		} else {
			alertWindHigh.WithLabelValues(msg.Zip, msg.Date).Set(0)
		}

	case "cloud":
		cloudGauge.WithLabelValues(msg.Zip, msg.Date).Set(msg.Cloud)
	}

	// Update the TSDB (persistence between programs)
	// Append the message to the JSONL file
	metricsFileMutex.Lock()
	defer metricsFileMutex.Unlock()

	// Begins by opening the metric file in the volume
	file, err := os.OpenFile(metricsFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Println("Error opening metrics file:", err)
		return
	}
	defer file.Close()

	// Marshals the message so it becomes data stream of bytes
	data, err := json.Marshal(msg)
	if err != nil {
		log.Println("Error marshaling metrics message:", err)
		return
	}

	// Write this data into the file
	file.Write(data)
	file.Write([]byte("\n"))
}

// Returns whether or not the given request was found in the Prometheus database
func isInTSDB(req PreCoordinateRequest) bool {

	// Gets ZIP code and the furthest date in YYYY-MM-DD format
	zip := req.ZIPCode
	date := time.Now().AddDate(0, 0, req.Days-1).Format("2006-01-02")

	// Opens the metric volume file
	file, err := os.Open(metricsFilePath)
	if err != nil {
		return false
	}
	defer file.Close()

	// Reads this file
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var msg WeatherMessage

		// Each line will be converted to a msg structure
		err := json.Unmarshal(scanner.Bytes(), &msg)
		if err != nil {
			continue
		}

		// If the same values are found as the request, then that means the API does NOT need to be called anymore
		if msg.Zip == zip && msg.Date == date {
			fmt.Printf("Found metric for %s-%s in file\n", zip, date)
			return true
		}
	}

	return false
}
