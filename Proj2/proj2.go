package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

// GeoCoding API (converts ZIP code to longitude and latitude coordinates)
type ZIPResponse struct {
	Zip       string  `json:"zip"`
	Name      string  `json:"name"`
	Latitude  float32 `json:"lat"`
	Longitude float32 `json:"lon"`
	Country   string  `json:"country"`

	Cod     any `json:"cod"`
	Message any `json:"message"`
}

// Important information from API
type MainResponse struct {
	Temp        float32 `json:"temp"`
	FeelsLike   float32 `json:"feels_like"`
	MinTemp     float32 `json:"temp_min"`
	MaxTemp     float32 `json:"temp_max"`
	Pressure    int     `json:"pressure"`
	SeaLevel    int     `json:"sea_level"`
	GroundLevel int     `json:"grnd_level"`
	Humidity    int     `json:"humidity"`
}

// Weather information from API
type WeatherResponse struct {
	ID   int    `json:"id"`
	Main string `json:"main"`
	Desc string `json:"description"`
	Icon string `json:"icon"`
}

// Cloud information from API
type CloudResponse struct {
	All int `json:"all"`
}

// Wind information from API
type WindResponse struct {
	Speed float32 `json:"speed"`
	Deg   int     `json:"deg"`
	Gust  float32 `json:"gust"`
}

// Rain information from API
type RainResponse struct {
	Vol3h float32 `json:"3h"`
}

// Snow information from API
type SnowResponse struct {
	Vol3h float32 `json:"3h"`
}

// For each day
type DailyResponse struct {
	Time       int               `json:"dt"`
	Main       MainResponse      `json:"main"`
	Weather    []WeatherResponse `json:"weather"`
	Clouds     CloudResponse     `json:"clouds"`
	Wind       WindResponse      `json:"wind"`
	Visibility int               `json:"visibility"`
	Pop        float32           `json:"pop"`
	Rain       RainResponse      `json:"rain"`
	Snow       SnowResponse      `json:"snow"`
}

// Overall API Results
type APIResponse struct {
	Cod     any `json:"cod"`
	Message any `json:"message"`

	DaysList []DailyResponse `json:"list"`
}

// A structure based off of the user input (BEFORE converting ZIP code to coordinates)
type PreCoordinateRequest struct {
	Days    int
	ZIPCode string

	LineNum int
}

// A structure based off of the user input (AFTER converting ZIP code to coordinates)
type PostLocationRequest struct {
	Days    int
	Name    string
	Lat     float32
	Lon     float32
	ZIPCode string

	LineNum int
}

// End program if there was an error
func check(e error) {
	if e != nil {
		fmt.Println("ERROR", e)
		os.Exit(1)
	}
}

// Parses each line of the file into a Request
func parseLine(text string, lineNum int) (PreCoordinateRequest, bool) {

	// Split each line and make sure input is valid
	parameters := strings.Split(text, "|")

	// Requests must be two parameters (days and ZIP code)
	if len(parameters) != 2 {
		fmt.Printf("ERROR on Line %d: Only two parameters allowed (days and ZIP code, separated by '|'). Currently has %d parameters. Skipping Request.\n", lineNum, len(parameters))
		return PreCoordinateRequest{}, false
	}

	// The number of days to forecast is the first value (index 0)
	// The ZIP code to look at is the second value (index 1)

	// Trim the leading and trailing spaces of each string
	daysStr := strings.TrimSpace(parameters[0])
	ZIPcode := strings.TrimSpace(parameters[1])

	// Days must be a number
	days, err := strconv.Atoi(daysStr)
	if err != nil || days <= 0 {
		fmt.Printf("ERROR on Line %d: The number of days must be a positive number! It is currently '%s'. Skipping Request.\n", lineNum, parameters[0])
		return PreCoordinateRequest{}, false
	}

	// Days must also be less than or equal to 5 due to API restrictions
	if days > 5 {
		fmt.Printf("WARNING on Line %d: The number of days must be less than or equal to 5 (due to free API)! Changing %d days --> 5 days.\n", lineNum, days)
		days = 5
	}

	// If request made it here, that means it is valid
	// Create the pre request and return success
	return PreCoordinateRequest{Days: days, ZIPCode: ZIPcode, LineNum: lineNum}, true
}

// Convert the ZIP code to latitude and longitude coordinates using GeoCoding API call
func convertToCoordinates(req PreCoordinateRequest, key string) (PostLocationRequest, bool) {

	// Retrieves values from pre coordinate request
	days := req.Days
	zipCode := req.ZIPCode
	lineNum := req.LineNum

	fmt.Println("API Call for Line", lineNum)

	// Make API request to get coordinates (assuming UNITED STATES)
	url := fmt.Sprintf("http://api.openweathermap.org/geo/1.0/zip?zip=%s,US&appid=%s", zipCode, key)

	// Make a HTTP GET request to this URL, returning an HTTP response
	resp, err := http.Get(url)
	check(err)

	// Uses HTTP response body to create a JSON Decoder
	// Parses the JSON to fill the ZIPResponse structure
	var response ZIPResponse
	err = json.NewDecoder(resp.Body).Decode(&response)
	check(err)

	// Closes once response is decoded
	resp.Body.Close()

	// If API key was not valid, end the program
	if response.Cod == 401 {
		fmt.Println(response.Message)
		os.Exit(1)
	}
	// If GET request had an error finding results (BUT API KEY WAS VALID), skip this request
	if response.Cod == "404" {
		fmt.Printf("ERROR on Line %d: Cannot find results for ZIP code '%s'. Skipping this request.\n", lineNum, zipCode)
		return PostLocationRequest{}, false
	}

	// Create PostLocationRequest using values from the ZIPResponse
	latitude := response.Latitude
	longitude := response.Longitude
	name := response.Name

	return PostLocationRequest{Days: days, Lat: latitude, Lon: longitude, Name: name, ZIPCode: zipCode, LineNum: lineNum}, true
}

// Do the API call to get results from the request
func processRequest(req PostLocationRequest, key string, kWriters *KafkaWriters) {

	// Retrieves values from the post location request
	days := req.Days
	lat := req.Lat
	lon := req.Lon
	location := req.Name
	zipCode := req.ZIPCode
	lineNum := req.LineNum

	// Get correct count value, since API returns results for every three hours, we want 24 hours of results (24 / 3 = 8)
	cnt := days * 8

	// Make API request to get results (using imperial units)
	url := fmt.Sprintf("https://api.openweathermap.org/data/2.5/forecast?lat=%f&lon=%f&cnt=%d&units=imperial&appid=%s", lat, lon, cnt, key)

	// Make a HTTP GET request to this URL, returning an HTTP response
	resp, err := http.Get(url)
	check(err)

	// Uses HTTP response body to create a JSON Decoder
	// Parses the JSON to fill the response structure
	var results APIResponse
	err = json.NewDecoder(resp.Body).Decode(&results)
	check(err)

	// Closes once response is decoded
	resp.Body.Close()

	// If GET request had an error, print the error message and end program
	if results.Cod != "200" {
		fmt.Printf("ERROR with request on Line %d: %s\n", lineNum, results.Message)
		os.Exit(1)
	}

	// Uses a string Builder to make sure all input prints out together at once
	// This avoids concurrency issues
	var sb strings.Builder

	fmt.Fprintf(&sb, "\n")

	// Get results for given amount of days (multiplied by 8 since API does three hour increments, and we want 24 hour increments)
	for i := 0; i < days && i*8 < len(results.DaysList); i++ {
		// Running every 8th entry
		r := results.DaysList[i*8]
		curTime := time.Unix(int64(r.Time), 0)
		date := curTime.Format("2006-01-02")

		// Create metric-specific payloads to add to Kafka Writers
		tempPayload := TemperaturePayload{
			Location:  location,
			Date:      date,
			Temp:      float64(r.Main.Temp),
			FeelsLike: float64(r.Main.FeelsLike),
		}

		humidityPayload := HumidityPayload{
			Location: location,
			Date:     date,
			Humidity: float64(r.Main.Humidity),
		}

		windPayload := WindPayload{
			Location: location,
			Date:     date,
			Speed:    float64(r.Wind.Speed),
			Degree:   float64(r.Wind.Deg),
		}

		cloudPayload := CloudPayload{
			Location:     location,
			Date:         date,
			CloudPercent: float64(r.Clouds.All),
		}

		// Key for each payload is the ZIP code and the date (zipcode-date)
		key := fmt.Sprintf("%s-%s", zipCode, date)

		// Publish payloads to their specific Kafka writer topics
		tempBytes, _ := json.Marshal(tempPayload)
		kWriters.TempWriter.WriteMessages(context.Background(), kafka.Message{Key: []byte(key), Value: tempBytes})

		humidityBytes, _ := json.Marshal(humidityPayload)
		kWriters.HumidityWriter.WriteMessages(context.Background(), kafka.Message{Key: []byte(key), Value: humidityBytes})

		windBytes, _ := json.Marshal(windPayload)
		kWriters.WindWriter.WriteMessages(context.Background(), kafka.Message{Key: []byte(key), Value: windBytes})

		cloudBytes, _ := json.Marshal(cloudPayload)
		kWriters.CloudWriter.WriteMessages(context.Background(), kafka.Message{Key: []byte(key), Value: cloudBytes})
	}
}

// MAIN ENTRY INTO THE PROGRAM
func main() {
	// Keep track of how long it takes to run this program
	start := time.Now()

	// Gets API key from environmental variable
	key := os.Getenv("API_KEY")

	// Gets file path from environmental variable
	filePath := os.Getenv("FILE")

	// Gets the number of workers working in the worker pool from environmental variable
	workers := os.Getenv("WORKERS")

	// Makes sure user supplied their API Key
	if key == "" {
		fmt.Println("Please supply API Key to the docker-compose.yml file to run the program. \n" +
			"docker-compose run --rm proj2")
		return
	}

	// Remove quotes from (if it exists)
	key = strings.Trim(key, "'\"")
	filePath = strings.Trim(filePath, "'\"")
	workers = strings.Trim(workers, "'\"")

	// Default number of worker if input wasn't valid
	DEFAULT_NUM_WORKERS := 10

	// Makes sure number of workers input is valid
	numWorkers, err := strconv.Atoi(workers)
	if err != nil || numWorkers <= 0 {
		fmt.Printf("Number of workers needs to be an integer! It is currently %s. Defaulting to %d Workers.\n", workers, DEFAULT_NUM_WORKERS)
		numWorkers = DEFAULT_NUM_WORKERS
	}

	// Creates HTTP server for Prometheus
	go startMetrics()

	// Initialize Kafka Writers (that will be closed at the end of this program)
	kafkaWriters := initKafkaWriters()
	defer kafkaWriters.closeKafkaWriters()

	// Launch consumers for all topics
	topics := []string{"temperature", "humidity", "wind", "cloud"}

	// Make sure the topic exists and load cache for that topic
	for _, topic := range topics {
		ensureKafkaTopic(topic)
	}

	// Setup Grafana dashboard after Prometheus and Kafka are ready
	// Wait for Grafana to start (max 60 seconds)
	err = waitForGrafana(60 * time.Second)
	check(err)

	// Cancellable context for the consumer (Prometheus)
	ctx, cancel := context.WithCancel(context.Background())

	// Goroutine that consumes Kafka data and writes it into the metric channel
	var kafkaWG sync.WaitGroup
	for range numWorkers {
		for _, topic := range topics {
			kafkaWG.Go(func() { consumeKafkaTopic(ctx, topic) })
		}
	}

	// Goroutine that collects data from metric channel and writes it into Prometheus
	var promWG sync.WaitGroup
	for range numWorkers {
		promWG.Go(func() {
			// Will wait until data gets put into the requests channel
			for msg := range metricsChan {
				updateMetrics(msg)
			}
		})
	}

	// Create a channel that stores requests BEFORE doing the GeoCoding API call
	preCoordinateChan := make(chan PreCoordinateRequest)

	// Create channel of requests doing the actual API call after ZIP code was converted to coordinates
	requestsChan := make(chan PostLocationRequest)

	// Waitgroup that waits for all ZIP codes to be processed before program ends
	var zipCodeWG sync.WaitGroup

	// Goroutine that collects data from the preCoordinate channel
	// Worker pool created for parallel GeoCoding API Requests
	for range numWorkers {
		zipCodeWG.Go(func() {
			// Will wait until data gets put into the requests channel
			for req := range preCoordinateChan {

				// Will check if this request already has results
				exists := isInTSDB(req)

				// If not in Prometheus TSDB, must create a new request and call API
				if !exists {
					// Convert ZIP code to coordinates, then add to request channel
					newRequest, success := convertToCoordinates(req, key)
					if success {
						requestsChan <- newRequest
					}
				}
			}
		})
	}

	// Waitgroup that waits for all results to be processed before program ends
	var resultsWG sync.WaitGroup

	// Goroutine that collects data from the request channel
	// Worker pool created for parallel API Requests
	for range numWorkers {
		resultsWG.Go(func() {
			// Will wait until data gets put into the requests channel
			for req := range requestsChan {
				processRequest(req, key, kafkaWriters)
			}
		})
	}

	// Make sure file path for user input is correct
	file, err := os.Open(filePath)
	check(err)

	// Close the file once the program is complete
	defer file.Close()

	// A waitgroup used to wait for all the goroutines launched to finish when reading the lines from the file
	var fileWG sync.WaitGroup

	// Create scanner to read file
	scanner := bufio.NewScanner(file)

	// Store line number of request
	lineNumber := 0

	// Reads file line by line concurrently (using goroutines and waitgroups)
	for scanner.Scan() {
		// Get text on current line
		text := scanner.Text()

		// Make a copy of the line number after its incrementation for better error messages
		lineNumber++
		currentLine := lineNumber

		// Each of these goroutines work concurrently
		fileWG.Go(func() {

			// Validate the current request
			req, success := parseLine(text, currentLine)

			// If it is valid, send to precoordinate channel for further processing
			if success {
				preCoordinateChan <- req
			}
		})
	}

	// Checks if there was an error reading the file
	check(scanner.Err())

	// SOME BUFFER TIME FOR EVERYTHING TO PROCESS CORRECTLY
	// Really wanted to avoid doing this, but it seemed that there was no other option
	time.Sleep(100 * time.Millisecond)

	// Waits for all lines to be read
	fileWG.Wait()

	// If there were no errors, close the precoordinate channel
	close(preCoordinateChan)

	// Waits for all pre-coordinate requests to be converted to coordinates
	zipCodeWG.Wait()

	// If there were no errors, close the requests channel
	close(requestsChan)

	// Waits for all API calls to be completed
	resultsWG.Wait()

	// Tells prometheus to stop processing messages and kafka to stop reading them
	cancel()

	// Wait for all logs to be read
	kafkaWG.Wait()

	// Wait for all metrics to be put in Prometheus and TSDB
	close(metricsChan)
	promWG.Wait()

	// Once ready, push dashboards
	setupGrafana()

	fmt.Println("\nPrometheus metrics available at http://localhost:8080/metrics")
	fmt.Println("Set up Grafana dashboards at http://localhost:3000 (user: admin, pass: admin). Metrics may take ~10 seconds to show.")

	// Once all lines of the file are read and the results are processed, the program can end
	fmt.Printf("\nProgram took %s to run.\n\nPress 'ENTER' to shut down server.\n", time.Since(start))

	// Wait for user to press Enter to stop the server
	bufio.NewReader(os.Stdin).ReadBytes('\n')
}
