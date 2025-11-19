package main

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// Global variables
var (
	// Reference to database (news_cache.db)
	db *sql.DB

	// Channel for writing results to DB safely
	// Holds the request as well as its corresponding response
	writeChan chan reqNresp

	// Mutex used to check cache to see if query has been asked before
	cacheMu sync.RWMutex
	cache   = make(map[string]*reqNresp)

	// All workers with the same query (and correct parameters) use the same mutex.
	queryMutexesMu sync.Mutex
	queryMutexes   = make(map[string]*RequestMutex)
)

// Structure for blocking off certain requests if similar requests are being processed
type RequestMutex struct {
	Request SearchRequest
	Mutex   *sync.Mutex
}

// A structure based off of the user request
type SearchRequest struct {
	Query string
	Days  string
	Limit string
}

// Structure for the source of each Article
type Source struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// Structure for each article that the API returns
type Article struct {
	Source      Source `json:"source"`
	Author      string `json:"author"`
	Title       string `json:"title"`
	Description string `json:"description"`
	URL         string `json:"url"`
	URLToImage  string `json:"urlToImage"`
	PublishedAt string `json:"publishedAt"`
	Content     string `json:"content"`
}

// The initial response response from the API contains status, totalResults, and the articles
// Use of JSON tags to map JSON fields to Go fields
type NewsAPIResponse struct {
	Status       string    `json:"status"`
	TotalResults int       `json:"totalResults"`
	Articles     []Article `json:"articles"`
	Message      string    `json:"message"`
}

// Structure used to write results to DB safely, storing request and corresponding result
type reqNresp struct {
	req  SearchRequest
	resp NewsAPIResponse
}

// Panic if there was an error
func check(e error) {
	if e != nil {
		panic(e)
	}
}

// Parses each line of the file into a Request
func parseLine(text string, lineNum int) (SearchRequest, bool) {

	// Split each line and make sure input is valid
	parameters := strings.Split(text, "|")

	// Requests must be three parameters
	if len(parameters) != 3 {
		fmt.Printf("Only three parameters allowed per line (query, days, and limit, separated by '|'). Line %d has %d parameters.\n", lineNum, len(parameters))
		return SearchRequest{}, false
	}

	// The search term is the first value (index 0)
	// The number of days since published is the second value (index 1)
	// The amount of articles displayed (limit) is the third value (index 2)

	// Trim the leading and trailing spaces of each string
	query := strings.TrimSpace(parameters[0])
	daysStr := strings.TrimSpace(parameters[1])
	limit := strings.TrimSpace(parameters[2])

	// Days must be a number
	days, err := strconv.Atoi(daysStr)
	if err != nil || days <= 0 {
		fmt.Printf("The number of days must be a positive number! On Line %d, it is currently '%s'.\n", lineNum, parameters[1])
		return SearchRequest{}, false
	}

	// Convert the day number to an actual date (Ex: if days was 1, date would be today, if it was 2, date would be yesterday, etc...)
	date := time.Now().AddDate(0, 0, -(days - 1)).Format("2006-01-02")

	// Limit must be a number (but still will be put into the request as a string since it is put into a URL for API calls)
	limitVal, err := strconv.Atoi(limit)
	if err != nil || limitVal <= 0 {
		fmt.Printf("The limit must be a positive number! On Line %d, it is currently '%s'\n.", lineNum, parameters[2])
		return SearchRequest{}, false
	}

	// If request made it here, that means it is valid
	// Create the request and return success
	return SearchRequest{Query: query, Days: date, Limit: limit}, true
}

// Creates the database using sqlite
func createDatabase() {
	var err error

	// Open the database
	db, err = sql.Open("sqlite", "./news_cache.db")
	check(err)

	// Limit database connections to a single open and idle connection
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	// Create the table (if this is the first time the program is run)
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS articles (
			query TEXT NOT NULL,
			days TEXT NOT NULL,
			data TEXT NOT NULL,
			PRIMARY KEY (query, days)
		)
	`)
	check(err)

	// Allows concurrent reading and writing (has limited effect due to open/idle connection limit)
	_, err = db.Exec("PRAGMA journal_mode=WAL;")
	check(err)
}

// Load current query from the Database, and return true if was found
func loadFromDatabase(req SearchRequest) (*NewsAPIResponse, bool) {

	// Query the table to check if database results can be used instead of using API
	row := db.QueryRow(`
		SELECT data FROM articles
		WHERE query = ? AND days <= ?`,
		req.Query, req.Days)

	// Store result from the query
	var data string

	// If there were no results in the query, return to process request using API
	err := row.Scan(&data)
	if err != nil {
		return nil, false
	}

	// Store the JSON response
	var response NewsAPIResponse

	// Attempt to unmarshal the JSON string from the database into the response struct.
	err = json.Unmarshal([]byte(data), &response)
	check(err)

	// If everything succeeds, return the response and true.
	return &response, true

}

// Save the response data to the database
func saveToDatabase(req SearchRequest, resp NewsAPIResponse) {

	// Convert the NewsAPIResponse struct to a JSON string for storage
	data, _ := json.Marshal(resp)

	// Adds a new row to the database with the given API data
	_, err := db.Exec(`
		INSERT OR REPLACE INTO articles (query, days, data)
		VALUES (?, ?, ?)`,
		req.Query, req.Days, string(data),
	)
	check(err)
}

// Processes the current request
func processRequest(request SearchRequest, apiKey string) {

	// Get query
	query := request.Query

	// Check the in-memory cache to see if request was asked previously
	cacheMu.RLock()
	mem, inCache := cache[query]
	cacheMu.RUnlock()

	// If it was asked (and current request has all results the cached request had)
	// Print the response based off of the map
	if inCache {
		cacheDate, _ := time.Parse("2006-01-02", mem.req.Days)
		requestDate, _ := time.Parse("2006-01-02", request.Days)

		if !cacheDate.After(requestDate) {
			printResponse(request, mem.resp, "CACHE")
			return
		}
	}

	// IF NOT IN THE DATABASE OR THE CACHE, DO AN API CALL
	// Makes sure spaces are handled if they are in the request
	q := url.QueryEscape(request.Query)

	// Create the URL using fields from the request and the API Key
	url := "https://newsapi.org/v2/everything?q=" + q + "&from=" + request.Days + "&sortBy=popularity&apiKey=" + apiKey

	// Make a HTTP GET request to this URL, returning an HTTP response
	resp, err := http.Get(url)
	check(err)

	// Uses HTTP response body to create a JSON Decoder
	// Parses the JSON to fill the response structure
	var response NewsAPIResponse
	err = json.NewDecoder(resp.Body).Decode(&response)
	check(err)

	// Closes once response is decoded
	resp.Body.Close()

	// If GET request had an error, print the error message
	if response.Status == "error" {
		panic(response.Message)
	}

	// Save the data to the database via the write channel
	writeChan <- reqNresp{req: request, resp: response}

	// Save to in-memory cache if it has more data than previous cached query, or this is the first instance of that query
	cacheMu.Lock()
	cache[query] = &reqNresp{req: request, resp: response}
	cacheMu.Unlock()

	// Print the response
	printResponse(request, response, "API")
}

// Prints the response from the request
func printResponse(req SearchRequest, resp NewsAPIResponse, location string) {

	// Uses a string Builder to make sure all input prints out together at once
	// This avoids concurrency issues
	var sb strings.Builder

	// Parse requested limit
	reqLimit, _ := strconv.Atoi(req.Limit)
	articleLength := len(resp.Articles)

	// Display that request was processed
	fmt.Fprintf(&sb, "\n--- USING: %s, RESULTS FOR QUERY: %s (Days=%s, Limit=%d) ---\n", location, req.Query, req.Days, reqLimit)

	// Keeps track of the minimum date in Time format
	minDate, _ := time.Parse("2006-01-02", req.Days)

	// Keeps track of how many requests were printed
	printed := 0

	// Print results
	// For each of the top results, print information
	for i := 0; i < articleLength && printed < reqLimit; i++ {
		currentArticle := resp.Articles[i]

		// Don't show results older than this request if coming from CACHE
		// Parse publishedAt key in RFC3339 format (if using cache and has a smaller day limit)
		published, _ := time.Parse(time.RFC3339, currentArticle.PublishedAt)

		// Keep only the year, month, day (time will be 00:00:00 UTC)
		publishedDate := time.Date(published.Year(), published.Month(), published.Day(), 0, 0, 0, 0, time.UTC)

		// Skip articles older than requested date
		if publishedDate.Before(minDate) {
			continue
		}

		fmt.Fprintf(&sb, "ENTRY %d: %s\n", printed+1, currentArticle.Title)
		fmt.Fprintf(&sb, "PUBLISH DATE: %s\n", currentArticle.PublishedAt)
		fmt.Fprintf(&sb, "DESCRIPTION: %s\n", currentArticle.Description)
		fmt.Fprintf(&sb, "URL: %s\n", currentArticle.URL)
		fmt.Fprintln(&sb)

		printed++
	}

	// Print message if results were empty
	if printed == 0 {
		fmt.Fprintln(&sb, "\nNo articles matched the request...")
	}

	// Print the final built String
	fmt.Print(sb.String())
}

// Gets the mutex for this query (so similar queries will need to wait until results are uploaded into cache)
func getQueryMutex(req SearchRequest) *sync.Mutex {
	queryMutexesMu.Lock()
	defer queryMutexesMu.Unlock()

	reqMutex, exists := queryMutexes[req.Query]

	// If query didn't exist, create a new Mutex in the map
	if !exists {
		mu := &sync.Mutex{}
		queryMutexes[req.Query] = &RequestMutex{req, mu}
		return mu
	}

	// Get the original cached request
	cachedReq := reqMutex.Request

	// If new request needs more data than that was cached (date is older), create a new Mutex
	if req.Days < cachedReq.Days {
		mu := &sync.Mutex{}
		queryMutexes[req.Query] = &RequestMutex{req, mu}
		return mu
	}

	// Otherwise, reuse existing mutex
	return reqMutex.Mutex
}

func main() {
	// Keep track of how long it takes to run this program
	start := time.Now()

	// Creates database and articles table (if it does not exist already)
	createDatabase()

	// Gets API key from environmental variables on CLI
	key := os.Getenv("NEWSAPI_KEY")

	// Gets the file path for the user prompt
	filePath := os.Getenv("FILE")

	// Gets the number of workers working in the worker pool
	workers := os.Getenv("WORKERS")

	// Makes sure user supplied their API Key
	if key == "" {
		fmt.Println("Please supply API Key to run the program. \nUsing Docker: \n " +
			"docker run --rm -e NEWSAPI_KEY='apiKey' -e FILE='file.txt' -e WORKERS='num' -v news_cache_volume:/app proj1")
		return
	}

	// Remove quotes from CLI input (if it exists)
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

	// Channel used to write safety into the database
	writeChan = make(chan reqNresp)

	// Waitgroup that waits for all entries to be added to the database
	var writeWG sync.WaitGroup

	// Goroutine that makes sure all writes happen in the database
	for range numWorkers {
		writeWG.Go(func() {
			for w := range writeChan {
				saveToDatabase(w.req, w.resp)
			}
		})
	}

	// Create a channel of requests
	requestsChan := make(chan SearchRequest)

	// Waitgroup that waits for all results to be processed before program ends
	var resultsWG sync.WaitGroup

	// Goroutine that collects data from the request channel
	// Worker pool created for parallel API Requests
	for range numWorkers {
		resultsWG.Go(func() {
			// Will wait until data gets put into the requests channel
			for req := range requestsChan {

				// Checks if result is already in the database
				results, inDB := loadFromDatabase(req)
				if inDB {
					printResponse(req, *results, "DATABASE")
				} else {
					// Only requests with the same query (and a smaller or equal date and limit) will be locked
					mu := getQueryMutex(req)

					mu.Lock()
					processRequest(req, key)
					mu.Unlock()
				}
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

			// If it is valid, send to requests channel for further processing
			if success {
				requestsChan <- req
			}
		})
	}

	// Checks if there was an error reading the file
	check(scanner.Err())

	// Waits for all lines to be read
	fileWG.Wait()

	// If there were no errors, close the request channel
	close(requestsChan)

	// Waits for all requests to be processed
	resultsWG.Wait()

	close(writeChan)

	// Waits for all writes to be processed in the database
	writeWG.Wait()

	// Once all lines of the file are read and the results are processed, the program can end
	fmt.Printf("\nProgram took %s to run.\n", time.Since(start))
}
