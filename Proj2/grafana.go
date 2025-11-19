package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"
)

var (
	// Grafana connection details (MAKE SURE YOU DONT RESET THE PASSWORD IF IT ASKS, JUST SKIP IT)
	grafanaURL  = "http://grafana:3000"
	grafanaUser = "admin"
	grafanaPass = "admin"

	// The metric topics correspond to Prometheus metric names exposed by proj2
	metricTopics = []string{"temperature", "feelslike", "humidity", "wind_speed", "wind_degree", "cloud_percent"}

	// Display-friendly names that match order found in metricTopics slice
	namedTopics = []string{"Temperature (°F)", "Feels Like (°F)", "Humidity (%)", "Wind Speed (MPH)", "Wind Degree (°)", "Cloud Coverage (%)"}
)

// Waits until Grafana responds on /api/health
func waitForGrafana(timeout time.Duration) error {
	client := &http.Client{}
	start := time.Now()

	for {
		// Build an authenticated HTTP GET request to Grafana's /api/health endpoint.
		req, _ := http.NewRequest("GET", grafanaURL+"/api/health", nil)
		req.SetBasicAuth(grafanaUser, grafanaPass)
		resp, err := client.Do(req)

		// If there were no error with the HTTP GET request
		if err == nil {
			resp.Body.Close()

			// Grafana is up if status is 200 or 401 (login required)
			if resp.StatusCode == 200 || resp.StatusCode == 401 {
				fmt.Println("Grafana is up and ready!")
				return nil
			}
		}

		// Program will shut down if it doesn't start in "timeout" duration
		if time.Since(start) > timeout {
			return fmt.Errorf("grafana did not become ready within %s", timeout)
		}

		// Retries every 2 seconds
		fmt.Println("Waiting for Grafana to start...")
		time.Sleep(2 * time.Second)
	}
}

// Ensures Grafana has Prometheus configured as a data source
func setupPrometheusDataSource() {
	client := &http.Client{}

	// Define the Prometheus data source payload
	// The URL is the Prometheus container URL
	dataSource := map[string]any{
		"name":      "Prometheus",
		"type":      "prometheus",
		"url":       "http://prometheus:9090",
		"access":    "proxy",
		"isDefault": true,
	}

	// Marshal the dataSource map into JSON for the HTTP request body
	payload, _ := json.Marshal(dataSource)

	// POST /api/datasources
	req, _ := http.NewRequest("POST", grafanaURL+"/api/datasources", bytes.NewBuffer(payload))
	req.SetBasicAuth(grafanaUser, grafanaPass)
	req.Header.Set("Content-Type", "application/json")

	// Sends the request
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("Error creating Prometheus data source:", err)
		return
	}
	defer resp.Body.Close()

	// If the request status is successful, that means Prometheus was configured successfully
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		fmt.Println("Prometheus data source configured successfully!")
	}
}

// Creates dashboards per ZIP code with separate graphs per metric
// Ensures Prometheus does not sum across instances by using only location and date labels
func setupGrafana() {

	// Ensure Prometheus data source exists
	setupPrometheusDataSource()

	// Returns all unique ZIP codes (given by the metrics file)
	zipCodes := getAllZipCodes()

	// Each ZIP code gets its own dashboard
	for _, zip := range zipCodes {

		// Generate a unique dashboard UID based on ZIP
		// Used so if dashboard is created again, will just update and not create a whole new dashboard
		uid := fmt.Sprintf("weather-%s", zip)

		// Creates the dashboard
		dashboard := createDashboardForZip(zip, uid)

		// Adds dashboard to Grafana
		pushDashboard(dashboard)
	}
}

// Builds a dashboard JSON object for a single ZIP code with a UID
func createDashboardForZip(zip, uid string) map[string]any {

	// Panels will hold all panel definitions (bar charts for each topic + alerts) for this dashboard
	panels := []map[string]any{}
	panelID := 1
	yPos := 0

	// Create graphs for each topic
	for i, topic := range metricTopics {

		// Grafana JSON Panel code that gets manipulated to add the data that is needed
		panel := map[string]any{
			"type":  "graph",
			"title": namedTopics[i],
			"id":    panelID,
			"gridPos": map[string]any{
				"h": 8,
				"w": 24,
				"x": 0,
				"y": yPos,
			},
			// The targets will get the data we need
			"targets": []map[string]any{
				{
					// Get last value over 15s window for this ZIP and metric
					"expr":         fmt.Sprintf("last_over_time(%s{location=\"%s\"}[15s])", topic, zip),
					"legendFormat": "{{date}}",
					"refId":        "A",
				},
			},
			"xaxis": map[string]any{
				"mode": "series",
				"name": "date",
			},
			"yaxis": map[string]any{
				"format": "short",
			},
		}

		// Append this metric panel to the list of panels
		panels = append(panels, panel)

		// Increment IDs and vertical position for next panel
		panelID++
		yPos += 8
	}

	// Add Stat panels for alerts
	// The key is the name of the alert, the value is the prometheus gauge name that will be used for data
	alerts := []struct {
		Name  string
		Gauge string
	}{
		{"High Temperature", "alert_temperature_high"},
		{"Low Temperature", "alert_temperature_low"},
		{"High Humidity", "alert_humidity_high"},
		{"Low Humidity", "alert_humidity_low"},
		{"High Wind Speed", "alert_wind_high"},
	}

	// Specifications for these new panels
	alertPanelWidth := 4.9
	alertPanelHeight := 4
	alertX := 0.0

	// These panels display alerts for high/low thresholds or extreme conditions
	// Each panel shows ALL GOOD! if no alert is active, or the date of the alert
	for _, alert := range alerts {
		panel := map[string]any{
			// Using a stat panel for single numeric/text value
			"type":  "stat",
			"title": alert.Name,
			"id":    panelID,
			"gridPos": map[string]any{
				"h": alertPanelHeight,
				"w": alertPanelWidth,
				"x": alertX,
				"y": yPos,
			},
			"targets": []map[string]any{
				{
					// Prometheus query: gauge == 1 means alert is active
					"expr":         fmt.Sprintf("%s{location=\"%s\"} == 1", alert.Gauge, zip),
					"refId":        "A",
					"legendFormat": "{{date}}",
				},
			},
			"fieldConfig": map[string]any{
				"defaults": map[string]any{
					// The Color of the alert is based on thresholds
					"color": map[string]any{"mode": "thresholds"},
					"unit":  "none",
					"mappings": []any{
						map[string]any{
							"type": "value_to_text",
							"options": map[string]any{
								// If it's 0, that means there was no alert
								"0": "",
								// If it's 1, display the date of that alert
								"1": "{{date}}",
							},
						},
					},
					"thresholds": map[string]any{
						"mode": "absolute",
						"steps": []map[string]any{
							// No alert will be green
							{"value": 0, "color": "green"},
							// Active alert will be red
							{"value": 0.5, "color": "red"},
						},
					},
					// Default text if no value exists
					"noValue": "ALL GOOD!",
				},
			},
			// Ignore null/0 values
			"options": map[string]any{
				"reduceOptions": map[string]any{
					"calcs": []string{"lastNotNull"},
				},
				// Show value and metric name
				"textMode": "value_and_name",
			},
		}

		// Add this alert panel to dashboard
		panels = append(panels, panel)
		panelID++

		// Move panel to next line if too many panels are already in that line
		// This currently isn't used, but would be if there were no more alerts, or if the panels were larger
		alertX += alertPanelWidth
		if alertX >= 24 {
			alertX = 0
			yPos += alertPanelHeight
		}
	}

	// Assemble the dashboard using these panels
	dashboard := map[string]any{
		"dashboard": map[string]any{
			// Unique identifier for updates
			"uid":           uid,
			"title":         fmt.Sprintf("Weather Dashboard - ZIP %s", zip),
			"panels":        panels,
			"time":          map[string]string{"from": "now-1s", "to": "now"},
			"schemaVersion": 36,
			"version":       0,
		},
		"refresh": "1s",
		// Ensures existing dashboard is updated
		"overwrite": true,
	}

	return dashboard
}

// Posts the dashboard JSON to Grafana API
func pushDashboard(dashboard map[string]any) {

	// Marshal the Go map into JSON bytes to send over HTTP
	data, err := json.Marshal(dashboard)
	if err != nil {
		fmt.Println("Error marshaling dashboard:", err)
		return
	}

	// Create a POST request to Grafana's /api/dashboards/db endpoint
	// This endpoint handles both creating new dashboards and updating existing dashboards
	req, err := http.NewRequest("POST", grafanaURL+"/api/dashboards/db", bytes.NewBuffer(data))
	if err != nil {
		fmt.Println("Error creating request:", err)
		return
	}
	// Set basic authentication for Grafana API access
	req.SetBasicAuth(grafanaUser, grafanaPass)

	// Set the content type header to application/json because the API expects JSON
	req.Header.Set("Content-Type", "application/json")

	// Use an HTTP client to send the request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("Error sending request to Grafana:", err)
		return
	}
	defer resp.Body.Close()

	// Extract the dashboard title from the JSON object for logging
	title := dashboard["dashboard"].(map[string]any)["title"]

	// Log results
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		fmt.Printf("Dashboard for ZIP %s created/updated successfully\n", title)
	} else {
		fmt.Printf("Failed to create/update dashboard for ZIP %s, status: %d\n", title, resp.StatusCode)
	}
}

// Reads unique ZIP codes from JSONL metrics file
func getAllZipCodes() []string {

	// Open the metrics file in read-only mode
	file, err := os.Open(metricsFilePath)
	if err != nil {
		fmt.Println("Error opening metrics file:", err)
		return nil
	}
	defer file.Close()

	// Use a map as a set to store unique ZIP codes
	zipSet := make(map[string]struct{})

	// Allows reading the file line by line (each line is a JSON object)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var msg WeatherMessage

		// Unmarshal each line into a WeatherMessage struct
		if err := json.Unmarshal(scanner.Bytes(), &msg); err == nil {
			zipSet[msg.Zip] = struct{}{}
		}
	}

	// Check for scanning errors
	if err := scanner.Err(); err != nil {
		fmt.Println("Error scanning metrics file:", err)
	}

	// Convert the set of ZIP codes into a slice for easier iteration
	zips := make([]string, 0, len(zipSet))
	for z := range zipSet {
		zips = append(zips, z)
	}
	return zips
}
