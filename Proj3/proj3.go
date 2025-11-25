package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

// Set global environment variables
var (
	BASE_URL string = os.Getenv("BASE_URL")
	model    string = os.Getenv("MODEL")

	religion0 string = os.Getenv("LLM_ZERO")
	religion1 string = os.Getenv("LLM_ONE")
	topic     string = os.Getenv("TOPIC")
)

// Message structure that both request and response use
type ChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// Request that is sent to the AI
type ChatRequest struct {
	Model    string        `json:"model"`
	Messages []ChatMessage `json:"messages"`
}

// Response that is received from the AI
type ChatResponse struct {
	Choices []struct {
		Message ChatMessage `json:"message"`
	} `json:"choices"`
}

// Ends program if there was an error
func check(e error) {
	if e != nil {
		log.Fatal(e)
	}
}

func sendRequest(history []ChatMessage) string {

	// Create the request
	reqBody := ChatRequest{
		Model:    model,
		Messages: history,
	}

	// Marshal this data into bytes
	reqBytes, err := json.Marshal(reqBody)
	check(err)

	// Create the HTTP POST Request
	req, err := http.NewRequest("POST", BASE_URL+"chat/completions", bytes.NewBuffer(reqBytes))
	check(err)

	// Sets headers for this request
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer API")

	// Client will do this request
	client := &http.Client{}
	resp, err := client.Do(req)
	check(err)
	defer resp.Body.Close()

	// Get information from request into bytes
	body, _ := io.ReadAll(resp.Body)

	// Unmarshal the bytes into JSON format
	var chatResp ChatResponse
	err = json.Unmarshal(body, &chatResp)
	check(err)

	// Makes sure a response is returned
	if len(chatResp.Choices) == 0 {
		return "(no response)"
	}

	// Print out and return the LLMs response
	respText := chatResp.Choices[0].Message.Content

	// Replace all new lines with just a space
	respText = strings.ReplaceAll(respText, "\n", " ")

	// Return this text
	return respText
}

// MAIN ENTRY INTO THE PROGRAM
func main() {
	// Keep track of how long it takes to run this program
	start := time.Now()

	// Fatal error if environment variables were NOT supplied
	if BASE_URL == "" || model == "" {
		log.Fatal("Missing BASE_URL or MODEL environmental variables.")
	}

	// Make sure topic is valid
	if topic == "" {
		topic = "The War in Gaza"
	}

	// Assign given religions to LLM0 and LLM1
	// If one of these variables were not set, or they were equal, use default religions
	if religion0 == religion1 || religion0 == "" || religion1 == "" {
		religion0 = "Muslim"
		religion1 = "Jewish"
	}

	// How many words per turn (guideline)
	words := 10

	// Set up initial system message for these LLMs
	llm0_message := fmt.Sprintf(
		"You speak from a %s perspective on the topic: %s. "+
			"Be calm, factual, concise, and logical. Present new points each turn, without repeating previous statements.",
		religion0, topic)

	llm1_message := fmt.Sprintf(
		"You speak from a %s perspective on the topic: %s. "+
			"Be calm, factual, concise, and logical. Present new points each turn, without repeating previous statements.",
		religion1, topic)

	// Initialize conversation histories
	histories := map[int][]ChatMessage{
		0: {
			{
				Role:    "system",
				Content: llm0_message,
			},
		},
		1: {
			{
				Role:    "system",
				Content: llm1_message,
			},
		},
	}

	// Store how many turns each LLM has to speak
	turns := 5

	// Start the debate
	for range turns {
		for id := range 2 {

			// For ID 0, the other ID is 1
			// For ID 1, the other ID is 0
			opponentID := 1 - id

			// Start fresh history for this LLM
			history := []ChatMessage{
				{
					Role: "system",

					// System message: this LLM's personality
					Content: histories[id][0].Content,
				},
			}

			// Get the last message from the opponent (if it exists)
			lastOpponentMessage := ""
			if len(histories[opponentID]) > 1 {
				lastOpponentMessage = histories[opponentID][len(histories[opponentID])-1].Content
			}

			userPrompt := ""
			if lastOpponentMessage != "" {
				userPrompt = fmt.Sprintf(
					"Your opponent stated: \"%s\". From your perspective, respond with a counterargument. "+
						"Do not quote your opponent verbatim; focus on your reasoning and beliefs. <=%d words.",
					lastOpponentMessage, words)
			} else {
				userPrompt = fmt.Sprintf("Start the debate from your perspective, <=%d words.", words)
			}

			// Add this prompt to the history
			history = append(history, ChatMessage{
				Role:    "user",
				Content: userPrompt,
			})

			// LOOKING AT THE PROMPTS FOR THIS
			//for i := range len(history) {
			//	fmt.Println(history[i].Content)
			//}
			//fmt.Println()

			// Get LLM to respond to this request
			response := sendRequest(history)

			// Save this turn
			histories[id] = append(histories[id], ChatMessage{
				Role:    "assistant",
				Content: response,
			})

			// Print message from this LLM
			fmt.Printf("\nLLM %d: %s", id, response)
		}
	}

	// Once the conversation is complete and the results are processed, the program can end
	fmt.Printf("\nProgram took %s to run.\n", time.Since(start))
}
