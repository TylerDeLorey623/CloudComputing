package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

// Set global environment variables
var (
	// Using the user's provided environment variable names
	BASE_URL string = os.Getenv("BASE_URL")
	model    string = os.Getenv("MODEL")

	// Debater 1 (ID 1)
	religion1 string = os.Getenv("LLM_ONE_RELIGION")
	// Debater 2 (ID 2)
	religion2 string = os.Getenv("LLM_TWO_RELIGION")

	topic        string = os.Getenv("TOPIC")
	turnsStr     string = os.Getenv("TURNS")
	sentencesStr string = os.Getenv("SENTENCES")

	// Global combined history of what the debaters say for the judge and debaters to read
	transcript []string

	// Output file
	file *os.File
)

// Declare constants
const (
	// Debaters will only read the last MAX_TRANSCRIPT_LENGTH chats in the transcript to form their response
	// The Judge will still read the entire transcript (unless too many tokens were passed in)
	MAX_TRANSCRIPT_LENGTH = 4

	// Constants for max sentences per turn and turn count
	MAX_SENTENCES = 5
	MAX_TURNS     = 8

	// Constants for DEFAULT sentences per turn and turn count
	DEFAULT_SENTENCES = 3
	DEFAULT_TURNS     = 5
)

// Message structure that both request and response use
type ChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// Chat Request Format
type ChatRequest struct {
	Model       string        `json:"model"`
	Messages    []ChatMessage `json:"messages"`
	Temperature float64       `json:"temperature"`
	TopP        float64       `json:"top_p"`
	Stop        []string      `json:"stop"`
}

// Chat Response Format
type ChatResponse struct {
	Choices []struct {
		Message ChatMessage `json:"message"`
	} `json:"choices"`
	Usage struct {
		PromptTokens     int `json:"prompt_tokens"`
		CompletionTokens int `json:"completion_tokens"`
		TotalTokens      int `json:"total_tokens"`
	} `json:"usage"`
}

// Ends program if there was an error
func check(e error) {
	if e != nil {
		log.Fatal(e)
	}
}

// Wrapping text for writing to output file
func writeToFile(text string) {

	// The maximum length is ~150 characters
	maxLength := 150

	// Split text by existing newlines to preserve them
	paragraphs := strings.Split(text, "\n")

	// Stores all lines for the file
	var lines []string

	// Iterate by each newline
	for _, paragraph := range paragraphs {
		paragraph = strings.TrimSpace(paragraph)

		// Preserve empty lines
		if paragraph == "" {
			lines = append(lines, "")
			continue
		}

		// Split paragraph into words
		words := strings.Fields(paragraph)

		// Keep track of the content on the current line
		var currentLine string

		// Iterate through each word
		for _, word := range words {

			// If adding this word exceeds maxLength, start a new line
			if len(currentLine)+len(word)+1 > maxLength {
				lines = append(lines, strings.TrimSpace(currentLine))
				currentLine = word + " "
			} else {
				currentLine += word + " "
			}
		}

		// Append any remaining text in currentLine
		if currentLine != "" {
			lines = append(lines, strings.TrimSpace(currentLine))
		}
	}

	// Join the lines with a newline
	result := strings.Join(lines, "\n")

	// Write this to the file
	_, err := file.WriteString(result + "\n")
	check(err)
}

// Prints out response word by word
func printResponse(text string, flowed bool) {

	// Write the text to the file
	writeToFile(text)

	// Just print the block of text if it doesn't need to be flowed
	if !flowed {
		fmt.Println(text)
		return
	}

	// Split response into words
	words := strings.SplitSeq(text, " ")

	// If made it here, the text needs to flow word by word
	// Print each word one by one
	for word := range words {
		fmt.Printf("%s ", word)
		time.Sleep(50 * time.Millisecond)
	}

	// After the text is flowed out and fully printed, add a newline
	fmt.Println()
}

// Sends a HTTP POST Request and receives a response from the LLM
func sendRequest(history []ChatMessage, removeNewLines bool) (string, bool) {

	// Create the request
	reqBody := ChatRequest{
		Model:       model,
		Messages:    history,
		Temperature: 0.7,
		TopP:        0.9,
	}

	// Marshal this data into bytes
	reqBytes, err := json.Marshal(reqBody)
	check(err)

	// Create the HTTP POST Request
	req, err := http.NewRequest("POST", BASE_URL+"chat/completions", bytes.NewBuffer(reqBytes))
	check(err)

	// Sets header for this request
	req.Header.Set("Content-Type", "application/json")

	// Client will do this request
	client := &http.Client{}
	resp, err := client.Do(req)
	check(err)
	defer resp.Body.Close()

	// Check HTTP Status
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)

		// If it was a 404 error, you must download the appropriate model
		if resp.StatusCode == http.StatusNotFound {
			log.Fatalf("Download the appropriate model by doing \"docker-compose up llm\": \n%s", body)
		}

		log.Printf("Llama API call failed! Status: %d, Body: %s\n", resp.StatusCode, body)
		log.Println("Retrying with smaller transcript...")
		log.Println()
		return "err", true
	}

	// Get information from request into bytes
	body, _ := io.ReadAll(resp.Body)

	// Unmarshal the bytes into JSON format
	var chatResp ChatResponse
	err = json.Unmarshal(body, &chatResp)
	check(err)

	// Makes sure a response is returned (was an error)
	if len(chatResp.Choices) == 0 {
		return "(no response)", true
	}

	// Return the LLM's response
	respText := chatResp.Choices[0].Message.Content

	// Replace all new lines with just a space if it needs to be formatted
	if removeNewLines {
		respText = strings.ReplaceAll(respText, "\n", " ")
	}

	// Return this text and that it was successful (no error)
	return respText, false
}

// judgeDebate runs the LLM0 (Judge) on the full debate history
func judgeDebate() {
	printResponse("\n--- JUDGMENT PHASE: LLM0 (The Judge) ---", false)

	// Improved system prompt for the judge
	judgeSysPrompt := fmt.Sprintf(
		"You are an impartial, non-biased, non-religious JUDGE. Read the debate between a %s follower (DEBATER_1) "+
			"and a %s follower (DEBATER_2) on the topic: %s.",
		religion1, religion2, topic,
	)

	// Send request to LLM
	sendARequest := true
	judgeResponse := ""

	// Transcript may need to be shortened, so store it in another variable
	currentTranscript := transcript

	// Send the request until there is not an error
	for sendARequest {

		// Convert full debate into a string
		fullDebate := strings.Join(currentTranscript, "\n")

		// Prepare conversation for the judge
		judgeHistory := []ChatMessage{
			{Role: "system", Content: judgeSysPrompt},
			{Role: "user", Content: "Here is the full debate:\n" + fullDebate},
			{Role: "user", Content: "YOU MUST CHOOSE THE WINNER AND EXPLAIN WHY."},
		}

		// Send the request
		resp, err := sendRequest(judgeHistory, false)

		// If there was no error with the request, continue on
		if !err {
			judgeResponse = resp
			sendARequest = false
		} else if len(currentTranscript) > MAX_TRANSCRIPT_LENGTH {
			// If there was an error (most likely max context size), resend the request with a shorter debate transcript
			currentTranscript = currentTranscript[len(currentTranscript)-MAX_TRANSCRIPT_LENGTH:]
		} else {
			// If the token limit is too large AND the transcription size is too small, SOMETHING WENT WRONG (THIS SHOULD NOT HAPPEN EVER)
			judgeResponse = "Something went wrong when generating response. Please try again later."
			sendARequest = false
		}
	}

	// Print its response
	printResponse(judgeResponse, true)
}

// MAIN ENTRY INTO THE PROGRAM
func main() {

	// Keep track of how long it takes to run this program
	start := time.Now()

	// Fatal error if environment variables were NOT supplied
	if BASE_URL == "" || model == "" {
		log.Fatal("Missing BASE_URL or MODEL environmental variables.")
	}

	// Create text file that will be the output of the debate
	var err error
	file, err = os.Create("/app/output/output.txt")
	check(err)
	defer file.Close()

	// Make sure topic is valid
	if topic == "" {
		topic = "The War in Gaza"
	}

	// Assign given religions to LLM1 and LLM2
	// If one of these variables were not set, or they were equal, use default religions
	if religion1 == religion2 || religion1 == "" || religion2 == "" {
		religion1 = "Muslim"
		religion2 = "Jewish"
	}

	// Store how many turns each LLM has to speak (default is 5)
	turns, err := strconv.Atoi(turnsStr)
	if err != nil || turnsStr == "" || turns <= 0 {
		fmt.Printf("Warning: Invalid turn amount: '%s'. Setting it to %d.\n", turnsStr, DEFAULT_TURNS)
		turns = DEFAULT_TURNS
	}
	// The maximum turn count is 8
	if turns > MAX_TURNS {
		fmt.Printf("Warning: Too many turns for each LLM: %d. Setting turn count to %d.\n", turns, MAX_TURNS)
		turns = MAX_TURNS
	}

	// Store how many sentences each LLM can to speak per turn (default is 3)
	sentences, err := strconv.Atoi(sentencesStr)
	if err != nil || sentencesStr == "" || sentences <= 0 {
		fmt.Printf("Warning: Invalid sentence amount: '%s'. Setting it to %d.\n", sentencesStr, DEFAULT_SENTENCES)
		sentences = DEFAULT_SENTENCES
	}
	// The maximum sentence count is 5
	if sentences > MAX_SENTENCES {
		fmt.Printf("Warning: Too many sentences for each LLM: %d. Setting sentence count to %d.\n", sentences, MAX_SENTENCES)
		sentences = MAX_SENTENCES
	}

	// Will store system messages for each LLM
	sysMessages := map[int]ChatMessage{}

	// Map religion to each LLM
	religions := map[int]string{
		1: religion1,
		2: religion2,
	}

	// Set up system messages for both LLMs
	for i := range 2 {
		// Iteration 0: ID 1
		// Iteration 1: ID 2
		id := i + 1

		// Set up initial system message for debater
		sys := fmt.Sprintf(`You are DEBATER_%d, a committed follower and articulate representative of the %s worldview. 

							Your goal is to argue your faith's perspective on the Topic: %s. Be respectful, persuasive, and concise.
							You must argue ONLY from within the authentic teachings, values, texts, and reasoning of the %s tradition. 
							Do not misrepresent or alter the faith. Do not invent scripture. 
							Do not use the opponent's scripture or theology as authoritative.
							
							Every response you give **MUST** be %d sentences.`,
			id, religions[id], topic, religions[id], sentences)

		// Add system message to the map
		sysMessages[id] = ChatMessage{
			Role:    "system",
			Content: sys,
		}
	}

	// TEXT BOX OF INFORMATION ABOUT THE DEBATE
	printResponse("", false)
	printResponse("----------------------------------------", false)
	printResponse(fmt.Sprintf("TOPIC: %s", topic), false)
	printResponse(fmt.Sprintf("DEBATER_1: %s", religion1), false)
	printResponse(fmt.Sprintf("DEBATER_2: %s", religion2), false)
	printResponse(fmt.Sprintf("TURNS PER DEBATER: %d", turns), false)
	printResponse(fmt.Sprintf("SENTENCES PER TURN: %d", sentences), false)
	printResponse("----------------------------------------", false)

	// Start the debate
	for turn := range turns * 2 {
		// id 1 (Debater 1) speaks on even turns (0, 2, 4...)
		// id 2 (Debater 2) speaks on odd turns (1, 3, 5...)
		id := (turn % 2) + 1

		// Determine the turn count (each LLM speaks in one turn)
		turnCount := int(math.Floor(float64(turn/2)) + 1)

		// Copy system message for this debater
		history := append([]ChatMessage{}, sysMessages[id])

		// Add new prompt
		if len(transcript) > 0 {
			history = append(history, ChatMessage{
				Role: "user",
				Content: fmt.Sprintf(
					`Respond to your opponent strictly from the perspective of the %s worldview. 
					Do NOT cite the opponent's scripture or use their theology as authority. 
					Use only arguments, principles, and reasoning that someone fully committed to the %s tradition would genuinely use.
					Do not quote scripture, doctrines, catechisms, hadith, or commentaries unless you are absolutely certain they are real.
					If unsure, paraphrase the tradition's general viewpoint rather than citing texts.`,
					religions[id], religions[id],
				),
			})

			// Get the debate
			currentFullDebate := strings.Join(transcript, "\n")

			// Will only provide the last FOUR entries in the debate so tokens will not overflow
			if len(transcript) > MAX_TRANSCRIPT_LENGTH {
				cutTranscript := transcript[len(transcript)-MAX_TRANSCRIPT_LENGTH:]
				currentFullDebate = strings.Join(cutTranscript, "\n")
			}

			// Let the LLM know the debate history up until this point
			history = append(history, ChatMessage{
				Role: "user",
				Content: fmt.Sprintf("Provide a new counter-argument on the topic %s with your own reasoning in %d sentence(s) "+
					"(do not mention this sentence count, but you MUST adhere to the rule). \n\nRemember, you are DEBATER_%d. "+
					"Here is the full debate up until this point: \n%s",
					topic, sentences, id, currentFullDebate),
			})
		} else {
			// If there is no transcript yet, this means the LLM will need to start the debate
			history = append(history, ChatMessage{
				Role: "user",
				Content: fmt.Sprintf(`Begin the debate. Present your worldview's core position on the topic %s in %d sentence(s) 
									(do not mention this sentence count, but you MUST adhere to the rule). 
									Speak exactly as a committed follower and scholar of the %s tradition would. 
									Use real teachings, real values, and real reasoning from that worldview.`, topic, sentences, religions[id]),
			})
		}

		// LLM responds
		response, _ := sendRequest(history, true)

		// Print real-time output
		printResponse(fmt.Sprintf("\n-- DEBATER_%d: %s (TURN %d) --", id, religions[id], turnCount), false)
		printResponse(response, true)

		// Add to the response to the transcript
		transcript = append(transcript, fmt.Sprintf("DEBATER_%d: %s\n\n", id, response))
	}

	// The Judge will judge this debate since it is now finished
	judgeDebate()

	// Once the Judge has given their response, the program can end
	fmt.Printf("\nProgram took %s to run.\n", time.Since(start))
}
