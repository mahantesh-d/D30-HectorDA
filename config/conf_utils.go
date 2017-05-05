package config

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
)

// DecodeJSON converts a JSON string to a map of string and interface
func decodeJSON(input interface{}) map[string]interface{} {

	var payload map[string]interface{}

	if !isJSON(input) {
		return payload
	}
	err := json.Unmarshal([]byte(input.(string)), &payload)

	if err != nil {
		fmt.Println("Error when decoding JSON", err)
	}

	return payload
}

func isJSON(input interface{}) bool {

	var output map[string]interface{}
	return json.Unmarshal([]byte(input.(string)), &output) == nil
}

func readFile(path string) string {

	raw, err := ioutil.ReadFile(path)

	if err != nil {
		fmt.Println("Error when reading file", path,  err, raw)
	}

	return string(raw)
}

