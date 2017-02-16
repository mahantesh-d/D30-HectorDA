package utils

import (
	"encoding/json"
	"github.com/dminGod/D30-HectorDA/logger"
	"strings"
	"fmt"
	"io/ioutil"
	"regexp"
	"os"
)

// IsJSON validates a JSON string
// For example:
//  IsJSON('{"foo" : "bar"}') // Output : true
//  IsJSON('foobar') // Output : false
func IsJSON(input interface{}) bool {

	var output map[string]interface{}
	
	return json.Unmarshal([]byte(input.(string)), &output) == nil	
}

// DecodeJSON converts a JSON string to a map of string and interface
// For example:
//  DecodeJSON('{"foo" : "bar"}') // Output : map[foo : bar]
func DecodeJSON(input interface{}) map[string]interface{} {

        var payload map[string]interface{}

	if !IsJSON(input) {
		return payload
	}
        err := json.Unmarshal([]byte(input.(string)), &payload)

        if err != nil {
                logger.Write("ERROR", err.Error())
        }

        return payload

}

// EncodeJSON converts a map of string and interface to a JSON string
// For example:
//  example := make(map[string]interface{})
//  example["foo"] = "bar"
//  EncodeJSON(example) // Output : {"foo" : "bar"}
func EncodeJSON(input interface{}) string {
	jsonString, err := json.Marshal(input)
	
	if err != nil {
		logger.Write("ERROR", "Error Parsing JSON")
	}

	return string(jsonString)
}

// KeyInMap checks if a given key exists in a map of string and interface
// For example:
//  example := make(map[string]interface{})
//  example["foo"] = "bar"
//  KeyInMap("foo") // Output : true
//  KeyInMap("bar") // Output : false
func KeyInMap(key string, attributes map[string]interface{}) (bool) {

    // iterate over each route
    for k := range attributes {

            if key == k {
                    return true
            }
    }

    return false
}

// FindMap checks if a given key matches a given value and returns the entire map
func FindMap(key string, value interface{}, input map[string]interface{}) map[string]interface{} {


	output := make(map[string]interface{})
	// iterate over each map
	for _,v := range input {
		meta := v.(map[string]interface{})
		if meta[key] == value {
			output = meta
		}
	}

	return output

}

// ReadFile returns the contents of the file
// For example :
//  ReadFile("/tmp/foo.txt") // Output : Contents of the file /tmp/foo.txt"
func ReadFile(path string) string {
	
	raw, err := ioutil.ReadFile(path)
 	if err != nil {
        	fmt.Println(err.Error())
 	}	

	return string(raw)
}

// ParseFilter is used to convert an LDAP type query filter to a map of string and interface
// For example:
//  ParseFilter("(&(fooKey=fooValue)(barKey=barValue))") // Output : map[fooKey: fooValue  barKey: barValue]
func ParseFilter(input string) map[string]string {

	output := make(map[string]string)

	pattern := `(^\(*\&?\(*)(.*)(\)?\)$)`

	if !RegexMatch(input, pattern) {
		return output
	}

	var validID = regexp.MustCompile(pattern)
	input = (validID.FindStringSubmatch(input))[2]

	/*if len(input) == 0 {
		return output
	}

	input = input[1:]
	input = strings.Trim(input,")")	
	if string(input[0]) == "&" {
		input = input[1:]
	}*/
	
	filters := strings.Split(input,")(")

	for _,v := range filters {

		v = strings.Replace(v,"(","",1)
		v = strings.Replace(v,")","",1)

		keyval := strings.Split(v,"=")
		output[keyval[0]] = keyval[1]
	}

	return output

}

// RegexMatch is used to match an input string with a regex pattern
// For example :
//  RegexMatch("abc",`[a-z]+`) // Output : true
//  RegexMatch("123", `[a-z]+`) // Output : false
func RegexMatch(input string,pattern string) bool {

	var validID = regexp.MustCompile(pattern)

	return validID.MatchString(input)
}

// Exit is used to Exit the application with the provided exit code
// For example:
//  Exit(1) // this will exit the application with exit code 1
func Exit(code int) {

	os.Exit(code)

}
