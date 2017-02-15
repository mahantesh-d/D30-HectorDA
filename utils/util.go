package utils

import (
	"encoding/json"
	"github.com/dminGod/D30-HectorDA/logger"
	"strings"
	"strconv"
	"fmt"
	"io/ioutil"
	"regexp"
	"os"
)

func IsJSON(input interface{}) bool {

	var output map[string]interface{}
	
	return json.Unmarshal([]byte(input.(string)), &output) == nil	
}


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


func EncodeJSON(input interface{}) string {
	jsonString, err := json.Marshal(input)
	
	if err != nil {
		logger.Write("ERROR", "Error Parsing JSON")
	}

	return string(jsonString)
}


func PrepareInsert(tableName string, attr map[string] interface{}) string {

        query := "INSERT INTO " + tableName

        name := " ( "
        value := " ( "
        for i,v := range attr {

                name += (i + ",")

		switch c := v.(type) {
         		case string:
				val := ""
                		if strings.HasSuffix(i, "_pk") && strings.ToLower(v.(string)) == "now()" {
					val = v.(string)
				} else {
					val = "'" + v.(string) + "'"
                 		}
				value += val
         		case int32, int64:
                 		val := (strconv.Itoa(v.(int)))
                 		value += val
         		case float32,float64:
                 		val := strconv.FormatFloat(v.(float64),'f',-1,64)
                 		value += val
         		default:
                 		_ = c
 		}

                value += ","
        }

        name = strings.Trim(name,",")
        value = strings.Trim(value,",")

        query += name + " ) VALUES " + value + " ) "


        return query
}


func KeyInMap(key string, attributes map[string]interface{}) (bool) {

    // iterate over each route
    for k,_ := range attributes {

            if key == k {
                    return true
            }
    }

    return false
}

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

func ReadFile(path string) string {
	
	raw, err := ioutil.ReadFile(path)
 	if err != nil {
        	fmt.Println(err.Error())
 	}	

	return string(raw)
}

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

func RegexMatch(input string,pattern string) bool {

	var validID = regexp.MustCompile(pattern)

	return validID.MatchString(input)
}

func Exit(code int) {

	os.Exit(code)

}
