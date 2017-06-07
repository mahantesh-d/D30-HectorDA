package utils

import (
	"encoding/json"
	"github.com/dminGod/D30-HectorDA/logger"
	"io/ioutil"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"bytes"
	"fmt"

	"github.com/dminGod/D30-HectorDA/config"
	"math/rand"
	"strconv"
	"encoding/base64"

)

// IsJSON validates a JSON string
func IsJSON(input interface{}) bool {

	var output map[string]interface{}

	return json.Unmarshal([]byte(input.(string)), &output) == nil
}

// DecodeJSON converts a JSON string to a map of string and interface
func DecodeJSON(input interface{}) map[string]interface{} {

	var payload map[string]interface{}

	if !IsJSON(input) {
		return payload
	}
	err := json.Unmarshal([]byte(input.(string)), &payload)
	HandleError(err)

	return payload
}

// EncodeJSON converts a map of string and interface to a JSON string
func EncodeJSON(input interface{}) string {
	jsonString, err := json.Marshal(input)
	HandleError(err)	

	return string(jsonString)
}

// KeyInMap checks if a given key exists in a map of string and interface
func KeyInMap(key string, attributes map[string]interface{}) bool {

	// iterate over each route
	for k := range attributes {

		if key == k {
			return true
		}
	}

	return false
}


// KeyInMap checks if a given key exists in a map of string and interface
func ValueInMapSelect(key string, attributes map[string]interface{}) bool {

	// iterate over each route
	for _, v := range attributes {

		vv := v.(map[string]interface{})

		for kkk, vvv := range vv {

			if kkk == "name" && vvv == key {

				return true
			}
		}
	}

	return false
}

// KeyInMap checks if a given key exists in a map of string and interface
func GetSelectMap(key string, attributes map[string]interface{}, v string) map[string]interface{} {

	retVal := map[string]interface{}{}

	// iterate over each route
	for _, v := range attributes {

		vv := v.(map[string]interface{})

		for kkk, vvv := range vv {

			if kkk == "name" && vvv == key {

				retVal = vv
			}
		}
	}


	// Bhagawan janta hai yeh aisa kyun kar raha hai.
	// Tumhe samjhe tao muje batana.
	// Bahut deep aur weird behaviour hai.

	if _, ok := retVal["value"].([]string); ok && len(retVal["value"].([]string)) > 0 {

		retVal["value"] = append(retVal["value"].([]string), v)
	} else {
		retVal["value"] = make([]string, 20)
		retVal["value"].([]string)[0] = v
	}

	return retVal
}

// KeyInMap checks if a given key exists in a map of string and interface
func GetFieldByName(key string, attributes map[string]interface{}) map[string]interface{} {

	retVal := map[string]interface{}{}

	// iterate over each route
	for _, v := range attributes {

		vv := v.(map[string]interface{})

		for kkk, vvv := range vv {

			if kkk == "name" && vvv == key {

				retVal = vv
			}
		}
	}

	return retVal
}

func matchFieldTag(tagsArr []interface{}, match string) bool {

	retVal := false

	for _, v := range tagsArr {

		if _, ok := v.(string); ok {
			if v == match {

				retVal = true
			}
		}
	}

	return retVal
}




// FindMap checks if a given key matches a given value and returns the entire map
func FindMapSelect(key string, table_name interface{}, json_records map[string]interface{}, filter_fields []string) map[string]interface{} {

	output := make(map[string]interface{})

	// iterate over each map
	for _, v := range json_records {

		meta := v.(map[string]interface{})

		if meta[key] == table_name {

			// In the matched table save the fields array as a tmpVar

			curFields := meta["fields"].(map[string]interface{})
			sendFields := make(map[string]interface{})
			limitedFields := make(map[string]interface{})

			for kk, vv := range curFields {

				curField := vv.(map[string]interface{})


				if _, ok := curField["tags"].([]interface{}); ok {


					if matchFieldTag(curField["tags"].([]interface{}), "internal_field") == false {

						sendFields[  kk  ] = curField
					}
				} else {

					sendFields[  kk  ] = curField
				}
			}



			for kk, vv := range sendFields {

				curField := vv.(map[string]interface{})

				for _, f_field := range filter_fields {

					if f_field == curField["name"] {

						limitedFields[  kk  ] = curField
					}
				}
			}

			if len(limitedFields) > 0 {

				meta["fields"] = limitedFields
			} else {

				meta["fields"] = sendFields
			}

			output = meta
		}
	}

	return output

}





// FindMap checks if a given key matches a given value and returns the entire map
func FindMap(key string, value interface{}, input map[string]interface{}) map[string]interface{} {

	output := make(map[string]interface{})

//	fmt.Println("Input passed to me is ....", input)

//	fmt.Println("Iterating over all the maps in the util.go")

	// iterate over each map
	for _, v := range input {

		meta := v.(map[string]interface{})

		if meta[key] == value {

//			fmt.Println("we hve a match, ", meta)
			output = meta
		}
	}

	return output

}

// ReadFile returns the contents of the file
func ReadFile(path string) string {

	raw, err := ioutil.ReadFile(path)
	HandleError(err)
	return string(raw)
}


func ParseSelectFields(passedVal string) []string {

	retStrs := strings.Split(passedVal, ",")

	fmt.Println("parse selecfeilds",retStrs)
	if len(retStrs) == 0 {

		return []string{}
	}

	return retStrs
}




// ParseFilter is used to convert an LDAP type query filter to a map of string and interface
func ParseFilter(input string) (map[string]interface{}, bool) {

	output := make(map[string]interface{})
	isOrCondition := false

	pattern := `(^\(*\&?\|?\(*)(.*)(\)?\)$)`

	if !RegexMatch(input, pattern) {

		return output, false
	}

	var validID = regexp.MustCompile(pattern)

	inputArr := (validID.FindStringSubmatch(input))

	if len(inputArr) > 2 {

		isOrCondition = strings.Contains(inputArr[1], "|")

		input = inputArr[2]

	if len(input) == 0 {
		return output, false
	}


	filters := strings.Split(input, ")(")

	// Passed Filters
	for _, v := range filters {

		if strings.Count(v, "=") == 0 {

			continue;
		}
		// Clean
		v = strings.Replace(v, "(", "", 1)
		v = strings.Replace(v, ")", "", 1)

		keyval := strings.Split(v, "=")
		rand_num := strconv.Itoa(rand.Intn(9000) + 1);

		// Key is throwaway, not using slice cause it was causing
		// wierd stuff to happen later..
		k := map[string]string{
			"key" : keyval[0],
			"value" : keyval[1],
		}

		output[ keyval[0] + rand_num ] = k
	}

	}

	return output, isOrCondition
}


// Given a fieldname and table name return the type of data type the column is.
func GetColumnDetails( table_name string, field_name string) map[string]interface{} {

	jsonData := config.Metadata_insert()
	var retType map[string]interface{}

	for _, v := range jsonData {

		fields := v.(map[string]interface{})

		if( fields["table"] == table_name) {

			// Looping over the fields block
			for _, vv := range fields["fields"].(map[string]interface{}) {

				curFieldBlock := vv.(map[string]interface{})

				if(curFieldBlock["name"] == field_name) {

					retType = curFieldBlock

				}

			}

		}
	}


	return retType
}

// Given a qualifier for JSON loop through the fields and get the corresponding field level data

func GetColumnDetailsGeneric( field_ouside_tag string, field_outside_value string, field_name string) map[string]interface{} {

	jsonData := config.Metadata_insert()
	var retType map[string]interface{}

	for _, v := range jsonData {

		fields := v.(map[string]interface{})

		if( fields[field_ouside_tag] == field_outside_value) {

			// Looping over the fields block
			for _, vv := range fields["fields"].(map[string]interface{}) {

				curFieldBlock := vv.(map[string]interface{})

				if(curFieldBlock["name"] == field_name) {

					retType = curFieldBlock
				}
			}
		}
	}


	return retType
}





// Given a fieldname and table name return the type of data type the column is.
func GetColumnType( table_name string, field_name string) string {

	jsonData := config.Metadata_insert()
	retType := ""

	for _, v := range jsonData {

		fields := v.(map[string]interface{})

		if( fields["table"] == table_name) {

			// Looping over the fields block
			for _, vv := range fields["fields"].(map[string]interface{}) {

				curFieldBlock := vv.(map[string]interface{})

				if(curFieldBlock["name"] == field_name) {

					retType = curFieldBlock["type"].(string)
 						}
			}
		}
	}

	return retType
}


// RegexMatch is used to match an input string with a regex pattern
func RegexMatch(input string, pattern string) bool {

	var validID = regexp.MustCompile(pattern)

	return validID.MatchString(input)
}

// Exit is used to Exit the application with the provided exit code
func Exit(code int) {

	os.Exit(code)

}

func ExecuteCommand(command string, args ...string) string {
	out, _ := exec.Command(command, args...).Output()
	output := string(out)
	output = strings.Trim(output, "\r")
	output = strings.Trim(output, "\n")

	return output
}


func HandleError(err error) {
	if err != nil {
		logger.Write("ERROR", err.Error())
	}
}

func ReturnMapStringVal(k map[string]interface{}, key string) string {

	if _, ok := k[key].(string); ok {

		return k[key].(string)
	} else {

		return ""
	}
}

func ReturnMapBoolVal(k map[string]interface{}, key string) bool {

	if _, ok := k[key].(string); ok {

		return k[key].(string) == "true"
	} else {

		return false
	}
}

func ReturnMapSliceStringVal(k map[string]interface{}, key string) []string {

	if _, ok := k[key].(string); ok {

		return k[key].([]string)
	} else {

		return []string{}
	}
}



func IsBase64(s string) bool {
	_, err := base64.StdEncoding.DecodeString(s)
	return err == nil
}


func ShowJSON(byte []byte) {
	buf := new(bytes.Buffer)
	json.Indent(buf, byte, "", "  ")
	fmt.Println(buf)
}

func IsDateValid(date string)  bool {

	strings.Replace(date,"","",-1)

	dateFormat := "(19|20)[0-9]{1}[0-9]{1}[0-1]{1}[0-9]{1}[0-3]{1}[0-9]{1}[0-2]{1}[0-9]{1}[0-5]{1}[0-9]{1}[0-5]{1}[0-9]{1}"

	ok,err := regexp.MatchString(dateFormat,date)

	 if err != nil {

		  logger.Write("ERROR", err.Error())
	 }

        return ok
}