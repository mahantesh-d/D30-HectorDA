package utils

import (
	"encoding/json"
	"github.com/dminGod/D30-HectorDA/logger"
	"strings"
	"strconv"
)

func DecodeJSON(input interface{}) map[string]interface{} {

        var payload interface{}

        err := json.Unmarshal([]byte(input.(string)), &payload)

        if err != nil {
                logger.Write("ERROR", err.Error())
        }

        return payload.(map[string]interface{})

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

