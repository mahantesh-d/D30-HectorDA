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



func PrepareInsert(tableName string, attr map[string] interface{}) string {

        query := "INSERT INTO " + tableName

        name := " ( "
        value := " ( "
        for i,v := range attr {

                name += (i + ",")

		switch c := v.(type) {

         		case string:
                		val := "'" + v.(string) + "'"
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
