package endpoint_common

import (
	"github.com/dminGod/D30-HectorDA/utils"
	"strings"
)

func ReturnString(input interface{}) string {

	return "'" + strings.Replace(input.(string), "'", "''", -1) + "'"

}

func ReturnInt(input interface{}) string {

	return strings.Replace(input.(string), "'", "\\'", -1)

}

func ReturnSetText(input interface{}) string {

	value := "{"

	inputs := input.([]interface{})

	for _, v := range inputs {
		switch vType := v.(type) {
		case string:
			value += (ReturnString(v) + ",")
		case map[string]interface{}:
			value += (ReturnString(utils.EncodeJSON(v.(map[string]interface{}))) + ",")
		default:
			_ = vType
		}
	}

	value = strings.Trim(value, ",")
	value += "}"

	return value
}

func ReturnMap(input interface{}) string {

	value := utils.EncodeJSON(input.(map[string]interface{}))

	return ReturnString(value)
}

func ReturnCondition(input map[string]interface{}) string {

	condition := ""
	relationalOperator := ""
	if input["valueType"].(string) == "single" {
		relationalOperator = "="
	} else if input["valueType"].(string) == "multi" {
		relationalOperator = "CONTAINS"
	}

	switch dataType := input["type"]; dataType {

	case "text", "timestamp", "set<text>":
		condition += " " + input["column"].(string) + " " + relationalOperator + " " + ReturnString(input["value"].(string))
	case "int":
		condition += " " + input["column"].(string) + " " + relationalOperator + " " + ReturnInt(input["value"].(string))
	}

	return condition
}
