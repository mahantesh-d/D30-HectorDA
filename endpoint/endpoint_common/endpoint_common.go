package endpoint_common

import (
	"github.com/dminGod/D30-HectorDA/utils"
	"strings"
	"github.com/dminGod/D30-HectorDA/logger"
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

func ReturnSetTextPG(input interface{}) string {

	value := "ARRAY["

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
	value += "]::text[]"

	return value
}


func ReturnMap(input interface{}) string {

	value := utils.EncodeJSON(input.(map[string]interface{}))

	return ReturnString(value)
}

func ReturnCondition(input map[string]interface{}, whereCondition string, dbType string) string {

	condition := ""
	relationalOperator := ""
	endRelationalOperator := ""
	isNotionalField := false
	notionalOperator := ""


	// Check if this is a notional field, if so set the flag
	if _, ok := input["is_notional_field"].(string); ok {

		if _, ok := input["notional_operator"].(string); ok && input["is_notional_field"].(string) == "true" {

			isNotionalField = true
			notionalOperator = input["notional_operator"].(string)
		} else {


			logger.Write("ERROR", "Field " + input["name"].(string) + " marked as notional but no operator specified.")
		}


	}


	if input["valueType"].(string) == "single" {


		if isNotionalField {


			relationalOperator = notionalOperator
		} else {

			relationalOperator = "="
		}

	} else if input["valueType"].(string) == "multi" {


		if isNotionalField {


			relationalOperator = notionalOperator
		} else {

			if dbType == "postgresxl" {

				relationalOperator = " = (ARRAY["
				endRelationalOperator = "]) "

			} else {

				relationalOperator = "CONTAINS"
			}

		}
	}

	switch dataType := input["type"]; dataType {

	case "text", "timestamp", "set<text>":

		for _, value := range input["value"].([]string) {

			if len(value) > 0 {

				condition += "  " + input["column"].(string) + " " + relationalOperator + " " + ReturnString(value) + " " + endRelationalOperator + " " + whereCondition
			}
		}

	case "int":

		for _, value := range input["value"].([]string) {

			if len(value) > 0 {
				condition += "  " + input["column"].(string) + " " + relationalOperator + " " + ReturnInt(input["value"].(string)) + " " + endRelationalOperator + " " + whereCondition
			}

		}

	}

	condition = strings.Trim(condition, whereCondition)

	return condition
}
