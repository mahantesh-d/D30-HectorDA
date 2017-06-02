package endpoint_common

import (
	"github.com/dminGod/D30-HectorDA/utils"
	"strings"
	"github.com/dminGod/D30-HectorDA/logger"
	"regexp"
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


func ReturnConditionKVComplex(input map[string]interface{}, value string, dbType string) string {

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

	case "timestamp":

		if len(value) > 0 {

			parsedTime := matchTimeFromStringGet( value )

			if len(parsedTime) > 6 {

				logger.Write("INFO", "Time got parsed from native format : " + value + "to " + parsedTime)
				condition += "  " + input["column"].(string) + " " + relationalOperator + " " + ReturnString(parsedTime) + " " + endRelationalOperator
			}
		}


	case "text",  "set<text>":

			if len(value) > 0 {

				condition += "  " + input["column"].(string) + " " + relationalOperator + " " + ReturnString(value) + " " + endRelationalOperator
			}


	case "int":

			if len(value) > 0 {
				condition += "  " + input["column"].(string) + " " + relationalOperator + " " + ReturnInt(input["value"].(string)) + " " + endRelationalOperator
			}


	default:
			logger.Write("ERROR", "Field sent for create " + input["column"].(string) + " is invalid")

		break

	}

	return condition
}


func matchTimeFromStringGet(timePassed string) string {

	var retString = ""


	re := regexp.MustCompile(`(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})\+?(\d{4})?`)
	segs := re.FindAllStringSubmatch(timePassed, -1)


	if len(segs) > 0 {

		retString = segs[0][1] + "-" + segs[0][2] + "-" + segs[0][3] + " " + segs[0][4] + ":" + segs[0][5] + ":" + segs[0][6] + "+0700"
	}

	return retString
}