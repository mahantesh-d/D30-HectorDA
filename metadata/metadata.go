package metadata

import (
	"github.com/dminGod/D30-HectorDA/utils"
	"github.com/dminGod/D30-HectorDA/logger"
	"github.com/gocql/gocql"
)

// Interpret is used to cross-reference application metadata with the request metadata
// and returns metadata specific to the request for further processing
func Interpret(metadata map[string]interface{}, payload map[string]interface{}) map[string]interface{} {

	return interpret(metadata, payload)
}

func interpret(metadata map[string]interface{}, payload map[string]interface{}) map[string]interface{} {

	output := make(map[string]interface{})
	output["databaseType"] = metadata["databaseType"]
	output["version"] = metadata["version"]
	output["database"] = metadata["database"]
	output["table"] = metadata["table"]
	output["child_table_prefix"] = metadata["child_table_prefix"]

	outputKeyValues := make(map[string]interface{})
	outputKeyMeta := make(map[string]interface{})

	record_uuid := ""


	for k, v := range metadata["fields"].(map[string]interface{}) {

		f := v.(map[string]interface{})
		val := make([]string, 2)
		val[0] = f["name"].(string)
		val[1] = f["type"].(string)


		switch t := val[1]; t {

		case "uuid":
			record_uuid_u, _ := gocql.RandomUUID()
			record_uuid = record_uuid_u.String()
			outputKeyValues[k] = record_uuid
			outputKeyMeta[k] = t

		case "text":
			addData(&outputKeyValues, &outputKeyMeta, k, payload, val[0], t)

		case "set<text>":
			addData(&outputKeyValues, &outputKeyMeta, k, payload, val[0], t)

		case "map<text,text>":
			addData(&outputKeyValues, &outputKeyMeta, k, payload, val[0], t)

		case "int":
			addData(&outputKeyValues, &outputKeyMeta, k, payload, val[0], t)

		case "timestamp":
			addData(&outputKeyValues, &outputKeyMeta, k, payload, val[0], t)
		}

	}

	output["field_keyvalue"] = outputKeyValues
	output["field_keymeta"] = outputKeyMeta
	output["record_uuid"] = record_uuid

	return output

}





func addData(outputKeyValues *map[string]interface{}, outputKeyMeta *map[string]interface{}, key string, payload map[string]interface{}, value interface{}, dataType string) {

	if utils.KeyInMap(value.(string), payload) {
		(*outputKeyValues)[key] = payload[value.(string)]
		(*outputKeyMeta)[key] = dataType
	}
}

// InterpretSelect is used to cross-reference application metadata with the request metadata
// and returns metadata specific to the request for further processing

func InterpretSelect(input map[string]interface{}, filters map[string]string) map[string]interface{} {

	output := make(map[string]interface{})

	fields := input["fields"].(map[string]interface{})

	for k, v := range filters {

		if utils.KeyInMap(k, fields) {

			fieldrecord := fields[k].(map[string]interface{})
			fieldrecord["value"] = v
			output[k] = fieldrecord

		} else {

			// If we find that some filter is used that is not valid for this API we are
			// removing everything so that the response goes back as invalid filter to the user
			// The API is strict and will not respond if the user is using any wrong filter

			output = make(map[string]interface{})
			input["fields"] = output

			// Logging this out as an error
			logger.Write("ERROR", "Field passed in the filters '" + k + "' was not found in the JSON API filter definition. Please use the correct filter. Using wrong filters has been set to cause API to fail." )
			return input
		}
	}

	input["fields"] = output
	return input
}
