package metadata

import (
	"github.com/dminGod/D30-HectorDA/logger"
	"github.com/dminGod/D30-HectorDA/utils"
	"github.com/gocql/gocql"
	"fmt"

	"github.com/dminGod/D30-HectorDA/config"
)

// Interpret is used to cross-reference application metadata with the request metadata
// and returns metadata specific to the request for further processing
func Interpret(metadata map[string]interface{}, payload map[string]interface{}) map[string]interface{} {

	return interpret(metadata, payload)
}


func interpret(metadata map[string]interface{}, payload map[string]interface{}) map[string]interface{} {

	output := make(map[string]interface{})

	// Get the details of the object
	output["databaseType"] = metadata["databaseType"]
	output["version"] = metadata["version"]
	output["database"] = metadata["database"]
	output["table"] = metadata["table"]
	// output["child_table_prefix"] = metadata["child_table_prefix"]

	outputKeyValues := make(map[string]interface{})
	outputKeyMeta := make(map[string]interface{})

	record_uuid := ""

	for k, v := range metadata["fields"].(map[string]interface{}) {
                fmt.Println(k)
		f := v.(map[string]interface{})

		fmt.Println("Metadata Debug f object: ", f)

		val := make([]string, 2)
		val[0] = f["name"].(string)
		val[1] = f["type"].(string)
		curKey := f["column"].(string)


		switch t := val[1]; t {

		case "uuid":
			record_uuid_u, _ := gocql.RandomUUID()
			record_uuid = record_uuid_u.String()
			outputKeyValues[curKey] = record_uuid
			outputKeyMeta[curKey] = t

		case "text":
			addData(&outputKeyValues, &outputKeyMeta, curKey, payload, val[0], t)

		case "set<text>":
			addData(&outputKeyValues, &outputKeyMeta, curKey, payload, val[0], t)

		case "map<text,text>":
			addData(&outputKeyValues, &outputKeyMeta, curKey, payload, val[0], t)

		case "int":
			addData(&outputKeyValues, &outputKeyMeta, curKey, payload, val[0], t)

		case "timestamp":
			addData(&outputKeyValues, &outputKeyMeta, curKey, payload, val[0], t)
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

// InterpretSelect ( table_related_data   map[string]interface{} -- query_related_data map[string]string )

func InterpretSelect(table_name string, filters map[string]string) map[string]interface{} {

	output := make(map[string]interface{})

	input := utils.FindMap("table", table_name, config.Metadata_get())

	// This is the table related data
	fmt.Println("Input sent to Interpret for select(expecting string of interface)", input)

	// This is query related data
	//fmt.Println("Filters map of string to string(expecting string of interface)", filters)

	if len(input) == 0 {

		logger.Write("ERROR", "Input values not found, something is breaking..")
		fmt.Println("Input values: ", input)
		return map[string]interface{}{}
	}

	fields := input["fields"].(map[string]interface{})

	for k, v := range filters {

		if utils.ValueInMapSelect(k, fields) {

			fieldrecord := utils.GetSelectMap(k, fields)
			fieldrecord["value"] = v
			output[k] = fieldrecord

		} else {

			// If we find that some filter is used that is not valid for this API we are
			// removing everything so that the response goes back as invalid filter to the user
			// The API is strict and will not respond if the user is using any wrong filter

			output = make(map[string]interface{})
			input["fields"] = output

			fmt.Println("DM : Searching for field", k, "Fields are ", fields)

			// Logging this out as an error
			logger.Write("ERROR", "Field passed in the filters '"+k+"' was not found in the JSON API filter definition. Please use the correct filter. Using wrong filters has been set to cause API to fail.")
			return input
		}
	}

	input["fields"] = output
	return input
}



func InterpretPostgres(metadata map[string]interface{}, payload map[string]interface{}) map[string]interface{} {
	outputdatakey:=make(map[string]interface{})
	output := make(map[string]interface{})
	outputdatakey["databaseType"] = metadata["databaseType"]
	outputdatakey["version"] = metadata["version"]
	outputdatakey["database"] = metadata["database"]
	outputdatakey["table"] = metadata["table"]
	outputKeyValues:=make(map[string]interface{})
	outputKeyMeta:=make(map[string]interface{})
	    for s,v:=range metadata["fields"].(map[string]interface{}){
		    value:=make([]string,2)
		    k:= v.(map[string]interface{})
		    value[0]=k["name"].(string)
		    value[1]=k["type"].(string)
		    switch t := value[1]; t{

		    case "int":
			    addData(&outputKeyValues, &outputKeyMeta, s, payload, value[0], t)
		    case "text":
			    addData(&outputKeyValues, &outputKeyMeta, s, payload, value[0], t)
		    case "date":
			    addData(&outputKeyValues, &outputKeyMeta, s, payload, value[0], t)
		    }

	    }

	return output

}

