package metadata

import (
	"github.com/dminGod/D30-HectorDA/logger"
	"github.com/dminGod/D30-HectorDA/utils"
	"github.com/gocql/gocql"
	"github.com/dminGod/D30-HectorDA/config"
	)


var AllAPIs APIs

func init() {

	//AllAPIs.Populate()
}



// Interpret is used to cross-reference application metadata with the request metadata
// and returns metadata specific to the request for further processing
func Interpret(metadata map[string]interface{}, payload map[string]interface{}, filters string) map[string]interface{} {

	return interpret(metadata, payload, filters)
}


func interpret(metadata map[string]interface{}, payload map[string]interface{}, filters string) map[string]interface{} {

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

	for _, v := range metadata["fields"].(map[string]interface{}) {
		f := v.(map[string]interface{})

//		fmt.Println("Metadata Debug f object: ", f)

		val := make([]string, 2)
		val[0] = f["name"].(string)
		val[1] = f["type"].(string)
		curKey := f["column"].(string)

		isCassandraPrimaryKey := false

		if _, ok := f["tags"].([]interface{}); ok {

			isCassandraPrimaryKey = utils.MatchFieldTag(f["tags"].([]interface{}), "cassandra_pk")
		}

		if isCassandraPrimaryKey {

			record_uuid_u, _ := gocql.RandomUUID()
			record_uuid = record_uuid_u.String()
			outputKeyValues[curKey] = record_uuid
			outputKeyMeta[curKey] = val[1]
		}

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

	output["put_supported"] = false
	output["possibleUpdateRequest"] = false
	output["updateCondition"] = map[string][]string{}

	if output["put_supported"] == false {

		output["possibleUpdateRequest"], output["updateCondition"] = CheckCondition(metadata, payload, filters)
	}

	output["field_keyvalue"] = outputKeyValues
	output["field_keymeta"] = outputKeyMeta
	output["record_uuid"] = record_uuid

	return output
}


// metadata map[string]interface{}, payload map[string]interface{}

// Input table / API related data

func InterpretUpdateFilters( input map[string]interface{}, payload map[string]interface{}, filters map[string]interface{}) (map[string]interface{}, bool) {

	outputFields := make(map[string]interface{})
	output := make(map[string]interface{})

	// Get the details of the object
	output["databaseType"] = input["databaseType"]
	output["version"] = input["version"]
	output["database"] = input["database"]
	output["table"] = input["table"]

	outputKeyValues := make(map[string]interface{})
	outputKeyMeta := make(map[string]interface{})

	record_uuid := ""

	for _, v := range input["fields"].(map[string]interface{}) {
		f := v.(map[string]interface{})

		//		fmt.Println("Metadata Debug f object: ", f)

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

	output["put_supported"] = false
	//output["possibleUpdateRequest"] = false
	//output["updateCondition"] = map[string][]string{}

	//if output["put_supported"] == false {
	//
	//	output["possibleUpdateRequest"], output["updateCondition"] = CheckCondition(input, payload)
	//}

	output["field_keyvalue"] = outputKeyValues
	output["field_keymeta"] = outputKeyMeta
	output["record_uuid"] = record_uuid

	input["updateCondition"] = map[string][]string{}
	tmpUpdateCondition := map[string][]string{}

	// This is from JSON
//	fields := input["fields"].(map[string]interface{})

	// Put field is there in the json
	output["put_supported"] = false

	if _, ok := input["put_supported"].(string); ok {

		if input["put_supported"] == "true" {

			output["put_supported"] = true;
		}
	} else {

		return map[string]interface{}{}, false
	}

	/*
	// Loop over the fields -- You want to extract the filters and fill the filters
	for _, vv := range filters {

		tmpVal := vv.(map[string]string)

		k := tmpVal["key"]
		v := tmpVal["value"]

		// Field is there in the get params
		if utils.ValueInMapSelect(k, fields) {

			tmpResp := utils.GetSelectMap(k, fields, v)

			if _, ok := tmpResp["column"].(string); !ok {

				continue
			}



			if _, ok := tmpResp["is_put_filter_field"]; ok && tmpResp["is_put_filter_field"] == "true" {

				outputFields[k] = tmpResp
				tmpUpdateCondition[ tmpResp["column"].(string) ] = append(tmpUpdateCondition[k], v)
			}

		} else {

			// If we find that some filter is used that is not valid for this API we are
			// removing everything so that the response goes back as invalid filter to the user
			// The API is strict and will not respond if the user is using any wrong filter

			kkk := map[string]interface{}{}
			kkk["fields"] = map[string]interface{}{}

			//			fmt.Println("DM : Searching for field", k, "Fields are ", fields)

			// Logging this out as an error
			logger.Write("ERROR", "Field passed in the filters '"+ k +"' was not found in the JSON API filter definition. Please use the correct filter. Using wrong filters has been set to cause API to fail.")
			fmt.Println("Fields are : ", fields)
			return kkk, false
		}
	}
	*/

	output["updateCondition"] = tmpUpdateCondition

	output["fields"] = outputFields

	return output, true
}


// KeyMeta is a map[string]sting hash Name to type
// OutputKeyValues is a map[string]interface{} for Key Value of the records
func addData(outputKeyValues *map[string]interface{}, outputKeyMeta *map[string]interface{}, key string, payload map[string]interface{}, value interface{}, dataType string) {

	if utils.KeyInMap(value.(string), payload) {
		(*outputKeyValues)[key] = payload[value.(string)]
		(*outputKeyMeta)[key] = dataType
	}
}







// InterpretSelect is used to cross-reference application metadata with the request metadata
// and returns metadata specific to the request for further processing

// InterpretSelect ( table_related_data   map[string]interface{} -- query_related_data map[string]string )

func InterpretSelect(table_name string, filters map[string]interface{}) map[string]interface{} {

	output := make(map[string]interface{})

	input := utils.FindMap("table", table_name, config.Metadata_get())

	// This is the table related data
//	fmt.Println("Input sent to Interpret for select(expecting string of interface)", input)

	// This is query related data
	//fmt.Println("Filters map of string to string(expecting string of interface)", filters)

	if len(input) == 0 {

		logger.Write("ERROR", "Input values not found, something is breaking..")
		return map[string]interface{}{}
	}

	// This is from JSON
	fields := input["fields"].(map[string]interface{})

	for _, vv := range filters {

		tmpVal := vv.(map[string]string)

		k := tmpVal["key"]
		v := tmpVal["value"]

		if utils.ValueInMapSelect(k, fields) {

			// Key is throwaway .... slice was duplicating values very weirdly..
			output[k] =  utils.GetSelectMap(k, fields, v)
		} else {

			// If we find that some filter is used that is not valid for this API we are
			// removing everything so that the response goes back as invalid filter to the user
			// The API is strict and will not respond if the user is using any wrong filter

			output := []map[string]interface{}{}
			input["fields"] = output

//			fmt.Println("DM : Searching for field", k, "Fields are ", fields)

			// Logging this out as an error
			// logger.Write("ERROR", "Field passed in the filters '"+ k +"' was not found in the JSON API filter definition. Please use the correct filter. Using wrong filters has been set to cause API to fail.")
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

