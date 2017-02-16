package metadata

import(
	"github.com/dminGod/D30-HectorDA/utils"
)

// Interpret is used to cross-reference application metadata with the request metadata
// and returns metadata specific to the request for further processing
func Interpret(metadata map[string]interface{}, payload map[string]interface{}) map[string]interface{} {
	
	return interpret(metadata, payload)
}

func interpret(metadata map[string]interface{}, payload map[string]interface{}) map[string]interface{} {

	outputString := ""


	output := make(map[string]interface{})
	output["databaseType"] = metadata["databaseType"]
	output["version"] = metadata["version"]
	output["database"] = metadata["database"]
	output["table"] = metadata["table"]

	outputKeyValues := make(map[string]interface{})
	outputKeyMeta := make(map[string]interface{})
	for k,v := range metadata["fields"].(map[string]interface{}) {
        
		f := v.(map[string] interface{})
        	val := make([]string,2)
        	val[0] = f["name"].(string)
		val[1] = f["type"].(string)

        	outputString += k

        	switch t := val[1]; t {

                case "uuid":
                        outputKeyValues[k] = "NOW()"
                        outputKeyMeta[k] = t
                case "text":
                        addData(&outputKeyValues,&outputKeyMeta, k , payload, val[0], t)
                case "set<text>":
                        addData(&outputKeyValues,&outputKeyMeta, k, payload, val[0], t)
                case "map<text,text>":
                        addData(&outputKeyValues,&outputKeyMeta, k, payload, val[0], t)
        	case "int":
			addData(&outputKeyValues,&outputKeyMeta, k, payload, val[0], t)
		case "timestamp":
			addData(&outputKeyValues,&outputKeyMeta, k, payload, val[0], t)
		}

	}

	output["field_keyvalue"] = outputKeyValues
	output["field_keymeta"] = outputKeyMeta

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
	for k,v := range filters {
		if utils.KeyInMap(k,fields) {
			fieldrecord := fields[k].(map[string]interface{})
			fieldrecord["value"] = v
			output[k] = fieldrecord
		} else {
			output = make(map[string]interface{})
			input["fields"] = output
			return input
		}
	}

	input["fields"] = output
	return input
}
