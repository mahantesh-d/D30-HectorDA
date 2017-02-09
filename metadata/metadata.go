package metadata

import(
	"os"
	"github.com/dminGod/D30-HectorDA/utils"
)

func Interpret(application string, payload map[string]interface{}) map[string]interface{} {
	
	path := os.Getenv("HECTOR_HOME") + "/" + application + "/meta.json"
	return interpretfile(path,payload)

}

func InterpretFile(path string, payload map[string]interface{}) map[string]interface{} {

	return interpretfile(path,payload)

}


func interpretfile(path string, payload map[string]interface{}) map[string]interface{} {
	
	metadataString := `
	{
	"databaseType" : "cassandra",
	"version" : 1,
	"database" : "all_trade",
	"table" : "foobar",
	"fields" : 
		{
		"id":  
			{
			"name": "id",
			"type" : "uuid"
			},
		"name" : 
			{ 
			"name": "name",
			"type" : "text"
			},
		"email":  
			{
			"name": "email_id",
			"type" : "set<text>"
			},
		"dynamic": 
			{
			"name": "dyn",
			"type": "map<text,text>"
			}
		}
	}
	`
	metadata := utils.DecodeJSON(metadataString)
		
	outputString := ""


	output := make(map[string]interface{})

	output["databaseType"] = metadata["databaseType"]
	output["version"] = metadata["version"]
	output["database"] = metadata["database"]
	output["table"] = metadata["table"]

 	output_key_values := make(map[string]interface{})
	output_key_meta := make(map[string]interface{})
	for k,v := range metadata["fields"].(map[string]interface{}) {

		f := v.(map[string] interface{})
	
		val := make([]string,2)
		val[0] = f["name"].(string)
		val[1] = f["type"].(string)	
			
        	outputString += k

        	
		switch t := val[1]; t {

       			case "uuid":
				output_key_values[k] = "NOW()"
				output_key_meta[k] = t
                	case "text":
				addData(&output_key_values,&output_key_meta, k , payload, val[0], t)
                	case "set<text>":
				addData(&output_key_values,&output_key_meta, k, payload, val[0], t)
			case "map<text,text>":
        			addData(&output_key_values,&output_key_meta, k, payload, val[0], t)
		}

	}

	output["field_keyvalue"] = output_key_values
	output["field_keymeta"] = output_key_meta

	return output

}

func addData(output_key_values *map[string]interface{}, output_key_meta *map[string]interface{}, key string, payload map[string]interface{}, value interface{}, dataType string) {
	
	if utils.KeyInMap(value.(string), payload) {
		(*output_key_values)[key] = payload[value.(string)]
		(*output_key_meta)[key] = dataType
	}
}
