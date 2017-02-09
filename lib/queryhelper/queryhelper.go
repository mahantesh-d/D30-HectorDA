package queryhelper

import(
	"fmt"
	"strings"
	"github.com/dminGod/D30-HectorDA/utils"
)
func PrepareQuery(metaInput map[string]interface{}) string {

	// get the endpoint
	databaseType := metaInput["databaseType"]
	
	if databaseType == "cassandra" {
		cassandraQueryBuild(metaInput)
	}
		
	return ""
}


func cassandraQueryBuild(metaInput map[string]interface{}) string {

	name := ""
	value := ""
	
	for k, v := range metaInput["field_keymeta"].(map[string]interface{}) {
		name += (k  + ",")
		value += " "
		switch dataType := v.(string); v {
			case "uuid":
				value += ((metaInput["field_keyvalue"].(map[string]interface{}))[k].(string))
			case "text":
				value += returnString((metaInput["field_keyvalue"].(map[string]interface{}))[k])
			case "set<text>":
				value += returnSetText((metaInput["field_keyvalue"].(map[string]interface{}))[k])
			case "map<text,text>":
				value += returnMap((metaInput["field_keyvalue"].(map[string]interface{}))[k])
			default:
				_ = dataType
		}
		value += ","
	}

	name = strings.Trim(name,",")
	value = strings.Trim(value,",")

	fmt.Println("INSERT INTO " + metaInput["table"].(string) + " ( " + name + " ) VALUES ( " + value + " ) ")
	

	return ""
}


func returnString(input interface{}) string{

	return "'" + strings.Replace(input.(string),"'","\\'",-1) + "'"

}

func returnSetText(input interface{}) string {

	value := "{"
	
	inputs := input.([] interface{})

	for _,v := range inputs {
		value += (returnString(v) + ",")
	}

	value = strings.Trim(value,",")	
	value += "}"

	return value
}

func returnMap(input interface{}) string {

	value := utils.EncodeJSON(input.(map[string]interface{}))
	
	return returnString(value)
}
