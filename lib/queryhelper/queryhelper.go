package queryhelper

import(
	_"fmt"
	"strings"
	"github.com/dminGod/D30-HectorDA/utils"
)
func PrepareInsertQuery(metaInput map[string]interface{}) string {

	// get the endpoint
	databaseType := metaInput["databaseType"].(string)
	
	query := ""
	if databaseType == "cassandra" {
		query = cassandraInsertQueryBuild(metaInput)
	}
		
	return query
}


func PrepareSelectQuery(metaInput map[string]interface{}) string {
	// get the endpoint
	databaseType := metaInput["databaseType"].(string)

	query := ""
	if databaseType == "cassandra" {
        	query = cassandraSelectQueryBuild(metaInput)
	}

	return query

}


func cassandraInsertQueryBuild(metaInput map[string]interface{}) string {

	name := ""
	value := ""
	
	for k, v := range metaInput["field_keymeta"].(map[string]interface{}) {
		name += (k  + ",")
		value += " "
		switch dataType := v.(string); v {
			case "uuid":
				value += ((metaInput["field_keyvalue"].(map[string]interface{}))[k].(string))
			case "text","timestamp":
				value += returnString((metaInput["field_keyvalue"].(map[string]interface{}))[k])
			case "set<text>":
				value += returnSetText((metaInput["field_keyvalue"].(map[string]interface{}))[k])
			case "map<text,text>":
				value += returnMap((metaInput["field_keyvalue"].(map[string]interface{}))[k])
			case "int":
				value += returnInt((metaInput["field_keyvalue"].(map[string]interface{}))[k])
			default:
				_ = dataType
		}
		value += ","
	}

	name = strings.Trim(name,",")
	value = strings.Trim(value,",")

	query := "INSERT INTO " + metaInput["table"].(string) + " ( " + name + " ) VALUES ( " + value + " ) "
	

	return query
}

func cassandraSelectQueryBuild(metaInput map[string]interface{}) string {

	table := metaInput["table"].(string)

	query := "SELECT * from " + table + " WHERE";

	fields := metaInput["fields"].(map[string]interface{})
	numberOfParams := len(fields)
	// three type of queries
	// single condition
	// multiple conditions in a specific predictable order
	// multiple conditions in a non-related order
	if numberOfParams == 1 {
		for _, v := range fields {
			fieldMeta := v.(map[string]interface{})
			query += returnCondition(fieldMeta)
		}
	} else {
		for _, v := range fields {
        		fieldMeta := v.(map[string]interface{})
        		query += returnCondition(fieldMeta) + " AND"
		}
		query = strings.Trim(query,"AND")
	
		query += "ALLOW FILTERING"
	}

	return query
}
func returnString(input interface{}) string{

	return "'" + strings.Replace(input.(string),"'","\\'",-1) + "'"

}


func returnInt(input interface{}) string{

        return strings.Replace(input.(string),"'","\\'",-1)

}


func returnSetText(input interface{}) string {

	value := "{"
	
	inputs := input.([] interface{})

	for _,v := range inputs {
		switch vType := v.(type) {
			case string:
				value += (returnString(v) + ",")
			case map[string]interface{}:
				value +=  (returnString(utils.EncodeJSON(v.(map[string]interface{}))) + ",")
			default:
				_ = vType
		}
	}

	value = strings.Trim(value,",")	
	value += "}"

	return value
}

func returnMap(input interface{}) string {

	value := utils.EncodeJSON(input.(map[string]interface{}))
	
	return returnString(value)
}

func returnCondition(input map[string]interface{}) (string) {

	condition := ""
	relationalOperator := ""
	if input["valueType"].(string) == "single" {
		relationalOperator = "="	
	} else if input["valueType"].(string) == "multi" {
		relationalOperator = "CONTAINS"
	}

	switch dataType := input["type"]; dataType {

		case "text","timestamp","set<text>":
			condition += " " + input["column"].(string) + " " + relationalOperator + " " + returnString(input["value"].(string))
		case "int":
			condition +=  " " + input["column"].(string) + " " + relationalOperator + " " + returnInt(input["value"].(string))	
	}	

	return condition	
}
