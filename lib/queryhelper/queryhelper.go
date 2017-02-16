package queryhelper

import (
	"github.com/dminGod/D30-HectorDA/utils"
	"strings"
)

// PrepareInsertQuery is used to parse Application metadata
// and return the corresponding INSERT query
func PrepareInsertQuery(metaInput map[string]interface{}) string {

	// get the endpoint
	databaseType := metaInput["databaseType"].(string)

	query := ""
	if databaseType == "cassandra" {
		query = cassandraInsertQueryBuild(metaInput)
	}

	return query
}

// PrepareSelectQuery is used to parse Application metadata
// and return the corresponding SELECT query
func PrepareSelectQuery(metaInput map[string]interface{}) string {
	// get the endpoint
	databaseType := metaInput["databaseType"].(string)

	query := ""
	if databaseType == "cassandra" {
		query = cassandraSelectQueryBuild(metaInput)
	} else if databaseType == "presto" {
		query = prestoSelectQueryBuild(metaInput)
	}
	return query

}

// IsValidCassandraQuery is used to analyze the metadata
// and check if the data provided is sufficient to trigger
// a Cassandra Query
func IsValidCassandraQuery(metaInput map[string]interface{}) bool {

	fields := metaInput["fields"].(map[string]interface{})

	// if no fields are passed, return false ( cannot query all data )
	if len(fields) == 0 {

	}

	// if query is only on 1 index ( custom or sasi ), return true
	if len(fields) == 1 {
		return true
	}

	uniqueIndex := ""
	count := 0
	for _, v := range fields {
		metaData := v.(map[string]interface{})
		if count == 0 {
			uniqueIndex = metaData["indexType"].(string)
		} else if uniqueIndex != "" && metaData["indexType"].(string) != uniqueIndex {
			return false
		}
		count++
	}

	return true

}

func cassandraInsertQueryBuild(metaInput map[string]interface{}) string {

	name := ""
	value := ""

	for k, v := range metaInput["field_keymeta"].(map[string]interface{}) {
		name += (k + ",")
		value += " "
		switch dataType := v.(string); v {
		case "uuid":
			value += ((metaInput["field_keyvalue"].(map[string]interface{}))[k].(string))
		case "text", "timestamp":
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

	name = strings.Trim(name, ",")
	value = strings.Trim(value, ",")

	query := "INSERT INTO " + metaInput["table"].(string) + " ( " + name + " ) VALUES ( " + value + " ) "

	return query
}

func cassandraSelectQueryBuild(metaInput map[string]interface{}) string {

	table := metaInput["table"].(string)

	query := "SELECT * from " + table
	fields := metaInput["fields"].(map[string]interface{})
	if len(fields) > 0 {

		query += " WHERE"

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
			querySorter := make([][]string, 20)
			for _, v := range fields {
				fieldMeta := v.(map[string]interface{})
				priority := int(fieldMeta["priority"].(float64))
				//query += returnCondition(fieldMeta) + " AND"
				querySorter[priority] = append(querySorter[priority], returnCondition(fieldMeta)+" AND")
			}

			for _, v := range querySorter {

				for _, vv := range v {
					query += vv
				}
			}

			query = strings.Trim(query, "AND")

			query += "ALLOW FILTERING"
		}

	} else {
		query = ""
	}
	return query
}
func returnString(input interface{}) string {

	return "'" + strings.Replace(input.(string), "'", "\\'", -1) + "'"

}

func returnInt(input interface{}) string {

	return strings.Replace(input.(string), "'", "\\'", -1)

}

func returnSetText(input interface{}) string {

	value := "{"

	inputs := input.([]interface{})

	for _, v := range inputs {
		switch vType := v.(type) {
		case string:
			value += (returnString(v) + ",")
		case map[string]interface{}:
			value += (returnString(utils.EncodeJSON(v.(map[string]interface{}))) + ",")
		default:
			_ = vType
		}
	}

	value = strings.Trim(value, ",")
	value += "}"

	return value
}

func returnMap(input interface{}) string {

	value := utils.EncodeJSON(input.(map[string]interface{}))

	return returnString(value)
}

func returnCondition(input map[string]interface{}) string {

	condition := ""
	relationalOperator := ""
	if input["valueType"].(string) == "single" {
		relationalOperator = "="
	} else if input["valueType"].(string) == "multi" {
		relationalOperator = "CONTAINS"
	}

	switch dataType := input["type"]; dataType {

	case "text", "timestamp", "set<text>":
		condition += " " + input["column"].(string) + " " + relationalOperator + " " + returnString(input["value"].(string))
	case "int":
		condition += " " + input["column"].(string) + " " + relationalOperator + " " + returnInt(input["value"].(string))
	}

	return condition
}

func prestoSelectQueryBuild(metaInput map[string]interface{}) string {

	table := metaInput["table"].(string)

	query := "SELECT * from " + table
	fields := metaInput["fields"].(map[string]interface{})

	for _, v := range fields {
		fieldMeta := v.(map[string]interface{})
		query += returnCondition(fieldMeta) + " AND"
	}

	query = strings.Trim(query, "AND")

	return query
}
