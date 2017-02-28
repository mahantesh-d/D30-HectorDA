package cassandra_helper

import (
	"fmt"
	"github.com/dminGod/D30-HectorDA/endpoint/endpoint_common"
	"strings"
)

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

func MakeCassandraInQuery(prestoResult []map[string]interface{}, metaInput map[string]interface{}) string {

	var makeRet []string
	tableName := metaInput["table"].(string)

	for _, val := range prestoResult {

		makeRet = append(makeRet, val["stock_adjustment_pk"].(string))
	}

	retStr := "SELECT * FROM " + tableName + " WHERE " + tableName + "_pk IN (" + strings.Join(makeRet, ",") + ")"

	fmt.Println(retStr)

	return retStr
}

func InsertQueryBuild(metaInput map[string]interface{}) []string {

	name := ""
	value := ""

	// This array will hold the minor queies
	var minor_queries []string
	var child_table_name string
	var record_uuid string
	var child_query string

	for k, v := range metaInput["field_keymeta"].(map[string]interface{}) {

		name += (k + ",")
		value += " "

		switch dataType := v.(string); v {
		case "uuid":
			value += ((metaInput["field_keyvalue"].(map[string]interface{}))[k].(string))

		case "text", "timestamp":
			value += endpoint_common.ReturnString((metaInput["field_keyvalue"].(map[string]interface{}))[k])

		case "set<text>":
			value += endpoint_common.ReturnSetText((metaInput["field_keyvalue"].(map[string]interface{}))[k])

			// Make the insert into the extra cassandra table :
			// Table name : Table Prefix + field name
			child_table_name = metaInput["child_table_prefix"].(string) + k
			record_uuid = metaInput["record_uuid"].(string)

			inputs := ((metaInput["field_keyvalue"].(map[string]interface{}))[k]).([]interface{})

			for _, v := range inputs {

				switch vType := v.(type) {

				case string:
					// Only for string values we are handling inserts into external tables
					child_query = "INSERT INTO " + child_table_name + " (ct_pk, parent_pk, value) VALUES ("
					child_query += "now(), "
					child_query += endpoint_common.ReturnString(record_uuid) + ", " + endpoint_common.ReturnString(value)

					minor_queries = append(minor_queries, child_query)
				case map[string]interface{}:
					// Do Nothing here
				default:
					_ = vType
				}
			}

		case "map<text,text>":
			value += endpoint_common.ReturnMap((metaInput["field_keyvalue"].(map[string]interface{}))[k])

		case "int":
			value += endpoint_common.ReturnInt((metaInput["field_keyvalue"].(map[string]interface{}))[k])

		default:
			_ = dataType
		}

		value += ","
	}

	name = strings.Trim(name, ",")
	value = strings.Trim(value, ",")

	main_query := "INSERT INTO " + metaInput["table"].(string) + " ( " + name + " ) VALUES ( " + value + " ) "

	queries := append(minor_queries, main_query)

	return queries
}

func SelectQueryBuild(metaInput map[string]interface{}) string {

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
				query += endpoint_common.ReturnCondition(fieldMeta)
			}
		} else {
			querySorter := make([][]string, 20)
			for _, v := range fields {
				fieldMeta := v.(map[string]interface{})
				priority := int(fieldMeta["priority"].(float64))
				//query += endpoint_common.ReturnCondition(fieldMeta) + " AND"
				querySorter[priority] = append(querySorter[priority], endpoint_common.ReturnCondition(fieldMeta)+" AND")
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

func SelectQueryCassandraByID(metaInput map[string]interface{}, pk_id string) string {

	table := metaInput["table"].(string)

	query := "SELECT * from " + table + " WHERE " + table + "_pk IN ( " + pk_id + ")"

	return query
}

// We are deciding what type of query to run for the application.
// Currently there are 3 choices :

//   Single Column Query "single_column" : This is a straight query, nothing to do.

//   Multi Column Same Index "multi_column_same_index" : This is also handled by cassandra, but will use allow
//     filtering, but the ordering of the fields will be based on the cardinality so we are working with the lowest
//     amount of data that needs to be processed.

//   Multi Column With Mixed Indexes "multi_column_mixed_index" : This is a query that combines fields that have both
//     SASI and secondary indexes. Because cassandra can not handle this type of a query it needs to be sent to Presto
//     Presto will only return back one or more IDs in an array which are the result of the search. The actual data will be
//     fetched from cassandra and returned back to the user as it is currently done.

// Using the older method for checking valid cassandra query, not this one

//func DecideQueryTypeByRequest(reqAbs *model.RequestAbstract, metaDataSelect map[string]interface{}) string {
//
//	// Get the details of the metadata columns and figure out what is going on
//
//	apiData := utils.FindMap("apiName", reqAbs.RouteName, metaDataSelect)
//
//	fmt.Println( reqAbs.Filters )
//
//	var allSame bool
//
//	allSame = true
//
//	lastRecIndType := ""
//
//	for k, _ := range reqAbs.Filters {
//
//		curFieldIndType := apiData["fields"].(map[string]interface{})[k].(map[string]interface{})["indexType"].(string)
//
//		if lastRecIndType == "" {
//
//			lastRecIndType = curFieldIndType
//		}
//
//		if lastRecIndType != curFieldIndType {
//
//			allSame = false
//		}
//
//	}
//
//
//	return "single_column"
//}
