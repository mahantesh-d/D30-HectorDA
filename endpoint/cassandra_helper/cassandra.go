package cassandra_helper

import (
	"github.com/dminGod/D30-HectorDA/endpoint/endpoint_common"
	"strings"
	"strconv"
	"fmt"
	"github.com/dminGod/D30-HectorDA/config"
	"github.com/dminGod/D30-HectorDA/utils"
	"reflect"
	"github.com/dminGod/D30-HectorDA/logger"
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
	database := metaInput["database"].(string)
	for _, val := range prestoResult {

		makeRet = append(makeRet, val["stock_adjustment_pk"].(string))
	}

	retStr := "SELECT * FROM " + database + "." + tableName + " WHERE " + tableName + "_pk IN (" + strings.Join(makeRet, ",") + ")"

	return retStr
}

func InsertQueryBuild(metaInput map[string]interface{}) []string {

	name := ""
	value := ""

	// This array will hold the minor queies
	var minor_queries []string
	// var child_table_name string
	var record_uuid string
	// var child_query string

	fmt.Println(record_uuid)



	database := metaInput["database"].(string)
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
			// mValue := endpoint_common.ReturnSetText((metaInput["field_keyvalue"].(map[string]interface{}))[k])
			// Make the insert into the extra cassandra table :
			// Table name : Table Prefix + field name
			// child_table_name = metaInput["child_table_prefix"].(string) + k
			record_uuid = metaInput["record_uuid"].(string)

			inputs := ((metaInput["field_keyvalue"].(map[string]interface{}))[k]).([]interface{})

			for _, v := range inputs {

				switch vType := v.(type) {

				case string:
					// Only for string values we are handling inserts into external tables
					// child_query = "INSERT INTO " + database + "." + child_table_name + " (ct_pk, parent_pk, value) VALUES ("
					// child_query += "now(), "
					// child_query += record_uuid + ", " + endpoint_common.ReturnString(mValue)
					// child_query += " ) "
					//minor_queries = append(minor_queries, child_query)
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

	main_query := "INSERT INTO " + database + "." + metaInput["table"].(string) + " ( " + name + " ) VALUES ( " + value + " ) "

	queries := append(minor_queries, main_query)

	return queries
}

func SelectQueryBuild(metaInput map[string]interface{}) string {

	table := metaInput["table"].(string)
	database := metaInput["database"].(string)

//	fmt.Println( " Metadata related to Get", config.Metadata_get)
//	fmt.Println( " Metadata related to Insert", config.Metadata_insert)

	myFields := utils.FindMap("table", table, config.Metadata_insert())

	var selectString string

	if len(myFields) != 0 {

		selectString = makeSelect(myFields)
	} else {

		logger.Write("ERROR", "Cassandra Query error, Couldn not find column information on the table from insert api file while trying to make the select query fields, is the entry put in? defaulting to *, but users will see table columns instead of expected field names.")
		selectString = "*"
	}


	query := "SELECT " + selectString + " FROM " + database + "." + table
	fields := metaInput["fields"].(map[string]interface{})
	limit := 100
	if len(fields) > 0 {

		query += " WHERE"

		if len(metaInput["token"].(string)) > 0 {
			query += (" token(" + table + "_pk) > token (" + metaInput["token"].(string) + ") AND")
		}
		if int(metaInput["limit"].(int32)) > 0 {
			limit = int(metaInput["limit"].(int32))
		}
		fmt.Println(query)
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

				query += " LIMIT " + strconv.Itoa(limit) + " ALLOW FILTERING"
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

			query += " LIMIT " + strconv.Itoa(limit) + " ALLOW FILTERING"
		}

	} else {
		query = ""
	}

	logger.Write("INFO", "The select query being run is : " + query)
	return query
}


func makeSelect(fields map[string]interface{}) string {

	if reflect.TypeOf(fields).String() == "map[string]interface {}" {

		selects := []string{}

		for k, v := range fields["fields"].(map[string]interface{}) {

			// If you are taking select data
			//fmt.Println(v.(map[string]interface{})["column"], " as ", k)

			selects = append(selects, k + " as \"" + v.(map[string]interface{})["name"].(string) + "\"")
		}

		return strings.Join(selects, ", ")
	} else {

		return "*"
	}

}


func StratioSelectQueryBuild(metaInput map[string]interface{}) string {

        table := metaInput["table"].(string)
	database := metaInput["database"].(string)
        query := "SELECT * FROM " + database + "." + table + " WHERE lucene= '"
        fields := metaInput["fields"].(map[string]interface{})


        filter := "{ filter : {  type: \"boolean\" , must: ["

        typeTemplate := "{ type: \"phrase\", field: \"|1\", value: \"|2\" }"

        for _,v := range fields {
                fieldInfo := v.(map[string]interface{})
		condition := strings.Replace(typeTemplate,"|1",fieldInfo["column"].(string),-1)
                condition = strings.Replace(condition,"|2",fieldInfo["value"].(string),-1)
                filter += condition + ","
        }

        filter = strings.Trim(filter,",")
        filter += "]}}"

        query += (filter + "'")

        return query
}


func SelectQueryCassandraByID(metaInput map[string]interface{}, pk_id string) string {

	table := metaInput["table"].(string)
	database := metaInput["database"].(string)
	query := "SELECT * FROM " + database + "." + table + " WHERE " + table + "_pk IN ( " + pk_id + ")"

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
