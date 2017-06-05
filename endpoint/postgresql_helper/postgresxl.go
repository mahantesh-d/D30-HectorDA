package postgresql_helper

import (
	/*"github.com/dminGod/D30-HectorDA/endpoint/endpoint_common"*/
	"strings"
	"github.com/dminGod/D30-HectorDA/endpoint/endpoint_common"
	"github.com/dminGod/D30-HectorDA/utils"
	"github.com/dminGod/D30-HectorDA/config"
	"reflect"
	"github.com/dminGod/D30-HectorDA/logger"
	"github.com/dminGod/D30-HectorDA/metadata"
	"fmt"
)
/**

Schema:
CREATE TABLE userinfo
(
    uid serial NOT NULL,
    username character varying(100) NOT NULL,
    departname character varying(500) NOT NULL,
    Created date,
    CONSTRAINT userinfo_pkey PRIMARY KEY (uid)
)
WITH (OIDS=FALSE);


 */

func ReturnWhereComplex(query string, table_name string, dbType string) (string, bool) {

	var Parser metadata.Pr

	fmt.Println("Query is ", query)

	Parser.SetString(query)

	Parser.Parse()

	retStr, isOk := Parser.MakeString(table_name, dbType)

	fmt.Println("Parser response is : ", retStr)

	return retStr, isOk
}


func UpdateQueryBuilder(metaInput map[string]interface{}) ([]string, bool){

	var query string
	query = ""
	value := ""
	name := ""
	where := ""
	isOk := true

//	database:= metaInput["database"].(string)
	table:= metaInput["table"].(string)

	for k, v := range metaInput["field_keymeta"].(map[string]interface{}) {
		value +=""
		switch s:=v.(string); s{
		case "int":
			name += " " + k + " = " + ((endpoint_common.ReturnString((metaInput["field_keyvalue"].(map[string]interface{}))[k].(string)))) + ","
			// value1 = ((endpoint_common.ReturnString((metaInput["field_keyvalue"].(map[string]interface{}))[k].(string))))

		case "text":
			name += " " + k + " = " + ((endpoint_common.ReturnString((metaInput["field_keyvalue"].(map[string]interface{}))[k].(string)))) + ","
			// value = ((endpoint_common.ReturnString((metaInput["field_keyvalue"].(map[string]interface{}))[k].(string))))

		case "set<text>":
			name += " " + k + " = " + ((endpoint_common.ReturnSetTextPG((metaInput["field_keyvalue"].(map[string]interface{}))[k]))) + ","
		// value = ((endpoint_common.ReturnString((metaInput["field_keyvalue"].(map[string]interface{}))[k].(string))))

		case "timestamp":
			if len((metaInput["field_keyvalue"].(map[string]interface{}))[k].(string)) > 0 {
				name += " " + k + " = " + ((endpoint_common.ReturnString((metaInput["field_keyvalue"].(map[string]interface{}))[k].(string)))) + ","
			} else {
				name += " " + k + " = " + " NULL,"
			}

		}


		// query+=","
	}

	name = strings.Trim(name,",")


	if  metaInput["is_post_update"].(bool) == true {

	for curKey, curVal := range metaInput["updateCondition"].(map[string][]string) {

		where += " " + curKey + " = '" + curVal[0] + "'  AND";
	}

		where = " WHERE " + strings.Trim(where, "AND")

	} else {


		tmpWhere, isOk := ReturnWhereComplex(metaInput["ComplexQuery"].(string), table, "postgresxl")


		countStr := len(strings.Replace(strings.Replace(tmpWhere, "(", "", -1), ")", "", -1))

		if countStr == 0 { isOk = false }

		if isOk {

			fmt.Println( "Length of tmpWhere ", len(tmpWhere), tmpWhere)

			where += " WHERE " + tmpWhere
		} else {

			return []string{}, false
		}
	}


	query="UPDATE " + table + " SET " + name + where

	query+=";"
	logger.Write("INFO", "Query is " + query)
	return []string{ query }, isOk
}

func DeleteQueryBuilder(metaInput map[string]interface{})  string {
     var query string
	   name:=""
           value:=""
	   database:=metaInput["database"].(string)
	   table:=metaInput["table"].(string)
	   query+="DELETE FROM"+" "+database+"."+table
         for k,v:=range metaInput["field_keymeta"].(map[string]interface{}){
		 name+=k
		 query+=" "
		 query+="WHERE"
		 query+=" "
		 switch datatype:=v.(string); datatype {
		 case "int":
			value+=((endpoint_common.ReturnString((metaInput["field_keyvalue"].(map[string]interface{}))[k].(string))))
		 }
                 query+=" "
		 query+=name+"="+value
		 query+=";"
	   }
         query=strings.Trim(query," ")
	 //fmt.Println(query)
	return query
}

func InsertQueryBuild(metaInput map[string]interface{})  []string {

	name := ""
	value := ""

	var minor_queries []string
	//database := metaInput["database"].(string)
	for k, v := range metaInput["field_keymeta"].(map[string]interface{}) {

		name += (k + ",")
		value += " "
		switch s:=v.(string); s{

		case "int":
			value +=((endpoint_common.ReturnString((metaInput["field_keyvalue"].(map[string]interface{}))[k].(string))))

		case "text":
			value +=((endpoint_common.ReturnString((metaInput["field_keyvalue"].(map[string]interface{}))[k].(string))))

		case "timestamp":
			if len((metaInput["field_keyvalue"].(map[string]interface{}))[k].(string)) > 0 {
				value += ((endpoint_common.ReturnString((metaInput["field_keyvalue"].(map[string]interface{}))[k].(string))))
			} else {
				value += " NULL "
			}

		case "set<text>":
			value += ((endpoint_common.ReturnSetTextPG((metaInput["field_keyvalue"].(map[string]interface{}))[k])))
		// value = ((endpoint_common.ReturnString((metaInput["field_keyvalue"].(map[string]interface{}))[k].(string))))

		}
		value += ","
	}
	name = strings.Trim(name, ",")
	value = strings.Trim(value, ",")
	//main_query := "INSERT INTO " + database + "." + metaInput["table"].(string) + " ( " + name + ") VALUES (" + value + ")"
	main_query := "INSERT INTO " + metaInput["table"].(string) + " ( " + name + ") VALUES (" + value + ")"

	queries := append(minor_queries, main_query)
        //fmt.Print(queries)
	return queries

}

func SelectQueryBuild(metaInput map[string]interface{})  (string, bool) {

	table := metaInput["table"].(string)
	isOrCondition := metaInput["isOrCondition"].(bool)
	isOk := true

	myFields := utils.FindMapSelect("table", table, config.Metadata_insert(), metaInput["SelectFields"].([]string))

	var selectString string

	if len(myFields) != 0 {

//		tempStr, _ := json.Marshal(myFields)

		//logger.Write("INFO", "Results from myFields" + string(tempStr) + "Length" + string(len(myFields)))
		selectString = makeSelectPG(myFields)
	} else {

		selectString = "*"
		logger.Write("ERROR", "Postgres Query error, Couldn not find column information on the table from insert api file while trying to make the select query fields, is the entry put in? defaulting to *, but users will see table columns instead of expected field names.")
	}

	whereCondition := "AND"


	if isOrCondition {

		whereCondition = "OR"
	}


	query := "SELECT " + selectString  + " FROM"+" " + table

//	fields := metaInput["fields"].([]map[string]interface{})
//          if len(fields) > 0 {

		  query +=" "

		  tmpWhere, isOk := ReturnWhereComplex(metaInput["ComplexQuery"].(string), table, "postgresxl")

		  countStr := len(strings.Replace(strings.Replace(tmpWhere, "(", "", -1), ")", "", -1))

		  if isOk && countStr > 0 {

			  query +=" WHERE "
			  query += tmpWhere
		  }
		  /*

		  for _, v := range fields {

			  field := v // .(map[string]interface{})
			  query += " " + endpoint_common.ReturnCondition(field, whereCondition, "postgresxl") + " " + whereCondition
		  }
		  */

//	  } else {

		  query += ""
//	  }

	query=strings.Trim(query, whereCondition)
	query+=" LIMIT 20;"

	//fmt.Println("Postgres query on limit: ", query)
	return query, isOk
}


func makeSelect(fields map[string]interface{}) string {

	if reflect.TypeOf(fields).String() == "map[string]interface {}" {

		selects := []string{}

		for _, v := range fields["fields"].(map[string]interface{}) {


			isNotionalField := false

			if _, ok := v.(map[string]interface{})["is_notional_field"].(string); ok {

				if v.(map[string]interface{})["is_notional_field"].(string) == "true" {

					isNotionalField = true
				}
			}

			// Dont select notional fields...
			if isNotionalField { continue }

			// If you are taking select data
			//fmt.Println(v.(map[string]interface{})["column"], " as ", k)

			selects = append(selects, v.(map[string]interface{})["column"].(string) + " as \"" + v.(map[string]interface{})["name"].(string) + "\"")
		}

		return strings.Join(selects, ", ")
	} else {

		return "*"
	}

}

func makeSelectPG(fields map[string]interface{}) string {

	if reflect.TypeOf(fields).String() == "map[string]interface {}" {

		selects := []string{}

		for _, v := range fields["fields"].(map[string]interface{}) {


			isNotionalField := false

			if _, ok := v.(map[string]interface{})["is_notional_field"].(string); ok {

				if v.(map[string]interface{})["is_notional_field"].(string) == "true" {

					isNotionalField = true
				}
			}

			// Dont select notional fields...
			if isNotionalField { continue }

			// If you are taking select data
			//fmt.Println(v.(map[string]interface{})["column"], " as ", k)

			if v.(map[string]interface{})["type"].(string) == "set<text>" {

				selects = append(selects, "array_to_json(" + v.(map[string]interface{})["column"].(string) + ") as \"" + v.(map[string]interface{})["name"].(string) + "\"")
			} else {

				selects = append(selects, v.(map[string]interface{})["column"].(string) + " as \"" + v.(map[string]interface{})["name"].(string) + "\"")
			}





		}

		return strings.Join(selects, ", ")
	} else {

		return "*"
	}

}



