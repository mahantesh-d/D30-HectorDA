package postgresql_helper

import (
	/*"github.com/dminGod/D30-HectorDA/endpoint/endpoint_common"*/
	"strings"
	"github.com/dminGod/D30-HectorDA/endpoint/endpoint_common"
	"github.com/dminGod/D30-HectorDA/utils"
	"github.com/dminGod/D30-HectorDA/config"
	"reflect"
	"github.com/dminGod/D30-HectorDA/logger"
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


func UpdateQueryBuilder(metaInput map[string]interface{}) []string{

	var query string
	query = ""
	value := ""
	name := ""
	where := ""

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
		}
		// query+=","
	}

	name = strings.Trim(name,",")


	for curKey, curVal := range metaInput["updateCondition"].(map[string][]string) {

		where += " " + curKey + " = '" + curVal[0] + "'  AND";
	}

	where = strings.Trim(where, "AND")

	query="UPDATE " + table + " SET " + name + " WHERE " + where


	query+=";"
	//fmt.Println(query)
	return []string{ query }
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

func SelectQueryBuild(metaInput map[string]interface{})  string {

	table := metaInput["table"].(string)
	isOrCondition := metaInput["isOrCondition"].(bool)

	myFields := utils.FindMap("table", table, config.Metadata_insert())

	var selectString string

	if len(myFields) != 0 {

//		tempStr, _ := json.Marshal(myFields)

		//logger.Write("INFO", "Results from myFields" + string(tempStr) + "Length" + string(len(myFields)))
		selectString = makeSelect(myFields)
	} else {

		selectString = "*"
		logger.Write("ERROR", "Postgres Query error, Couldn not find column information on the table from insert api file while trying to make the select query fields, is the entry put in? defaulting to *, but users will see table columns instead of expected field names.")
	}

	whereCondition := "AND"


	if isOrCondition {

		whereCondition = "OR"
	}


	query := "SELECT " + selectString  + " FROM"+" " + table

	fields := metaInput["fields"].(map[string]interface{})
          if len(fields) > 0 {

		  query +=" "
		  query +="WHERE"

		  for _, v := range fields {
			  field := v.(map[string]interface{})
			  query += " " + endpoint_common.ReturnCondition(field, whereCondition) + " " + whereCondition
		  }

	  } else {

		  query += ""
	  }

	query=strings.Trim(query, whereCondition)
	query+=" LIMIT 20;"

	//fmt.Println("Postgres query on limit: ", query)
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