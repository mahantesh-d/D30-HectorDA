package postgresql_helper

import (
	"fmt"
	/*"github.com/dminGod/D30-HectorDA/endpoint/endpoint_common"*/
	"strings"
	"github.com/dminGod/D30-HectorDA/endpoint/endpoint_common"
	"github.com/dminGod/D30-HectorDA/utils"
	"github.com/dminGod/D30-HectorDA/config"
	"reflect"
	"github.com/dminGod/D30-HectorDA/logger"
	"encoding/json"
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


func UpdateQueryBuilder(metaInput map[string]interface{}) string{
         var query string
	  query=""
	  value:=""
	  name :=""
	  value1:=""
	  name1:=""
	  database:=metaInput["database"].(string)
	  table:=metaInput["table"].(string)

	for k, v := range metaInput["field_keymeta"].(map[string]interface{}) {
		value +=""
		switch s:=v.(string); s{
		case "int":
			name1 += k
			value1 = ((endpoint_common.ReturnString((metaInput["field_keyvalue"].(map[string]interface{}))[k].(string))))
		case "text":
			name +=k
			value = ((endpoint_common.ReturnString((metaInput["field_keyvalue"].(map[string]interface{}))[k].(string))))
		}
		query+=","
	}
	query=strings.Trim(query,",")
	query="UPDATE"+" "+database+"."+table+" "+"SET"+" "+name+"="+value+" "+"WHERE"+" "+name1+"="+value1
	query+=";"
	fmt.Println(query)
	return query
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
	 fmt.Println(query)
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
			value +=((endpoint_common.ReturnString((metaInput["field_keyvalue"].(map[string]interface{}))[k].(string))))
		}
		value += ","
	}
	name = strings.Trim(name, ",")
	value = strings.Trim(value, ",")
	//main_query := "INSERT INTO " + database + "." + metaInput["table"].(string) + " ( " + name + ") VALUES (" + value + ")"
	main_query := "INSERT INTO " + metaInput["table"].(string) + " ( " + name + ") VALUES (" + value + ")"

	queries := append(minor_queries, main_query)
        fmt.Print(queries)
	return queries

}

func SelectQueryBuild(metaInput map[string]interface{})  string {

	table := metaInput["table"].(string)

	myFields := utils.FindMap("table", table, config.Metadata_insert)

	var selectString string

	if len(myFields) != 0 {

		tempStr, _ := json.Marshal(myFields)

		logger.Write("INFO", "Results from myFields" + string(tempStr) + "Length" + string(len(myFields)))
		selectString = makeSelect(myFields)
	} else {

		selectString = "*"
		logger.Write("ERROR", "Postgres Query error, Couldn not find column information on the table from insert api file while trying to make the select query fields, is the entry put in? defaulting to *, but users will see table columns instead of expected field names.")
	}





	query := "SELECT " + selectString  + " FROM"+" " + table

	fields := metaInput["fields"].(map[string]interface{})
          if len(fields)>0{
		  query +=" "
		  query +="WHERE"
		  for _,v:=range fields {
			  field := v.(map[string]interface{})
			  query += endpoint_common.ReturnCondition(field) + "" + "AND"
		  }
	  }else {
		  query += ""
	  }
	query=strings.Trim(query,"AND")
	query+=" LIMIT 200;"

	fmt.Println("Postgres query on limit: ", query)
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