package presto_helper

import (
	_ "github.com/avct/prestgo"
	"github.com/dminGod/D30-HectorDA/endpoint/endpoint_common"
	"strings"
	"github.com/dminGod/D30-HectorDA/logger"
)

func SelectQueryBuild(metaInput map[string]interface{}) string {

	table := metaInput["table"].(string)

	query := "SELECT * from " + table


	whereCondition := "AND"

	if metaInput["isOrCondition"].(bool) {
		whereCondition = "OR"
	}

	fields := metaInput["fields"].(map[string]interface{})

	for _, v := range fields {
		fieldMeta := v.(map[string]interface{})
		query += endpoint_common.ReturnCondition(fieldMeta, whereCondition) + " AND"
	}

	query = strings.Trim(query, "AND")

	return query
}

func FindIDQueryBuild(metaInput map[string]interface{}) string {

	table := metaInput["table"].(string)
	// ct_prefix := metaInput["child_table_prefix"].(string)
	ct_prefix := ""


	pk := table + "_pk"

	q_select := ""
	q_from := ""
	q_join := ""
	q_where := ""

	// For this we only want the distinct PK
	q_select = "SELECT distinct(" + pk + ") from " + table

	fields := metaInput["fields"].(map[string]interface{})

	for _, v := range fields {
		fieldMeta := v.(map[string]interface{})

		// Make the where and joins
		returnConditions(fieldMeta, table, ct_prefix, &q_where, &q_join)
	}

	q_where = strings.Trim(q_where, "AND")

	if len(q_where) > 0 {
		q_where = " WHERE " + q_where
	}

	query := q_select + " " + q_from + " " + q_join + " " + q_where

	return query
}

func returnConditions(input map[string]interface{}, table string, ct_prefix string, q_where *string, q_join *string) {

	relationalOperator := "="

	switch dataType := input["type"]; dataType {

	case "text", "timestamp":

		for _, vv := range input["value"].([]string) {

			*q_where += " " + input["column"].(string) + " " + relationalOperator + " " + endpoint_common.ReturnString(vv) + " AND"
		}


	case "set<text>":

		// ct_name := ct_prefix + input["column"].(string)
		// *q_join += " LEFT JOIN " + ct_name + " ON " + ct_name + ".parent_pk = " + table + "." + table + "_pk "
		// *q_where += " " + ct_name + ".value " + relationalOperator + " " + endpoint_common.ReturnString(input["value"].(string)) + " AND"

	case "int":

		*q_where += " " + input["column"].(string) + " " + relationalOperator + " " + endpoint_common.ReturnInt(input["value"].(string)) + " AND"

	default:
		logger.Write("ERROR", "This is a problem, this datatype is not getting captured...." + dataType.(string))
	}

}
