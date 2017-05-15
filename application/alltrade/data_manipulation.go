package alltrade

import (
	"github.com/dminGod/D30-HectorDA/utils"
	"time"
	"github.com/dminGod/D30-HectorDA/model"
)

func mapRecord(v map[string]interface{}, curRecord *map[string]interface{}) {

	// Loop through all fields of the record
	for kk, vv := range v { (*curRecord)[kk] = vv }
}

func manipulateData(dbAbs model.DBAbstract, curRecord *map[string]interface{}) {

		// Loop through all fields of the record
		for kk, vv := range *curRecord {

			// Get the type, given a table name and a field name
			columnDetails := utils.GetColumnDetails( dbAbs.TableName, kk )

//			fmt.Println("Got column details ", columnDetails)

			if len(columnDetails) > 0 {
				columnType := columnDetails["type"].(string)
				columnTags := columnDetails["tags"].([]interface{})

				if columnType == "timestamp" {


					if ! vv.(time.Time).IsZero() {
						loc, _ := time.LoadLocation("Asia/Bangkok")
						vv = vv.(time.Time).In(loc).Format("20060102150405-0700")

						(*curRecord)[kk] = vv
					} else {

						vv = vv.(time.Time).Format("20060102150405-0700")
						(*curRecord)[kk] = vv
					}

				}

				if len(columnTags) > 0 && columnTags[0] == "json_array" {

					var jsonArray []map[string]interface{}

					for _, vvv := range vv.([]string) {

						jsonArray = append(jsonArray, utils.DecodeJSON(vvv))
					}
					
					(*curRecord)[kk] = jsonArray
				}
			}
		}
}