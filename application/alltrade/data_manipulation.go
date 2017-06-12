package alltrade

import (
	"time"
	"github.com/dminGod/D30-HectorDA/model"
	"encoding/json"
	"strconv"
	"sync"
	"fmt"
)

func mapRecord(v map[string]interface{}, curRecord *map[string]interface{}) {

	// Loop through all fields of the record
	for kk, vv := range v { (*curRecord)[kk] = vv }
}

func manipulateData(dbAbs model.DBAbstract, curRecord map[string]interface{}, fieldsConfig map[string]interface{}, retData *[]map[string]interface{}, wg *sync.WaitGroup) {

			// Loop through all fields of the record
			for kk, vv := range curRecord {

				// Get the type, given a table name and a field name
//				columnDetails := utils.GetColumnDetails(dbAbs.TableName, kk)
				columnDetails := fieldsConfig[kk].(map[string]interface{})

				if len(columnDetails) > 0 {
					columnType := columnDetails["type"].(string)
					columnTags := columnDetails["tags"].([]interface{})


					if columnType == "timestamp" {

						if _, ok := vv.(time.Time); ok {
							if ! vv.(time.Time).IsZero() {
								loc, _ := time.LoadLocation("Asia/Bangkok")
								vv = vv.(time.Time).In(loc).Format("20060102150405-0700")

								(curRecord)[kk] = vv
							} else {

								vv = vv.(time.Time).Format("20060102150405-0700")
								(curRecord)[kk] = vv
							}
						} else {
							(curRecord)[kk] = ""
						}
					}

					if columnType == "int" {

						if vv != nil {

							(curRecord)[kk] = strconv.Itoa(int(vv.(int64)))
						}
					}

					if dbAbs.DBType == "postgresxl" && columnType == "set<text>" {

						if vv != nil {

							var payload interface{}

							err := json.Unmarshal(vv.([]byte), &payload)

							if err == nil {

								(curRecord)[kk] = payload
							}

						} else {

							(curRecord)[kk] = []string{}
						}
					}

					fmt.Println(columnTags)


					if len(columnTags) > 0 && columnTags[0] == "json_array" {

						var jsonArray []map[string]interface{}

						if _, ok := vv.([]string); ok {

							for _, vvv := range vv.([]string) {

								var payload map[string]interface{}

								err := json.Unmarshal([]byte(vvv), &payload)

								if err == nil {

									jsonArray = append(jsonArray, payload)
								}
							}

							(curRecord)[kk] = jsonArray

						}

						if _, ok := vv.([]uint8); ok {

							var payload interface{}

							tmpStr := string(vv.([]uint8))


							err2 := json.Unmarshal([]byte(tmpStr), &payload)

							if err2 != nil {
								fmt.Println("Error is...", err2.Error())
							}

							var retObj []interface{}

							for _, kkk := range payload.([]interface{}) {

								var tmpInterface interface{}

								json.Unmarshal([]byte(kkk.(string)), &tmpInterface)

								retObj = append(retObj, tmpInterface)
							}

							fmt.Println("retObj for return is", retObj)

							(curRecord)[kk] = retObj

							fmt.Println(curRecord)

						}

					}

				}
				if vv == nil {

					columnType := columnDetails["type"].(string)

					if columnType == "set<text>" {

						(curRecord)[kk] = []string{}
					} else {

						(curRecord)[kk] = ""
					}

				}
			}

		*retData = append(*retData, curRecord)
		wg.Done()
}