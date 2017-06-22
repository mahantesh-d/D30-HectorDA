package config

import (
	"sync"
	"encoding/json"
	"fmt"
	"io/ioutil"
)


func ValidateJSON(jsonFile string) []string {

	var listOfError []string

	//jsonData := readJsonFile(jsonFile)

	mapData := decodeJson(jsonFile)

	listOfError = checkJsonFieldsAndTag(mapData)

	if len(listOfError) > 0 {

		return listOfError
	}

	return listOfError
}

func isJsonValid(jsonFile string) bool {

	var jsonData map[string]string

	flag := false

	if err := json.Unmarshal([]byte(jsonFile), &jsonData); err != nil {

		flag = true

		return flag
	}

	return flag
}

func decodeJson(jsonData string) map[string]interface{} {

	var valideJson map[string]interface{}

	if !isJsonValid(jsonData) {

		fmt.Println("Problem with json file")
	}

	err := json.Unmarshal([]byte(jsonData), &valideJson)

	exceptionHandler(err)

	return valideJson
}

func readJsonFile(jsonFile string) string {

	jsonByteArray, err := ioutil.ReadFile(jsonFile)

	exceptionHandler(err)

	return string(jsonByteArray)
}

func exceptionHandler(err error) {

	if err != nil {

		fmt.Println(err.Error())
	}

}

func checkJsonFieldsAndTag(jsonMapData map[string]interface{}) []string {

	listOfError := []string{}

	var wg sync.WaitGroup

	for _, value := range jsonMapData {

		metaData := value.(map[string]interface{})

		wg.Add(1)

		go checkMandatoryFields(metaData, &listOfError, &wg)

		wg.Wait()
	}

	return listOfError
}

func checkMandatoryFields(metaData map[string]interface{}, Errors *[]string, wg *sync.WaitGroup) {

	var ListOfError string

	for fieldKey, fieldColumn := range metaData["fields"].(map[string]interface{}) {

		listOfFields := fieldColumn.(map[string]interface{})

		columnList := []string{"name", "type", "column", "tags", "valueType"}

		for _, v := range columnList {

			flag := false

			for kk, _ := range listOfFields {

				if v == kk {

					flag = true

					if v == "tags" {

						if _, ok := listOfFields[v].([]interface{}); !ok {

							ListOfError = "The Problem with  DataType the field name is  " + v + " it  must be array The column name is " + fieldKey + " and table name is " + metaData["table"].(string)

						}
					} else if v == "name" || v == "type" || v == "column" || v == "valueType" {

						if _, ok := listOfFields[v].(string); !ok {

							ListOfError = "The Problem with DataType The field name is  " + v + " it must be string The column name is " + fieldKey + " and table name is " + metaData["table"].(string)
						}
					}

				}
			}
			if !flag {

				ListOfError = "The Field name is  " + v + " not found in json file " + " The column name is " + fieldKey + " and The table name is " + metaData["table"].(string)
			}
		}

	}
	*Errors = append(*Errors, ListOfError)

	wg.Done()
}
