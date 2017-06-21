package metadata

import (
	"strings"
	"reflect"
	"github.com/dminGod/D30-HectorDA/logger"
)

// Condition
func CheckCondition(metadata map[string]interface{}, payload map[string]interface{}, filters string) (bool, map[string][]string)  {


	var updateProblem bool
	isRequirementArray := false
	var loopOver []map[string]interface{}

	if _, ok := metadata["updateCondition"].(string); !ok {

		updateProblem = true
	}

	if _, ok := metadata["updateKeys"].(string); !ok {

		updateProblem = true
	}


	if _, ok := metadata["update_list_array"].([]interface{}); ok {

		isRequirementArray = true
		updateProblem = false


		for _, v := range metadata["update_list_array"].([]interface{}){

			if _, ok := v.(map[string]interface{}); ok {

				loopOver = append(loopOver, v.(map[string]interface{}))
			} else {

				logger.Write("ERROR", "Problem with JSON file, update_list_array is not map[string]interface inside, why? Check the json file for update_list_array for")
			}


		}
	}

	if updateProblem {

		return false, map[string][]string{}
	}

	if isRequirementArray == false {

		loopOver = []map[string]interface{}{{
			"updateCondition" : metadata["updateCondition"].(string),
			"updateKeys": metadata["updateKeys"].(string),
			}}
	}

	if len(loopOver) > 0 {

		for _, curRec := range loopOver {

			condition := curRec["updateCondition"].(string)
			updateKeys := curRec["updateKeys"].(string)

			splits, valid := checkConditionsAndMassage(condition)

			// Conditions are valid
			if valid {

				shouldTryUpdate := checkConditionsPayload(splits, payload, filters)

				if !shouldTryUpdate {

					logger.Write("INFO", "Recommend not to try update...", shouldTryUpdate)

					// return false, map[string]string{}
					continue
				}

				updateKeysRet, updateKeysOk := checkGetUpdateKeys(updateKeys, payload)

				// Update Keys seem to be okay as well. Now we need to get the primary key for this record
				if updateKeysOk {

					logger.Write("INFO", "Returning okay for update keys -- should try updates", shouldTryUpdate, "Update keys", updateKeysRet)
					return shouldTryUpdate, updateKeysRet

				} else {

					logger.Write("INFO", "Returning false, updateKeys not okay")
					return false, map[string][]string{}
					continue
				}
			}

		}

	}

	logger.Write("INFO", "Update parser, CheckCondition did not get anything return blank")
	return false, map[string][]string{}
}

// Check if the splits of conditions match with the passed payload.. this is the decider

func checkConditionsPayload(splits map[string]interface{}, payload map[string]interface{}, filters string) bool {

	var shouldTryUpdate bool

	// condition := splits["cond"].(string) // All the conditions without split
	isOrCondition := splits["isOrCondition"].(bool)
	fields := splits["fields"].([]map[string]interface{})

	if isOrCondition {

		// Or condition will not work with contains!
		// This is a new feature will need to be built!!

		shouldTryUpdate = checkOrCondition(fields, payload)
	} else {

		shouldTryUpdate = checkAndCondition(fields, payload, filters)
	}

	return shouldTryUpdate
}


func checkOrCondition(conditionPairs []map[string]interface{}, values map[string]interface{}) bool {

	orPasses := false

	// Loop over all conditions, if any passes send true
	for _, curField := range conditionPairs {

		if _, ok := curField["key"].(string); !ok {

			logger.Write("INFO", "update parser, checkOrCondition is expecting curField['key'] to be string Curfield is : ", curField, " The passed type is ", reflect.TypeOf(curField["key"]).String())
			continue
		}

		if _, ok := values[ curField["key"].(string) ].(string); ok {

			if curField["isEqualCheck"].(bool) {

				if _, okk := values[ curField["key"].(string) ].(string); okk {

						if values[ curField["key"].(string) ].(string) == curField["value"].(string) {

						orPasses = true
					}
				}
			} else {

				// value exists and we want to just do an exists check, so this is good
				orPasses = true
			}
		}
	}

	return orPasses
}


func checkAndCondition(conditionPairs []map[string]interface{}, values map[string]interface{}, filters string) bool {

	andPasses := true

	var Parser Pr

	if len(filters) > 0 {

		Parser.SetString(filters)
		Parser.Parse()
	}


	for _, curField := range conditionPairs {

		//if _, ok := curField["key"].(string); !ok {
		//
		//	logger.Write("INFO", "update parser, checkAndCondition is expecting curField['key'] to be string Curfield is : ", curField, " The passed type is ", reflect.TypeOf(curField["key"]).String())
		//	continue
		//}

		tmpPass := true
		tmpPass2 := true


		if _, ok := values[ curField["key"].(string) ].(string); !ok { tmpPass = false }
		if _, ok := values[ curField["key"].(string) ].([]interface{}); !ok { tmpPass2 = false }


		// If we dont get it, then surely not passing
		if tmpPass == false && tmpPass2 == false {

			logger.Write("ERROR", "Type of curfield is not string and array of interface, it is ", reflect.TypeOf( values[ curField["key"].(string) ]), "Key is", curField["key"].(string))
			andPasses = false
		} else {

			// It passes, but if this is an equals then we need to check and
			// fail them if equals does not pass, otherwise we're considering true anyways

			// This is a equals check field
			if curField["isEqualCheck"].(bool) && tmpPass == true {

				if values[ curField["key"].(string) ].(string) != curField["value"].(string) {

					andPasses = false
				} else {

					logger.Write("INFO", "Update parser, checkAndCondition both are equal key and val", curField["value"].(string))
				}
			}

			// This is a contains type field -- Contains type field will need to be pulled from the filter...
			// Parserbaba ki jai ho! Explicit assumption is made, if its contains it will come from URL
			if curField["isEqualCheck"].(bool) == false {

				logger.Write("INFO", "update_parser, checkAndCondition, Checking the URL for contains ", curField["key"].(string))

				// If key does not exit make this false
				if doesKeyExist := Parser.CheckKeyExists( curField["key"].(string) ); doesKeyExist == false {

					logger.Write("INFO", "update_parser, checkAndCondition, The key does not exist in the URL : ", curField["key"].(string))
					andPasses = false
				} else {

					logger.Write("INFO", "update_parser, checkAndCondition, The key exists in the URL : ", curField["key"].(string), " Good..")
				}
			}
		}
	}

	return andPasses
}

func checkGetUpdateKeys(updateKeys string, payload map[string]interface{}) (map[string][]string, bool) {

	updateKeysArr := strings.Split(updateKeys, ",")
	retKeyVal := make(map[string][]string)
	retUpdatesFound := true

	for _, v := range updateKeysArr {

		matchFound := false;
		keyPairs := strings.Split(v, "|")

		for kk, vv := range payload {

			if kk == keyPairs[0] {

				_, isStr := vv.(string);
				_, isArr := vv.([]interface{})

				// Not supporting stuff other than string coming in
				if !isStr && !isArr {

					logger.Write("INFO", "update_parser, checkGetUpdateKeys Continuing, Type of vv is not string or []interface it is is", reflect.TypeOf(vv), " -- vv value :", vv)
					continue
				}

				if isStr {

					logger.Write("INFO", "update_parser, checkGetUpdateKeys MatchFound is true", reflect.TypeOf(vv), " -- vv value :", vv)
					matchFound = true;
					retKeyVal[ keyPairs[1] ] = []string{ vv.(string) }
				}

				if isArr {

					logger.Write("INFO", "update_parser, checkGetUpdateKeys MatchFound Arr is true", reflect.TypeOf(vv), " -- vv value :", vv)

					if len(vv.([]interface{})) > 0 {

						if _, ok := vv.([]interface{})[0].(string); ok {

							matchFound = true;
							retKeyVal[ keyPairs[1] ] = []string{ vv.([]interface{})[0].(string) }
						}
					}
				}


			}
		}

		if matchFound == false {

			retUpdatesFound = false
		}
	}

	return retKeyVal, retUpdatesFound
}


func checkConditionsAndMassage(condition string) (map[string]interface{}, bool) {

	retVals := make(map[string]interface{})

	if ! strings.Contains(condition, "~") {

		logger.Write("ERROR", "update_parser, checkConditionsAndMassage Condition, " + condition + " does not contain ~ - Failing")

		return map[string]interface{}{}, false
	}

	if !strings.Contains(condition, "=") && !strings.Contains(condition, "^") {

		logger.Write("ERROR", "update_parser, checkConditionsAndMassage Condition, values " + condition + " does not contain = or ^ - Failing")
		return map[string]interface{}{}, false
	}

	splitCons := strings.Split(condition, "~")

	retVals["cond"] = splitCons[0]
	retVals["isOrCondition"] = isOrConditionCheck(retVals["cond"].(string))

	tmp_vals := strings.Split(splitCons[1], ",")

	loopVals := []map[string]interface{}{}

	for _, v := range tmp_vals {

		forVal, ok := checkMassageFields(v)

		if ok {
			loopVals = append(loopVals, forVal)
		}
	}

	retVals["fields"] = loopVals

	return retVals, true
}


func checkMassageFields(conditions string) (map[string]interface{}, bool) {

	hasCaret := strings.Contains(conditions, "^")
	hasEqual := strings.Contains(conditions, "=")

	retVal := make(map[string]interface{})

	if !hasCaret && !hasEqual {

		logger.Write("ERROR", "update_parser, checkMassageFields Field is invalid" + conditions)
		return map[string]interface{}{}, false
	}

	var tmpSplit []string

	if hasEqual {

		tmpSplit = strings.Split(conditions, "=")

		retVal["isEqualCheck"] = true
		retVal["key"] = tmpSplit[0]
		retVal["value"] = tmpSplit[1]

	} else {

		tmpSplit = strings.Split(conditions, "^")

		retVal["isEqualCheck"] = false
		retVal["key"] = tmpSplit[0]
		retVal["value"] = "isContainsCondition"
	}

	return retVal, true
}


func isOrConditionCheck(condition string) bool {

	condition = strings.Trim(condition, " ")

	if condition == "&" {

		return false
	} else if condition == "|" {

		return true
	} else {

		logger.Write("ERROR", "update_parser, isOrConditionCheck not &, Not |")
	}

	return false
}
