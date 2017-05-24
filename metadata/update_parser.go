package metadata

import (
	"strings"
	"fmt"
	"reflect"
	"github.com/dminGod/D30-HectorDA/logger"
)

// Condition
func CheckCondition(metadata map[string]interface{}, payload map[string]interface{}) (bool, map[string][]string)  {


	var updateProblem bool
	isRequirementArray := false
	var loopOver []map[string]interface{}

	if _, ok := metadata["updateCondition"].(string); !ok {

		updateProblem = true
	}

	if _, ok := metadata["updateKeys"].(string); !ok {

		updateProblem = true
	}

	fmt.Println("This is the type : ", reflect.TypeOf(metadata["update_list_array"]))

	if _, ok := metadata["update_list_array"].([]interface{}); ok {

		isRequirementArray = true
		updateProblem = false
//		fmt.Println("This is is the array I am getting...", metadata["update_list_array"].([]map[string]interface{}))

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

			fmt.Println("My updateCondition is ", curRec["updateCondition"])
			fmt.Println("My updateKeys are", curRec["updateKeys"])

			condition := curRec["updateCondition"].(string)
			updateKeys := curRec["updateKeys"].(string)

			splits, valid := checkConditionsAndMassage(condition)

			// Conditions are valid
			if valid {

				fmt.Println("Splits", splits)

				shouldTryUpdate := checkConditionsPayload(splits, payload)

				if !shouldTryUpdate {

					fmt.Println("Recommend not to try update...", shouldTryUpdate)
					// return false, map[string]string{}
					continue
				}

				updateKeysRet, updateKeysOk := checkGetUpdateKeys(updateKeys, payload)

				// Update Keys seem to be okay as well. Now we need to get the primary key for this record
				if updateKeysOk {

					fmt.Println("Returning okay for update keys -- should try updates", shouldTryUpdate, "Update keys", updateKeysRet)
					return shouldTryUpdate, updateKeysRet

				} else {

					fmt.Println("Returning false, updateKeys not okay")
					return false, map[string][]string{}
					continue
				}
			}

		}

	}

	fmt.Println("Did not get anything, going out.... ")
	return false, map[string][]string{}
}

// Check if the splits of conditions match with the passed payload.. this is the decider

func checkConditionsPayload(splits map[string]interface{}, payload map[string]interface{}) bool {

	var shouldTryUpdate bool

	// condition := splits["cond"].(string) // All the conditions without split
	isOrCondition := splits["isOrCondition"].(bool)
	fields := splits["fields"].([]map[string]interface{})

	if isOrCondition {

		shouldTryUpdate = checkOrCondition(fields, payload)
	} else {

		shouldTryUpdate = checkAndCondition(fields, payload)
	}

	return shouldTryUpdate
}


func checkOrCondition(conditionPairs []map[string]interface{}, values map[string]interface{}) bool {

	orPasses := false

	// Loop over all conditions, if any passes send true
	for _, curField := range conditionPairs {

		if _, ok := curField["key"].(string); !ok {

			fmt.Println("ISSUE with this curFiled", curField)
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


func checkAndCondition(conditionPairs []map[string]interface{}, values map[string]interface{}) bool {

	andPasses := true

	for _, curField := range conditionPairs {

		if _, ok := curField["key"].(string); !ok {

			fmt.Println("ISSUE with this curFiled", curField)
			continue
		}

		// If we dont get it, then surely not passing
		if _, ok := values[ curField["key"].(string) ].(string); !ok {

			andPasses = false
		} else {

			// It passes, but if this is an equals then we need to check and
			// fail them if equals does not pass, otherwise we're considering true anyways

			if curField["isEqualCheck"].(bool) {


				fmt.Println( "Val1 -->", values[ curField["key"].(string) ].(string), "'")
				fmt.Println( "Val2 -->", curField["value"].(string), "'")

				if values[ curField["key"].(string) ].(string) != curField["value"].(string) {

					andPasses = false
				} else {

					fmt.Println("Both are equal")
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

				// Not supporting stuff other than string coming in
				if _, ok := vv.(string); !ok {

					fmt.Println("Continuing, Type of vv got is", reflect.TypeOf(vv), " -- vv value :", vv)
					continue
				}

				matchFound = true;
				retKeyVal[ keyPairs[1] ] = []string{ vv.(string) }
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

		fmt.Println("Condition, " + condition + " does not contain ~ - Failing")
		return map[string]interface{}{}, false
	}

	if !strings.Contains(condition, "=") && !strings.Contains(condition, "^") {

		fmt.Println("Condition, values " + condition + " does not contain = or ^ - Failing")
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

		fmt.Println("ERROR, Field is invalid" + conditions)
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

		fmt.Println("ERROR, not &, Not |")
	}

	return false
}
