package metadata

import (
	"github.com/dminGod/D30-HectorDA/config"
	"github.com/dminGod/D30-HectorDA/endpoint/endpoint_common"
	"github.com/dminGod/D30-HectorDA/utils"
	"math/rand"
	"regexp"
	"strconv"
	"strings"

	"github.com/dminGod/D30-HectorDA/logger"
)

type Pr struct {

	// Parser
	CurPos             int    // Parser current postion
	CurLevel           int    // Present level that we are on
	CurrentId          string // ID of the element being worked on
	CurrentCondition   string
	CurrentElementType string
	IncNumber          int // Incremental number that tracks the element number

	ParsedString string // Parsed string

	// All the Level# stuff is transient stuff except for LevelDict

	LevelCounter map[int]int // Level Counter, Keeps record of what the numbers were in the previous
	// levels so when you come out of the level into a lower level the
	// numbers are correct

	LevelIdTracker map[int]string // This is the same as before but instead of storing a running counter
	// it saves a string with the ID of the that record.

	PrevParentHash  map[int]string    // This is the Level # to ID hash, same as LevelIdTracker (??)
	LevelCondition  map[string]string // Condition tracked by Level#
	LevelDict       map[int][]string  // Level# to all the strings that are on that level
	LastLevel       int               // Last level that was worked on
	MaxLevels       int               // Maximum levels
	LastToLastLevel int

	// Where all the elements will go
	Elements []Element
}

// Sort Methods
func (p Pr) Len() int {

	return len(p.Elements)
}
func (p Pr) Less(i, j int) bool {

	return p.Elements[i].IncNumber < p.Elements[i].IncNumber
}
func (p Pr) Swap(i, j int) {
	p.Elements[i].IncNumber, p.Elements[j].IncNumber = p.Elements[j].IncNumber, p.Elements[i].IncNumber
}

type Element struct {
	UniqueId        string // Unique Id for this
	IncNumber       int    // This is the incremental number of the element
	ElementType     string // Is this parent (container element) or leaf element
	Level           int    // Numeric level of the element
	SubLevel        int    // Sublevel of the element
	ParentId        string // UniqueId of the parent, 0 if this is root
	ParentCondition string // Will be and, or

	// Will be used for Leaf elements
	Key               string // Key of the element sent
	Value             string // Value of the element sent
	Operator          string // will be =, <, >, like etc
	DatabaseFieldType string // What is the field type in the database for this (maybe used later)

	// Will be used for container elements
	HasChildren bool
	Condition   string
}

type FilterKeyVals struct {
	Key      string
	Value    string
	Operator string
}

// Add Ids to Levels
func (p *Pr) LevelDictAdd(level int, str string) {

	// Check if it already exists, exit, dont want duplicates
	for _, v := range p.LevelDict[level] {
		if v == str {
			return
		}
	}

	// Add to Dictionary
	p.LevelDict[level] = append(p.LevelDict[level], str)
}

// This is init for now
func (p *Pr) SetString(s string) {

	p.LevelCounter = make(map[int]int)
	p.LevelIdTracker = make(map[int]string)
	p.LevelDict = make(map[int][]string)
	p.LevelCondition = make(map[string]string)
	p.PrevParentHash = make(map[int]string)

	p.IncNumber = 0

	//p.ParsedString, _ =  url.QueryUnescape(s)

	p.ParsedString = s

}

// Start of a new level and element
func (p *Pr) IncreaseLevel() {

	p.CurLevel += 1
	p.IncNumber += 1

	prevParentId := ""
	CurrentId := makeAnID()
	p.CurrentId = CurrentId

	// For the present level set the ID and set it as per level so it can be used by its future children
	p.PrevParentHash[p.CurLevel] = CurrentId

	// Set the parent HashId if present -- This is going to change as we keep moving forward but because we came from a lower level
	// to this, that's why we can take the previous level just by the integer.
	if _, ok := p.PrevParentHash[p.CurLevel-1]; ok {

		// If Prev parent hash is available, then use that as the makeId for LevelIdTracker
		prevParentId = p.PrevParentHash[p.CurLevel-1]
	}

	// Add to Level tracker and to the Level Dict
	p.LevelIdTracker[p.CurLevel] = CurrentId
	p.LevelDictAdd(p.CurLevel, CurrentId)

	// Means we are moving into a new level, the sublevels will start from 1
	if p.LastLevel < p.CurLevel {

		p.LevelCounter[p.CurLevel] = 1
	} else {

		p.LevelCounter[p.CurLevel] += 1
	}

	l1 := p.PrevParentHash[p.CurLevel-1]
	l2 := p.PrevParentHash[p.CurLevel-2]
	l3 := p.PrevParentHash[p.CurLevel-3]
	l4 := p.PrevParentHash[p.CurLevel-4]

	VOldParentId := ""

	if len(l4) > 0 {
		VOldParentId = l4
	}
	if len(l3) > 0 {
		VOldParentId = l3
	}
	if len(l2) > 0 {
		VOldParentId = l2
	}
	if len(l1) > 0 {
		VOldParentId = l1
	}

	// We can add Elements from here for now...
	e := Element{

		ElementType: "node", // Dont know as of now cause we havent seen the inside of this thing as yet..

		// Correct
		UniqueId:        CurrentId,
		IncNumber:       p.IncNumber,
		ParentId:        prevParentId,
		Level:           p.CurLevel,
		SubLevel:        p.LevelCounter[p.CurLevel],
		ParentCondition: p.LevelCondition[VOldParentId],
	}

	p.Elements = append(p.Elements, e)

	p.LastToLastLevel = p.LastLevel
	p.LastLevel = p.CurLevel

	if p.MaxLevels < p.CurLevel {

		p.MaxLevels = p.CurLevel
	}
}

func (p *Pr) DecreaseLevel() {

	p.LevelCondition[p.CurrentId] = ""

	p.LastLevel = p.CurLevel
	p.CurLevel -= 1
}

func (p *Pr) SetCondition(condition string) {

	curLevel := p.PrevParentHash[p.CurLevel]

	p.LevelCondition[curLevel] = condition

	p.CurrentCondition = condition

}

func (p *Pr) updateValueByUID(key string, value string) {

	for i, e := range p.Elements {

		if e.UniqueId == p.CurrentId {

			myElem := &p.Elements[i]

			switch key {

			case "condition":
				myElem.Condition = value
				break

			case "type":
				myElem.ElementType = value
			}
		}
	}
}

func (p *Pr) CheckKeyExists(key string) bool {

	retCond := false
	//	retVal := ""

	for _, v := range p.Elements {

		logger.Write("INFO", "Parser, CheckKeyExists for Update using POST. parsed Key "+v.Key+" checking key is "+key)

		if v.ElementType == "leaf" && v.Key == key {

			retCond = true
			//			retVal = v.Value
		}
	}

	return retCond
}

func (p *Pr) GetKeyVals() (FilterKeyVals) {

	var retKV FilterKeyVals

	for _, v := range p.Elements {

		if v.ElementType == "leaf" {

			retKV.Key = v.Key
			retKV.Value = v.Value

			retKV.Operator = v.Operator
		}
	}

	return retKV
}

func (p *Pr) MakeString(table_name string, dbType string) (string, bool) {

	curLevel := 0
	MyString := ""
	isOk := true

	LoopPrevCondition := ""

	//	output := make(map[string]interface{})
	input := utils.FindMap("table", table_name, config.Metadata_get())
	fields := input["fields"].(map[string]interface{})

	for _, v := range p.Elements {

		Condition := ""

		if v.ElementType == "bad" {

			isOk = false
			break
		}

		if len(LoopPrevCondition) > 0 {
			Condition = LoopPrevCondition
		}
		if len(v.Condition) > 0 {
			Condition = v.Condition
		}
		if len(v.ParentCondition) > 0 {
			Condition = v.ParentCondition
		}

		if v.ElementType == "node" {
			if curLevel < v.Level {

				//				cond := ""

				// Should have a condition and should not be in the start..
				//if len(Condition) > 0 && len(MyString) > 0 && string(MyString[len(MyString)-1:]) != "("{
				//
				//	if Condition == "&" { cond = "AND" }
				//	if Condition == "|" { cond = "OR" }
				//}

				LoopPrevCondition = Condition

				MyString += "("

			} else if curLevel > v.Level {

				for i := 0; i < (curLevel-v.Level)-1; i++ {

					MyString += ")"
				}

				cond := ""

				if Condition == "&" {
					cond = "AND"
				}
				if Condition == "|" {
					cond = "OR"
				}

				MyString += ")" + cond

			} else if curLevel == v.Level {

				cond := ""

				if Condition == "&" {
					cond = "AND"
				}
				if Condition == "|" {
					cond = "OR"
				}

				MyString += "" + cond

			}

		} else {

			cond := ""
			if Condition == "&" {
				cond = "AND"
			}
			if Condition == "|" {
				cond = "OR"
			}

			if utils.ValueInMapSelect(v.Key, fields) {

				fieldData := utils.GetFieldByName(v.Key, fields)

				if curLevel == v.Level {

					MyString += "( " + endpoint_common.ReturnConditionKVComplex(fieldData, v.Value, dbType, v.Operator) + ")"

					//MyString += "( " + v.Key + " = " +v.Value + ")"
				} else {

					MyString += "( " + endpoint_common.ReturnConditionKVComplex(fieldData, v.Value, dbType, v.Operator) + ")" + cond
					//MyString += "( " + v.Key + " = " +v.Value + ")" + cond

				}

			} else {

				isOk = false
			}
		}
		curLevel = v.Level
	}

	if curLevel > 0 {

		for i := 0; i < curLevel; i++ {

			MyString = strings.Trim(MyString, "AND")
			MyString = strings.Trim(MyString, "OR")
			MyString += ")"
		}
	}

	logger.Write("INFO", "Parser, MakeString return statement", MyString)

	return MyString, isOk
}

func AddElement(p *Pr, kv string) {

	//	keyVal := strings.Split(kv, "=")

	keyVal := []string{}
	listOfOperators := []string{"<=", ">=", "=>", "=<", "<", ">", "="}

	op := ""

	for _, operator := range listOfOperators {

		if strings.Contains(kv, operator) {

			if operator == "=" && strings.Contains(kv, "*") {

				vvv := strings.Replace(kv, "*", "%", -1)
				keyVal = strings.Split(vvv, operator)
				op = "like"
				break

			} else {

				keyVal = strings.Split(kv, operator)

				if operator == "=>" {
					operator = ">="
				}
				if operator == "=<" {
					operator = "<="
				}

				op = operator
				break
			}
		}
	}

	if len(keyVal) > 1 && len(strings.Trim(keyVal[1], " ")) > 0 {

		p.IncNumber += 1
		CurrentId := makeAnID()
		p.CurrentId = CurrentId

		parentId := p.PrevParentHash[p.CurLevel]

		// Add to Level tracker and to the Level Dict
		p.LevelIdTracker[p.CurLevel] = CurrentId
		p.LevelDictAdd(p.CurLevel, CurrentId)

		key := keyVal[0]
		val := keyVal[1]

		p.Elements = append(p.Elements, Element{
			Key:         key,
			Value:       val,
			Condition:   p.CurrentCondition,
			UniqueId:    CurrentId,
			IncNumber:   p.IncNumber,
			Level:       p.CurLevel,
			HasChildren: false,
			ParentId:    parentId,
			Operator:    op,
			ElementType: "leaf",
		})
	} else {

		if len(keyVal) > 0 {

			logger.Write("INFO", "Got bad element without value", keyVal[0])

			p.Elements = append(p.Elements, Element{
				ElementType: "bad",
			})
		} else {

			logger.Write("INFO", "Got bad element without value")

			p.Elements = append(p.Elements, Element{
				ElementType: "bad",
			})
		}
	}
}

func (p *Pr) MoveForward() {

	p.CurPos += 1
}

func (p *Pr) MoveBack() {

	p.CurPos -= 1
}

func (p *Pr) GetCurr() string {

	return string([]rune(p.ParsedString)[p.CurPos])
}

func (p *Pr) GetPrev() string {

	retStr := ""

	if p.CurPos > 0 {
		retStr = string([]rune(p.ParsedString)[p.CurPos-1])
	}

	return retStr
}

func (p *Pr) GetNext() string {

	retStr := ""

	if (p.CurPos + 1) < len([]rune(p.ParsedString)) {

		retStr = string([]rune(p.ParsedString)[p.CurPos+1])
	}

	return string(retStr)
}

func (p *Pr) MoveGet() string {

	if p.CurPos != 0 && p.CurPos != (p.Size()-1) {
		p.MoveForward()
	}

	retStr := string(p.ParsedString[p.CurPos])

	if p.CurPos == 0 {

		p.MoveForward()
	}

	return retStr
}

func (p *Pr) Parse() {

	for l := 0; l < p.Size(); l++ {

		if p.GetCurr() == ")" {

			p.DecreaseLevel()
			printByLevel(*p)
		}

		if p.GetCurr() == "(" {

			printByLevel(*p)
			p.IncreaseLevel()
		}

		if p.GetCurr() == "&" || p.GetCurr() == "|" {

			//			k.RegisterCondition( k.GetCurr() )
			p.SetCondition(p.GetCurr())

			p.updateValueByUID("condition", p.GetCurr())

			printWordByLevel(*p, p.GetCurr())

		}

		if mat, _ := regexp.MatchString(`[^\(\)&\|]`, p.GetCurr()); mat {

			tmpStr := ""

			for {
				if mat, _ := regexp.MatchString(`[^\(\)&\|]`, p.GetCurr()); mat || p.matchesAndInsideText() {
					tmpStr += p.GetCurr()
					p.MoveForward()
				} else {
					p.MoveBack()
					break
				}
			}

			AddElement(p, tmpStr)
			printWordByLevel(*p, tmpStr)

			//		p.updateValueByUID("key_value", tmpStr)
		}

		p.MoveForward()

		if p.CurPos == p.Size() {
			break
		}
	}
}

func (p *Pr) matchesAndInsideText() bool {

	text := p.GetPrev() + p.GetCurr() + p.GetNext()
	mat, _ := regexp.MatchString(`[\w \t_-]&[\w \t_-]`, text)
	return mat
}

func (p *Pr) Size() int {

	return len(p.ParsedString)
}

func (p *Pr) Validate() bool {

	countOpen := strings.Count(p.ParsedString, "(")
	countClose := strings.Count(p.ParsedString, ")")

	if countOpen != countClose {

		logger.Write("ERROR", "Parser, Validate open and close counts do not match")

	}

	return true
}

func makeAnID() string {

	a := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	return string(a[rand.Intn(52)]) + string(a[rand.Intn(52)]) + string(a[rand.Intn(52)]) + strconv.Itoa(rand.Intn(100000)+1) + strconv.Itoa(rand.Intn(100000)+1)
}

func printByLevel(k Pr) {

	tabChar := "\t\t"
	showString := ""

	for i := 0; i < k.CurLevel; i++ {

		showString += tabChar
	}

}

func printWordByLevel(k Pr, word string) {

	tabChar := "\t\t"
	showString := ""

	for i := 0; i < k.CurLevel; i++ {

		showString += tabChar
	}

}
