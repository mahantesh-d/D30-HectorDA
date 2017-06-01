package metadata

import (
"strings"
"math/rand"
"strconv"
"fmt"
"os"
"regexp"
	"github.com/dminGod/D30-HectorDA/endpoint/endpoint_common"
	"github.com/dminGod/D30-HectorDA/utils"
	"github.com/dminGod/D30-HectorDA/config"
)

type Pr struct {

						   // Parser
	CurPos           int              // Parser current postion
	CurLevel         int              // Present level that we are on
	CurrentId        string           // ID of the element being worked on
	CurrentCondition string
	CurrentElementType  string
	IncNumber        int              // Incremental number that tracks the element number

	ParsedString     string           // Parsed string


						   // All the Level# stuff is transient stuff except for LevelDict

	LevelCounter     map[int]int      // Level Counter, Keeps record of what the numbers were in the previous
						   // levels so when you come out of the level into a lower level the
						   // numbers are correct

	LevelIdTracker   map[int]string   // This is the same as before but instead of storing a running counter
						   // it saves a string with the ID of the that record.

	PrevParentHash  map[int]string    // This is the Level # to ID hash, same as LevelIdTracker (??)
	LevelCondition  map[string]string 	  // Condition tracked by Level#
	LevelDict       map[int][]string  // Level# to all the strings that are on that level
	LastLevel       int               // Last level that was worked on
	MaxLevels       int               // Maximum levels
	LastToLastLevel int

						   // Where all the elements will go
	Elements        []Element


}

// Sort Methods
func (p Pr) Len() int {

	return len(p.Elements)
}
func (p Pr) Less(i, j int) bool {

	return p.Elements[i].IncNumber < p.Elements[i].IncNumber;
}
func (p Pr) Swap(i, j int) {
	p.Elements[i].IncNumber, p.Elements[j].IncNumber = p.Elements[j].IncNumber, p.Elements[i].IncNumber
}

type Element struct {

	UniqueId		string		// Unique Id for this
	IncNumber 		int		// This is the incremental number of the element
	ElementType		string  	// Is this parent (container element) or leaf element
	Level 			int		// Numeric level of the element
	SubLevel		int		// Sublevel of the element
	ParentId 		string		// UniqueId of the parent, 0 if this is root
	ParentCondition 	string		// Will be and, or


	// Will be used for Leaf elements
	Key			string		// Key of the element sent
	Value  			string		// Value of the element sent
	Operator		string		// will be =, <, >, like etc
	DatabaseFieldType       string		// What is the field type in the database for this (maybe used later)

	// Will be used for container elements
	HasChildren		bool
	Condition 		string
}

// Add Ids to Levels
func (p *Pr) LevelDictAdd(level int, str string) {

	// Check if it already exists, exit, dont want duplicates
	for _, v := range p.LevelDict[ level ] {   if v == str {   return  }    }

	// Add to Dictionary
	p.LevelDict[ level ] = append( p.LevelDict[ level ],  str )
}

// This is init for now
func (p *Pr) SetString(s string) {

	//fmt.Println("Setting string....")
	p.LevelCounter = make(map[int]int)
	p.LevelIdTracker = make(map[int]string)
	p.LevelDict = make(map[int][]string)
	p.LevelCondition = make(map[string]string)
	p.PrevParentHash = make(map[int]string)

	p.IncNumber = 0

	p.ParsedString =  s
}

// Start of a new level and element
func (p *Pr) IncreaseLevel() {

	p.CurLevel += 1
	p.IncNumber += 1

	prevParentId := ""
	CurrentId := makeAnID()
	p.CurrentId = CurrentId

	// For the present level set the ID and set it as per level so it can be used by its future children
	p.PrevParentHash[ p.CurLevel  ] = CurrentId

	// Set the parent HashId if present -- This is going to change as we keep moving forward but because we came from a lower level
	// to this, that's why we can take the previous level just by the integer.
	if _, ok := p.PrevParentHash[ p.CurLevel - 1  ]; ok {

		// If Prev parent hash is available, then use that as the makeId for LevelIdTracker
		prevParentId = p.PrevParentHash[ p.CurLevel - 1  ]
	}

	// Add to Level tracker and to the Level Dict
	p.LevelIdTracker[ p.CurLevel ] = CurrentId
	p.LevelDictAdd( p.CurLevel, CurrentId )

	// Means we are moving into a new level, the sublevels will start from 1
	if p.LastLevel < p.CurLevel {

		p.LevelCounter[ p.CurLevel ]  = 1
	} else {

		p.LevelCounter[ p.CurLevel ]  += 1
	}

	l1 := p.PrevParentHash[ p.CurLevel - 1 ]
	l2 := p.PrevParentHash[ p.CurLevel - 2 ]
	l3 := p.PrevParentHash[ p.CurLevel - 3 ]
	l4 := p.PrevParentHash[ p.CurLevel - 4 ]

	VOldParentId := ""

	if len(l4) > 0 { VOldParentId = l4 }
	if len(l3) > 0 { VOldParentId = l3 }
	if len(l2) > 0 { VOldParentId = l2 }
	if len(l1) > 0 { VOldParentId = l1 }


	// We can add Elements from here for now...
	e := Element{

		ElementType: "node", // Dont know as of now cause we havent seen the inside of this thing as yet..

		// Correct
		UniqueId: CurrentId,
		IncNumber: p.IncNumber,
		ParentId: prevParentId,
		Level: p.CurLevel,
		SubLevel: p.LevelCounter[ p.CurLevel ],
		ParentCondition: p.LevelCondition[ VOldParentId ],
	}

	p.Elements = append(p.Elements, e)

	//	fmt.Println("I am on level", p.CurLevel, "Sublevel ", p.LevelCounter[ p.CurLevel ], "My ID is : ", p.PrevParentHash[ p.CurLevel  ],"Parent ID is :", p.LevelIdTracker[ p.CurLevel - 1 ])

	p.LastToLastLevel = p.LastLevel
	p.LastLevel = p.CurLevel


	if p.MaxLevels < p.CurLevel {

		p.MaxLevels  = p.CurLevel
	}
}

func (p *Pr) DecreaseLevel() {

	p.LevelCondition[ p.CurrentId ] = ""

	p.LastLevel = p.CurLevel
	p.CurLevel -= 1
}

func (p *Pr) SetCondition(condition string) {

	curLevel := p.PrevParentHash[ p.CurLevel ]

	p.LevelCondition[ curLevel ] = condition

	p.CurrentCondition = condition

	//	fmt.Println("setting condition for level : ", p.PrevParentHash[ p.CurLevel  ], " Condition : ", condition)
}

func (p *Pr) updateValueByUID( key string, value string ) {

	for i, e := range p.Elements {

		if e.UniqueId == p.CurrentId {

			myElem := &p.Elements[i]

			switch key {

			case "condition":
				myElem.Condition = value
				break

			case "key_value":

				tmpSplit := strings.Split(value, "=")

				if len(tmpSplit) > 1 {
					myElem.Key = tmpSplit[0]
					myElem.Value = tmpSplit[1]
					myElem.ElementType = "leaf"
				}
				break

			case "type":
				myElem.ElementType = value
			}
		}
	}
}

func (p *Pr) MakeString(table_name string, dbType string) string {

	curLevel := 0
	MyString := ""

	LoopPrevCondition := ""

//	output := make(map[string]interface{})
	input := utils.FindMap("table", table_name, config.Metadata_get())
	fields := input["fields"].(map[string]interface{})


	for _, v := range p.Elements {

		//fmt.Println("IncNumber", v.IncNumber, "Level", v.Level)

		Condition := ""

		if len(LoopPrevCondition) > 0 {  Condition = LoopPrevCondition }
		if len(v.Condition) > 0 {  Condition = v.Condition }
		if len(v.ParentCondition) > 0 {  Condition = v.ParentCondition }

		//fmt.Println("My Condition", v.Condition, "Parent C", v.ParentCondition, "Passed condition", Condition)

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

				//fmt.Println(cond , "(")
				MyString +=   "("

			} else if curLevel > v.Level {

				for i := 0; i < (curLevel - v.Level) - 1; i++ {


					MyString += ")"
					//fmt.Println(")")
				}

				cond := ""

				if Condition == "&" { cond = "AND" }
				if Condition == "|" { cond = "OR" }

				MyString += ")" + cond

				//fmt.Println("Coming here )" , cond)

			} else if curLevel == v.Level {

				//fmt.Println("Coming here ==", Condition, MyString)

				cond := ""

				if Condition == "&" { cond = "AND" }
				if Condition == "|" { cond = "OR" }


				MyString +=  "" + cond

				//fmt.Println("Coming here ==", Condition, MyString)
			}


		} else {

			//fmt.Println("Key", v.Key, "Value", v.Value, "Condition", v.Condition)

			cond := ""
			if Condition == "&" { cond = "AND" }
			if Condition == "|" { cond = "OR" }

			if utils.ValueInMapSelect(v.Key, fields) {

				fieldData := utils.GetFieldByName(v.Key, fields)

				if curLevel == v.Level {

					MyString += "( " + endpoint_common.ReturnConditionKVComplex(fieldData, v.Value, dbType) + ")"

					//MyString += "( " + v.Key + " = " +v.Value + ")"
				} else {

					MyString += "( " + endpoint_common.ReturnConditionKVComplex(fieldData, v.Value,dbType) + ")" + cond
					//MyString += "( " + v.Key + " = " +v.Value + ")" + cond
				}

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

	fmt.Println("MyString-------------", MyString)

	return MyString
}

func AddElement(p *Pr, kv string) {

	keyVal := strings.Split(kv, "=")


	if len(keyVal) > 1 {

		p.IncNumber += 1
		CurrentId := makeAnID()
		p.CurrentId = CurrentId

		parentId := p.PrevParentHash[ p.CurLevel  ]

		// Add to Level tracker and to the Level Dict
		p.LevelIdTracker[ p.CurLevel ] = CurrentId
		p.LevelDictAdd( p.CurLevel, CurrentId )


		key := keyVal[0]
		val := keyVal[1]


		p.Elements = append(p.Elements, Element{
			Key: key,
			Value: val,
			Condition: p.CurrentCondition,
			UniqueId: CurrentId,
			IncNumber: p.IncNumber,
			Level: p.CurLevel,
			HasChildren: false,
			ParentId: parentId,
		})
	}
}

func (p *Pr) MoveForward(){

	p.CurPos += 1
}

func (p *Pr) MoveBack() {

	p.CurPos -= 1
}

func (p *Pr) GetCurr() string {

	return string( p.ParsedString[ p.CurPos ] )
}

func (p *Pr) MoveGet() string {

	if p.CurPos != 0 && p.CurPos != (p.Size() - 1) {
		p.MoveForward()
	}

	retStr := string( p.ParsedString[ p.CurPos ] )

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
			printWordByLevel( *p, p.GetCurr() )

		}

		if mat, _ := regexp.MatchString("[a-zA-Z=]", p.GetCurr()); mat {

			tmpStr := ""

			for {
				if mat, _ := regexp.MatchString(`[a-zA-Z_0-9\-\=]`, p.GetCurr()); mat {

					tmpStr += p.GetCurr()
					p.MoveForward()
				} else {

					p.MoveBack()
					break
				}
			}

			AddElement(p, tmpStr )
			printWordByLevel( *p, tmpStr)

			//		p.updateValueByUID("key_value", tmpStr)
		}

		p.MoveForward()

		if p.CurPos == p.Size() {  break;  }
	}
}

func (p *Pr) Size() int {

	return len( p.ParsedString)
}

func (p *Pr) Validate() bool {

	countOpen := strings.Count(p.ParsedString, "(")
	countClose := strings.Count(p.ParsedString, ")")

	if countOpen != countClose {

		fmt.Println("Open and close counts do not match")
		os.Exit(1)
	}

	return true
}

func makeAnID() string {

	a := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	return string(a[ rand.Intn(52) ]) + string(a[ rand.Intn(52) ]) + string(a[ rand.Intn(52) ]) + strconv.Itoa(rand.Intn(100000) + 1) + strconv.Itoa(rand.Intn(100000) + 1)
}

func printByLevel(k Pr) {

	tabChar := "\t\t"
	showString := ""

	for i := 0; i < k.CurLevel; i++ {

		showString += tabChar
	}

	//	fmt.Println(showString + k.GetCurr())
}

func printWordByLevel(k Pr, word string) {

	tabChar := "\t\t"
	showString := ""

	for i := 0; i < k.CurLevel; i++ {

		showString += tabChar
	}

	//	fmt.Println(showString + word)
}




