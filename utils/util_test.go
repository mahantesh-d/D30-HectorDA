package utils

import (
	"encoding/json"
	"fmt"
	"github.com/dminGod/D30-HectorDA/logger"
	"io/ioutil"
	"regexp"
	"strings"
)

func ExampleIsJSON(input interface{}) bool {
	example := `
		{
		"foo": "bar"
		}
		`
	fmt.Println(IsJSON(example)) // true
}

func ExampleDecodeJSON(input interface{}) map[string]interface{} {
	example := `
		{
		"foo": "bar"
		}
		`
	fmt.Println(DecodeJSON(example)) // map[foo: bar]
}

func ExampleEncodeJSON(input interface{}) string {
	example := make(map[string]interface{})
	example["foo"] = "bar"
	fmt.Println(EncodeJSON(example)) // {"foo": "bar"}
}

func ExampleKeyInMap(key string, attributes map[string]interface{}) bool {
	example := make([]map[string]interface{})
	example["fookey"] = "foovalue"
	example["barkey"] = "barvalue"
	fmt.Println(KeyInMap("fookey"))       // true
	fmt.Println(KeyInMap("someotherkey")) // false
}

func ExampleParseFilter(input string) map[string]string {
	example := "((foo=bar))"
	fmt.Println(example) // map[foo: bar]
}

func ExampleRegexMatch(input string, pattern string) bool {
	exampleString = "abc"
	examplePattern = `[0-9]+`
	fmt.Println(RegexMatch(exampleString, examplePattern)) // true
}
