package utils

import (
	"fmt"
)

func ExampleIsJSON(input interface{}) {
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

func ExampleEncodeJSON(input interface{}) {
	example := make(map[string]interface{})
	example["foo"] = "bar"
	fmt.Println(EncodeJSON(example)) // {"foo": "bar"}
}

func ExampleKeyInMap(key string, attributes map[string]interface{}) {
	example := make([]map[string]interface{})
	example["fookey"] = "foovalue"
	example["barkey"] = "barvalue"
	fmt.Println(KeyInMap("fookey", example))       // true
	fmt.Println(KeyInMap("someotherkey", example)) // false
}

func ExampleParseFilter(input string) {
	example := "((foo=bar))"
	fmt.Println(example) // map[foo: bar]
}

func ExampleRegexMatch(input string, pattern string) {
	exampleString := "abc"
	examplePattern := `[0-9]+`
	fmt.Println(RegexMatch(exampleString, examplePattern)) // true
}
