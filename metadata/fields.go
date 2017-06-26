package metadata

import (
	"github.com/dminGod/D30-HectorDA/config"
	"github.com/dminGod/D30-HectorDA/logger"
	"github.com/dminGod/D30-HectorDA/utils"
	"strings"
)

type APIs struct {
	AllApis []API
}

type API struct {
	DatabaseType       string
	DatabaseName       string
	Table              string
	APIName            string
	APITags            []string
	PrimaryKeysStrings []string
	Fields             []Field
	PrimaryFields      []Field
	NonPrimaryFields   []Field
	PutSupported       bool
	DeleteSupported    bool
	InsertSupported    bool
}

type Field struct {
	TableName    string
	DatabaseName string
	DatabaseType string

	FieldName        string
	FieldType        string
	ColumnName       string
	ValueType        string
	FieldTags        []string
	IsPutField       bool
	IsPutFilterField bool
	IsFilterField    bool
	IsGetField       bool
	IsMultiValue     bool
	IsPrimaryKey     bool

	IsNotionalField  bool
	NotionalOperator string

	IsInternalField bool

	Value interface{} // This is used only to send values back.. never gets filled for the normal stuff
}

func (apis *APIs) GetByAPIName(api_name string) (API, bool) {

	var retAPI API
	retBool := false

	for _, api := range apis.AllApis {

		if api.APIName == api_name {

			retBool = true
			retAPI = api
		}
	}

	return retAPI, retBool
}

func (apis *APIs) GetByTableName(table_name string) (API, bool) {

	var retAPI API
	retBool := false

	for _, api := range apis.AllApis {

		if api.Table == table_name {

			retBool = true
			retAPI = api
		}
	}

	return retAPI, retBool
}

func (apis *APIs) Populate() bool {

	apis_data := config.Metadata_get()

	apis.AllApis = []API{}

	for _, api := range apis_data {

		curAPI := API{}

		curAPI.InsertSupported = false
		curAPI.DeleteSupported = false

		for api_field, api_value := range api.(map[string]interface{}) {

			switch api_field {

			case "databaseType":
				curAPI.DatabaseType = api_value.(string)

			case "database":
				curAPI.DatabaseName = api_value.(string)

			case "table":
				curAPI.Table = api_value.(string)

			case "apiName":
				curAPI.APIName = api_value.(string)

			case "tags":

				for _, tag := range api_value.([]interface{}) {

					switch tag.(string) {
					case "supports_delete":
						curAPI.DeleteSupported = true

					case "supports_insert":
						curAPI.InsertSupported = true
					}

					curAPI.APITags = append(curAPI.APITags, tag.(string))
				}

			case "primary_keys":
				curAPI.PrimaryKeysStrings = strings.Split(api_value.(string), ",")

			case "put_supported":

				curAPI.PutSupported = false
				if api_value == "true" {
					curAPI.PutSupported = true
				}

			case "fields":

				for _, table_fields := range api_value.(map[string]interface{}) {

					curField := Field{}

					curField.IsInternalField = false
					curField.IsPutField = false
					curField.IsPutFilterField = false
					curField.IsNotionalField = false
					curField.IsMultiValue = false

					curField.DatabaseName = curAPI.DatabaseName
					curField.TableName = curAPI.Table
					curField.DatabaseType = curAPI.DatabaseType

					for field_name, field_value := range table_fields.(map[string]interface{}) {

						switch field_name {

						case "name":
							curField.FieldName = field_value.(string)

						case "type":
							curField.FieldType = field_value.(string)

						case "column":
							curField.ColumnName = field_value.(string)

						case "valueType":
							curField.ValueType = field_value.(string)

							if field_value == "multi" {
								curField.IsMultiValue = true
							}

						case "is_put_field":
							if strings.ToLower(field_value.(string)) == "true" {

								curField.IsPutField = true
							}

						case "is_put_filter_field":
							if strings.ToLower(field_value.(string)) == "true" {

								curField.IsPutFilterField = true
							}

						case "tags":

							for _, ft := range field_value.([]interface{}) {

								switch ft.(string) {
								case "internal_field":
									curField.IsInternalField = true
								}

								curField.FieldTags = append(curField.FieldTags, ft.(string))
							}

						case "is_notional_field":

							if field_value == "true" {

								curField.IsNotionalField = true
							}

						case "is_get_field":
							curField.IsGetField = false
							if field_value == "true" {
								curField.IsGetField = true
							}
						}

						curField.IsPrimaryKey = apis.isPrimaryKey(curAPI.PrimaryKeysStrings, curField.FieldName)
					}

					// Add the field
					curAPI.Fields = append(curAPI.Fields, curField)

					if curField.IsPrimaryKey {

						curAPI.PrimaryFields = append(curAPI.PrimaryFields, curField)
					} else {

						curAPI.NonPrimaryFields = append(curAPI.NonPrimaryFields, curField)
					}
				}
			}

			apis.AllApis = append(apis.AllApis, curAPI)

		}
	}

	return len(apis.AllApis) > 0
}

func (apis *APIs) isPrimaryKey(pk_names_arr []string, name string) bool {

	var retBool bool

	for _, v := range pk_names_arr {

		if v == name {
			retBool = true
		}
	}

	return retBool
}

func (apis *APIs) GetFieldsByName(table_name string, field_names []string) []Field {

	curAPI, _ := apis.GetByTableName(table_name)
	var retFields []Field

	for _, v := range curAPI.Fields {

		for _, vv := range field_names {

			if vv == v.FieldName {

				retFields = append(retFields, v)
			}
		}
	}

	return retFields
}

func (apis *APIs) GetColumnNameByName(table_name string, name string) (string, bool) {

	var retStr string
	var retBool bool
	var gotDetails bool

	curAPI, gotDetails := apis.GetByTableName(table_name)

	if gotDetails {

		for _, field := range curAPI.Fields {

			if field.FieldName == name {

				retStr = field.ColumnName
				retBool = true
			}
		}
	}

	return retStr, retBool
}

func (apis *APIs) GetNameByeColumnName(table_name string, column_name string) (string, bool) {

	var retStr string
	var retBool bool
	var gotDetails bool

	curAPI, gotDetails := apis.GetByTableName(table_name)

	if gotDetails {

		for _, field := range curAPI.Fields {

			if field.FieldName == column_name {

				retStr = field.ColumnName
				retBool = true
			}
		}
	}

	return retStr, retBool
}

func (apis *APIs) GetPrimaryKeys(table_name string) ([]Field, bool) {

	var retField []Field
	var retBool bool
	var gotDetails bool

	curAPI, gotDetails := apis.GetByTableName(table_name)

	if gotDetails {

		pkFields := curAPI.PrimaryKeysStrings
		retField = apis.GetFieldsByName(table_name, pkFields)
		retBool = true

	}

	return retField, retBool
}

func (apis *APIs) GetNonPrimaryKeys(table_name string) ([]Field, bool) {

	var retField []Field
	var retBool bool
	var gotDetails bool

	curAPI, gotDetails := apis.GetByTableName(table_name)

	if gotDetails {

		retField = curAPI.NonPrimaryFields
		retBool = true
	}

	return retField, retBool
}

func (apis *APIs) GetAllPrimaryKeysWithValue(table_name string, payload map[string]interface{}) ([]Field, bool) {

	var retField []Field
	var retBool = true
	var gotDetails = true

	pkFields, gotDetails := apis.GetPrimaryKeys(table_name)

	logger.Write("INFO", "--->>>>Got Primary Keys for table ", pkFields, " table_name:", table_name)

	if gotDetails {

		for _, v := range pkFields {

			var curField Field

			if utils.KeyInMap(v.FieldName, payload) {

				curField = v
				curField.Value = payload[v.FieldName]

				retField = append(retField, curField)
			} else {

				retBool = false
			}
		}
	}

	return retField, retBool
}

func (apis *APIs) GetColumnsByTable(table_name string) ([]Field, bool) {

	var retFields []Field
	var retBool bool
	var gotDetails bool

	curAPI, gotDetails := apis.GetByTableName(table_name)

	if gotDetails {

		retFields = curAPI.Fields
		retBool = true
	}

	return retFields, retBool

}

func (apis *APIs) APIHasDeleteMethod(table_name string) bool {

	var retBool bool
	var gotDetails bool

	curAPI, gotDetails := apis.GetByTableName(table_name)

	if gotDetails {

		retBool = curAPI.DeleteSupported
	}

	return retBool
}

func (apis *APIs) APIHasInsertMethod(table_name string) bool {

	var retBool bool
	var gotDetails bool

	curAPI, gotDetails := apis.GetByTableName(table_name)

	if gotDetails {

		retBool = curAPI.InsertSupported
	}

	return retBool
}
