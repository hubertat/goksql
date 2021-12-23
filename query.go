package goksql

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/pkg/errors"
)

type Query struct {
	RestApi *RestKsql

	queryId string
	schema  string
}

func NewQuery(restApi *RestKsql) *Query {
	return &Query{RestApi: restApi}
}

func (qry *Query) LoadTable(table string, kindPtr interface{}, targetSlice interface{}) (err error) {

	if qry.RestApi == nil {
		err = errors.Errorf("ksql rest api not conifgured")
		return
	}
	if !qry.RestApi.IsReady() {
		err = errors.Errorf("ksql rest api not ready")
		return
	}

	names, _, err := getQueryPartial(kindPtr)
	if err != nil {
		err = errors.Wrapf(err, "get query partials failed (with table: %s)", table)
		return
	}

	queryString := fmt.Sprintf("SELECT %s FROM %s;", names, table)
	result, err := qry.RestApi.RunQuery(queryString)
	if err != nil {
		err = errors.Wrap(err, "failed to run query")
		return
	}
	if result.Error() != nil {
		err = errors.Wrap(err, "returned ksql result contains error")
		return
	}

	err = fillResultIntoSlice(result, targetSlice)
	if err != nil {
		err = errors.Wrap(err, "couldnt fill result into slice")
		return
	}
	return
}

func getQueryPartial(modelPtr interface{}) (names string, types string, err error) {
	val := reflect.ValueOf(modelPtr)

	if val.Kind() != reflect.Ptr {
		err = errors.Errorf("getQueryPartial received non pointer type (got %s)", val.Kind())
		return
	}

	structType := val.Type().Elem()
	if structType.Kind() != reflect.Struct {
		err = errors.Errorf("getQueryPartial received pointer to non-struct type (%s)", structType.Kind())
		return
	}

	fieldMap := map[string]reflect.Type{}
	fieldNames := []string{}
	fieldTypes := []string{}

	for _, field := range reflect.VisibleFields(structType) {
		if field.IsExported() {
			loName := strings.ToLower(field.Name)
			_, exist := fieldMap[loName]
			if exist {
				err = errors.Errorf("got the same name: %s twice (in lowercase) - struct can't have such in exported fields", loName)
				return
			}
			fieldMap[loName] = field.Type
			fieldNames = append(fieldNames, loName)
		}
	}

	if len(fieldMap) == 0 {
		err = errors.Errorf("received struct with zero exported fields")
		return
	}

	timeField := time.Time{}
	timeType := reflect.TypeOf(timeField)

	for _, name := range fieldNames {
		t, exist := fieldMap[name]
		if !exist {
			err = errors.Errorf("field named %s missing in fieldMap", name)
			return
		}
		switch t.Kind() {
		case reflect.Bool:
			fieldTypes = append(fieldTypes, "boolean")
		case reflect.String:
			fieldTypes = append(fieldTypes, "string")
		case reflect.Int32:
			fieldTypes = append(fieldTypes, "int")
		case reflect.Int64:
			fieldTypes = append(fieldTypes, "bigint")
		case reflect.Float64:
			fieldTypes = append(fieldTypes, "double")
		case timeType.Kind():
			fieldTypes = append(fieldTypes, "timestamp")
		default:
			err = errors.Errorf("unsupported field kind: %s", t.Name())
			return
		}
	}

	for ix, name := range fieldNames {
		types += fmt.Sprintf("%s %s", name, strings.ToUpper(fieldTypes[ix]))
		if ix+1 < len(fieldTypes) {
			types += ", "
		}
	}

	names = strings.Join(fieldNames, ", ")
	return
}

func fillResultIntoSlice(result *KsqlResult, targetSlice interface{}) error {
	val := reflect.ValueOf(targetSlice)
	if val.Kind() != reflect.Ptr {
		return errors.Errorf("expected pointer to slice, got: %s", val.Kind())
	}
	valElem := val.Elem()

	sliceType := val.Type().Elem()
	if sliceType.Kind() != reflect.Slice {
		return errors.Errorf("targetSlice is a pointer to non slice type (%s)", sliceType.Kind())
	}

	elemType := sliceType.Elem()
	if elemType.Kind() != reflect.Struct {
		return errors.Errorf("targetSlice is a pointer to slice of non structs (%s)", elemType.Kind())
	}

	_, rows := result.Get()
	for _, row := range rows {
		rowVal := reflect.ValueOf(row.Columns)
		if rowVal.Type().Kind() != reflect.Slice {
			return errors.Errorf("ksql result contains rows with non slice type (%s)", rowVal.Type().Kind())
		}
		if rowVal.Type().Elem().Kind() != reflect.Interface {
			return errors.Errorf("ksql result row has non interface (%s) type", rowVal.Type().Elem().Kind())
		}

		element := reflect.New(elemType)
		for ix, field := range reflect.VisibleFields(elemType) {
			if ix >= rowVal.Len() {
				return errors.Errorf("ksql result row struct insufficient len (%d) on index %d", rowVal.Len(), ix)
			}
			rowField := rowVal.Index(ix)
			if !rowField.IsValid() {
				return errors.Errorf("invalind row field index %d", ix)
			}
			if !rowField.IsNil() && !rowField.IsZero() {
				switch field.Type.Kind() {
				case reflect.Bool:
					reflect.Indirect(element).Field(ix).SetBool(rowField.Interface().(bool))
				case reflect.String:
					reflect.Indirect(element).Field(ix).SetString(rowField.Interface().(string))
				case reflect.Int32:
					reflect.Indirect(element).Field(ix).SetInt(rowField.Interface().(int64))
				case reflect.Int64:
					reflect.Indirect(element).Field(ix).SetInt(rowField.Interface().(int64))
				case reflect.Float64:
					reflect.Indirect(element).Field(ix).SetFloat(rowField.Interface().(float64))
				}
			}
		}
		valElem.Set(reflect.Append(valElem, reflect.Indirect(element)))
	}
	return nil
}
