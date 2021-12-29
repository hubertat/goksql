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
	schema  map[string]KsqlField
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
	result, err := qry.RestApi.RunQuery(queryString, "query")
	if err != nil {
		err = errors.Wrapf(err, "failed to run querym, ksql response error: %s", result.Error())
		return
	}
	if result.Error() != nil {
		err = errors.Wrap(err, "returned ksql result contains error")
		return
	}

	header, rows := result.Get()
	qry.queryId = header.QueryId
	qry.schema, err = header.Schema()
	if err != nil {
		err = errors.Wrap(err, "failed to get schema fields")
		return
	}
	_, err = qry.verifySchema(kindPtr)
	if err != nil {
		err = errors.Wrap(err, "verification of schema fields failed")
		return
	}

	err = fillResultIntoSlice(rows, targetSlice)
	if err != nil {
		err = errors.Wrap(err, "couldnt fill result into slice")
		return
	}
	return
}

func (qry *Query) InsertRow(stream string, row interface{}) (err error) {
	if qry.RestApi == nil {
		err = errors.Errorf("ksql rest api not conifgured")
		return
	}
	if !qry.RestApi.IsReady() {
		err = errors.Errorf("ksql rest api not ready")
		return
	}

	val := reflect.ValueOf(row)

	if val.Kind() != reflect.Struct {
		err = errors.Errorf("received non struct type (got %s)", val.Kind())
		return
	}

	_, names, err := getQueryPartial(row)
	if err != nil {
		err = errors.Wrapf(err, "get query partials failed (insert into stream: %s)", stream)
		return
	}

	valString := ""
	for ix, field := range reflect.VisibleFields(val.Type()) {
		if ix > 0 {
			valString += ", "
		}
		switch field.Type.Kind() {
		case reflect.Int, reflect.Int16, reflect.Int32, reflect.Int64:
			valString += fmt.Sprintf("%d", val.FieldByName(field.Name).Interface())
		case reflect.Float32, reflect.Float64:
			valString += fmt.Sprintf("%f", val.FieldByName(field.Name).Interface())
		case reflect.Bool:
			valString += fmt.Sprintf("%v", val.FieldByName(field.Name).Interface())
		case reflect.String:
			valString += fmt.Sprintf("'%v'", val.FieldByName(field.Name).Interface())
		case reflect.TypeOf(time.Time{}).Kind():
			valString += "'" + val.FieldByName(field.Name).Interface().(time.Time).Format(time.RFC3339) + "'"
		default:
			err = errors.Errorf("unsupported type %s", field.Type.Kind())
			return
		}
	}

	queryString := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);", stream, names, valString)

	result, err := qry.RestApi.RunQuery(queryString, "ksql")
	if err != nil {
		err = errors.Wrapf(err, "failed to run querym, ksql response error: %s", result.Error())
		return
	}
	if result.Error() != nil {
		err = errors.Wrap(err, "returned ksql result contains error")
		return
	}

	return
}

func getQueryPartial(modelPtr interface{}) (selectQuery string, insertQuery string, err error) {
	val := reflect.ValueOf(modelPtr)

	var structType reflect.Type

	switch val.Kind() {
	case reflect.Ptr:
		structType = val.Type().Elem()
		if structType.Kind() != reflect.Struct {
			err = errors.Errorf("getQueryPartial received pointer to non-struct type (%s)", structType.Kind())
			return
		}
	case reflect.Struct:
		structType = val.Type()

	default:
		err = errors.Errorf("getQueryPartial received non struct, nor pointer to struct type (got %s)", val.Kind())
		return
	}

	fieldMap := map[string]reflect.Type{}
	insertNames := []string{}
	selectNames := []string{}

	for _, field := range reflect.VisibleFields(structType) {
		if field.IsExported() {
			loName := strings.ToLower(field.Name)
			_, exist := fieldMap[loName]
			if exist {
				err = errors.Errorf("got the same name: %s twice (in lowercase) - struct can't have such in exported fields", loName)
				return
			}
			fieldMap[loName] = field.Type
			switch field.Type.Kind() {
			case reflect.Int, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Float64, reflect.Float32, reflect.Bool, reflect.String:
				selectNames = append(selectNames, loName)
				insertNames = append(insertNames, loName)
			case reflect.TypeOf(time.Time{}).Kind():
				selectNames = append(selectNames, fmt.Sprintf("UNIX_TIMESTAMP(%s) as %s", loName, loName))
				insertNames = append(insertNames, loName)
			default:
				err = errors.Errorf("unsupported type (kind): %s struct field", field.Type.Kind())
				return
			}
		}
	}

	if len(fieldMap) == 0 {
		err = errors.Errorf("received struct with zero exported fields")
		return
	}

	selectQuery = strings.Join(selectNames, ", ")
	insertQuery = strings.Join(insertNames, ", ")
	return
}

func (qry *Query) verifySchema(modelPtr interface{}) (fieldsOk bool, err error) {
	val := reflect.ValueOf(modelPtr)

	if val.Kind() != reflect.Ptr {
		err = errors.Errorf("verifyFieldsInSchema received non pointer type (got %s)", val.Kind())
		return
	}

	structType := val.Type().Elem()
	if structType.Kind() != reflect.Struct {
		err = errors.Errorf("verifyFieldsInSchema received pointer to non-struct type (%s)", structType.Kind())
		return
	}

	for _, field := range reflect.VisibleFields(structType) {
		schemaField, fieldPresent := qry.schema[strings.ToLower(field.Name)]
		if !fieldPresent {
			err = errors.Errorf("field %s, kind %s not present in schema fields ksql response", field.Name, field.Type.Kind())
			return
		}
		switch field.Type.Kind() {
		case reflect.Bool:
			if strings.EqualFold(schemaField.Type, "boolean") {
				fieldsOk = true
			} else {
				fieldsOk = false
				err = errors.Errorf("field %s, kind %s mismatch type in schema (%s)", field.Name, field.Type.Kind(), schemaField.Type)
				return
			}
		case reflect.String:
			if strings.EqualFold(schemaField.Type, "string") || strings.EqualFold(schemaField.Type, "varchar") {
				fieldsOk = true
			} else {
				fieldsOk = false
				err = errors.Errorf("field %s, kind %s mismatch type in schema (%s)", field.Name, field.Type.Kind(), schemaField.Type)
				return
			}
		case reflect.Int32:
			if strings.EqualFold(schemaField.Type, "int") || strings.EqualFold(schemaField.Type, "integer") {
				fieldsOk = true
			} else {
				fieldsOk = false
				err = errors.Errorf("field %s, kind %s mismatch type in schema (%s)", field.Name, field.Type.Kind(), schemaField.Type)
				return
			}
		case reflect.Int64:
			if strings.EqualFold(schemaField.Type, "bigint") || strings.EqualFold(schemaField.Type, "long") {
				fieldsOk = true
			} else {
				fieldsOk = false
				err = errors.Errorf("field %s, kind %s mismatch type in schema (%s)", field.Name, field.Type.Kind(), schemaField.Type)
				return
			}
		case reflect.Float64:
			if strings.EqualFold(schemaField.Type, "double") {
				fieldsOk = true
			} else {
				fieldsOk = false
				err = errors.Errorf("field %s, kind %s mismatch type in schema (%s)", field.Name, field.Type.Kind(), schemaField.Type)
				return
			}
		case reflect.TypeOf(time.Time{}).Kind():
			if strings.EqualFold(schemaField.Type, "bigint") {
				fieldsOk = true
			} else {
				fieldsOk = false
				err = errors.Errorf("field %s, kind %s mismatch type in schema (%s)", field.Name, field.Type.Kind(), schemaField.Type)
				return
			}
		default:
			fieldsOk = false
			err = errors.Errorf("unsupported field %s, kind: %s", field.Name, field.Type.Kind())
			return
		}
	}
	return
}

func fillResultIntoSlice(rows []*KsqlRow, targetSlice interface{}) error {
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
				rowFieldVal := reflect.ValueOf(rowField.Interface())
				switch field.Type.Kind() {
				case reflect.Bool:
					switch rowFieldVal.Kind() {
					case reflect.Bool:
						reflect.Indirect(element).Field(ix).SetBool(rowField.Interface().(bool))
					default:
						return errors.Errorf("unsupported conversion, rowField kind is: %s, target field kind: %s", rowFieldVal.Kind(), field.Type.Kind())
					}
				case reflect.String:
					switch rowFieldVal.Kind() {
					case reflect.String:
						reflect.Indirect(element).Field(ix).SetString(rowField.Interface().(string))
					default:
						return errors.Errorf("unsupported conversion, rowField kind is: %s, target field kind: %s", rowFieldVal.Kind(), field.Type.Kind())
					}
				case reflect.Int32, reflect.Int64:
					switch rowFieldVal.Kind() {
					case reflect.Int, reflect.Int16, reflect.Int32, reflect.Int64:
						reflect.Indirect(element).Field(ix).SetInt(rowField.Interface().(int64))
					case reflect.Float32, reflect.Float64:
						reflect.Indirect(element).Field(ix).SetInt(int64(rowField.Interface().(float64)))
					default:
						return errors.Errorf("unsupported conversion, rowField kind is: %s, target field kind: %s", rowFieldVal.Kind(), field.Type.Kind())
					}
				case reflect.Float64:
					switch rowFieldVal.Kind() {
					case reflect.Float32, reflect.Float64:
						reflect.Indirect(element).Field(ix).SetFloat(rowField.Interface().(float64))
					default:
						return errors.Errorf("unsupported conversion, rowField kind is: %s, target field kind: %s", rowFieldVal.Kind(), field.Type.Kind())
					}
				case reflect.TypeOf(time.Time{}).Kind():
					switch rowFieldVal.Kind() {
					case reflect.Float64:
						t := time.UnixMilli(int64(rowField.Interface().(float64)))
						reflect.Indirect(element).Field(ix).Set(reflect.ValueOf(t))
					default:
						return errors.Errorf("unsupported conversion, rowField kind is: %s, target field kind: %s", rowFieldVal.Kind(), field.Type.Kind())
					}
				}
			}
		}
		valElem.Set(reflect.Append(valElem, reflect.Indirect(element)))
	}
	return nil
}
