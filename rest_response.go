package goksql

import (
	"strings"

	"github.com/pkg/errors"
)

type KsqlColumn struct {
	Key    interface{}
	Values string
}

type KsqlRow struct {
	Columns interface{}
}

type KsqlError struct {
	ErrorCode     int    `json:"error_code"`
	Message       string `json:"message"`
	StatementText string `json:"statementText"`
}

type KsqlField struct {
	Name  string
	Type  string
	IsKey bool
}

type KsqlHeader struct {
	QueryId      string `json:"queryId"`
	SchemaString string `json:"schema"`
}

func (khead *KsqlHeader) Schema() (schema map[string]KsqlField, err error) {
	schemaSplitted := strings.Split(khead.SchemaString, ",")
	if len(schemaSplitted) == 0 {
		err = errors.Errorf("splitted schema string (%s) is zero length", khead.SchemaString)
		return
	}

	schema = map[string]KsqlField{}

	for _, fieldString := range schemaSplitted {
		field := KsqlField{}
		fieldString = strings.ToLower(strings.TrimSpace(fieldString))
		if strings.HasSuffix(fieldString, "key") {
			fieldString = strings.TrimRight(fieldString, "key")
			field.IsKey = true
			fieldString = strings.TrimSpace(fieldString)
		}
		nameTypePair := strings.Split(fieldString, " ")
		if len(nameTypePair) != 2 {
			err = errors.Errorf("name-type slice incorrect (%d) size for fieldString %s", len(nameTypePair), fieldString)
		}
		field.Type = strings.TrimSpace(nameTypePair[1])
		field.Name = strings.TrimFunc(nameTypePair[0], func(r rune) bool {
			return r == 96 || r == 20
		})
		schema[field.Name] = field
	}

	return
}

type KsqlResponseItem struct {
	Err    *KsqlError  `json:",omitempty"`
	Header *KsqlHeader `json:",omitempty"`
	Row    *KsqlRow    `json:",omitempty"`
}

func (kres *KsqlResponseItem) Error() error {
	if kres.Err == nil {
		return nil
	}

	return errors.Errorf("ksql returned error (code: %d): %s, statement: %s", kres.Err.ErrorCode, kres.Err.Message, kres.Err.StatementText)
}

type KsqlResult struct {
	Result []KsqlResponseItem
}

func (kres *KsqlResult) Error() error {
	for _, item := range kres.Result {
		if item.Err != nil {
			return item.Error()
		}
	}
	return nil
}

func (kres *KsqlResult) Get() (header *KsqlHeader, rows []*KsqlRow) {
	rows = []*KsqlRow{}

	for _, item := range kres.Result {
		if item.Header != nil {
			header = item.Header
		}
		if item.Row != nil {
			rows = append(rows, item.Row)
		}
	}

	return
}
