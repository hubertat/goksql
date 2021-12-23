package goksql

import "github.com/pkg/errors"

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

type KsqlHeader struct {
	QueryId string `json:"queryId"`
	Schema  string `json:"schema"`
}

type KsqlResponseItem struct {
	Err  *KsqlError  `json:",omitempty"`
	Head *KsqlHeader `json:",omitempty"`
	Row  *KsqlRow    `json:",omitempty"`
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
		if item.Head != nil {
			header = item.Head
		}
		if item.Row != nil {
			rows = append(rows, item.Row)
		}
	}

	return
}
