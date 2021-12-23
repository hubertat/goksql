package goksql_test

import (
	"goksql"
	"log"
	"testing"
)

func TestLoadTable(t *testing.T) {
	ksql := &goksql.RestKsql{}
	err := ksql.Init("http://10.10.35.31:8088")
	if err != nil {
		t.Error(err)
		return
	}

	type TestRow struct {
		// TId int32
		Name       string
		Value      float64
		OtherValue float64
	}
	result := []TestRow{}
	query := goksql.NewQuery(ksql)
	err = query.LoadTable("rowsWithNames", &TestRow{}, &result)

	if err != nil {
		t.Error(err)
		return
	}

	if len(result) == 0 {
		t.Errorf("0 len result")
		return
	}
	for _, el := range result {
		log.Println(el)
	}

	// want := []TestRow{
	// 	{1, "longer name", 3.0, 1.1},
	// 	{2, "noname", 3.0, 22.2},
	// }

	// for ix, val := range rows {
	// 	if want[ix].TId != int(val.TId) {
	// 		t.Errorf("row mismatch! (showing rows) want:\n%v\ngot:\n%v\n", want[ix], val)
	// 		return
	// 	}
	// }

}
