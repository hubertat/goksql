package goksql_test

import (
	"goksql"
	"testing"
	"time"
)

func TestLoadTable(t *testing.T) {
	ksql := &goksql.RestKsql{}
	err := ksql.Init("http://10.10.35.31:8088")
	if err != nil {
		t.Error(err)
		return
	}

	type TestRow struct {
		TId        int32
		IsValid    bool
		Name       string
		Value      float64
		OtherValue float64
		CustomDate time.Time
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

	want := []TestRow{
		{1, false, "longer name", 3.0, 1.1, time.Now()},
		{2, false, "noname", 3.0, 22.2, time.Now()},
	}

	for ix, val := range result {
		if len(want) <= ix {
			break
		}
		if want[ix].TId != val.TId || want[ix].Value != val.Value {
			t.Errorf("row mismatch! (showing rows) want:\n%v\ngot:\n%v\n", want[ix], val)
			return
		}
	}

	writeQ := goksql.NewQuery(ksql)
	newRow := TestRow{}
	newRow.TId = 10
	newRow.IsValid = true
	newRow.Name = "to jest z GO"
	newRow.CustomDate = time.Now()
	err = writeQ.InsertRow("testrows", newRow)
	if err != nil {
		t.Error(err)
		return
	}

	newRow.TId = 11
	newRow.CustomDate = time.Now().Add(-30 * time.Hour)
	writeQ.InsertRow("testrows", newRow)

}
