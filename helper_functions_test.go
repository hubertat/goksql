package goksql

import (
	"strings"
	"testing"
	"time"
)

func TestQueryPartials(t *testing.T) {
	type SomeModel struct {
		Name  string
		Ref   int64
		Uref  int32
		Value float64
		YesNo bool
		When  time.Time
	}

	partialNames, partialWithTypes, err := getQueryPartial(&SomeModel{})
	if err != nil {
		t.Error(err)
		return
	}

	want := "name, ref, uref, value, yesno, when"
	if !strings.EqualFold(want, partialNames) {
		t.Errorf("got:\t%s\nwant:\t%s\n", partialNames, want)
	}

	want = "name STRING, ref BIGINT, uref INT, value DOUBLE, yesno BOOLEAN, when TIMESTAMP"
	if !strings.EqualFold(want, partialWithTypes) {
		t.Errorf("got:\t%s\nwant:\t%s\n", partialWithTypes, want)
	}

	type IncorrectType struct {
		Name     string
		Uref     uint
		WrongInt int
	}

	wrong := IncorrectType{}
	_, _, err = getQueryPartial(wrong)
	if err == nil {
		t.Error("expected error")
	}

}
