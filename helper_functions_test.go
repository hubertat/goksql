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

	selectPartials, insertPartials, err := getQueryPartial(&SomeModel{})
	if err != nil {
		t.Error(err)
		return
	}

	want := "name, ref, uref, value, yesno, UNIX_TIMESTAMP(when) as when"
	if !strings.EqualFold(want, selectPartials) {
		t.Errorf("got:\t%s\nwant:\t%s\n", selectPartials, want)
	}

	want = "name, ref, uref, value, yesno, when"
	if !strings.EqualFold(want, insertPartials) {
		t.Errorf("got:\t%s\nwant:\t%s\n", insertPartials, want)
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
