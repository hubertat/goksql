package goksql_test

import (
	"goksql"
	"testing"
)

func TestRestInit(t *testing.T) {
	k := &goksql.RestKsql{}

	if k.IsReady() {
		t.Error("ksql ready, but not initialized")
	}
	err := k.Init("http://10.10.35.31:8088")

	if err != nil {
		t.Error(err)
	}

	if !k.IsReady() {
		t.Error("ksql not ready")
	}

	err = k.Init("http://invalidurl.com:9999/")
	if err == nil {
		t.Error("Error expected")
	}

	if k.IsReady() {
		t.Errorf("ksql ready but url incorrect")
	}
}
