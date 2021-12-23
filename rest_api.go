package goksql

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/pkg/errors"
)

type RestKsql struct {
	host *url.URL
	info ServerInfo
}

type ServerInfo struct {
	Version        string
	KafkaClusterId string
	KsqlServiceId  string
	ServerStatus   string
}

func (si *ServerInfo) IsRunning() bool {
	return strings.EqualFold(strings.ToLower(si.ServerStatus), "running")
}

/*
"KsqlServerInfo": {
    "version": "0.22.0",
    "kafkaClusterId": "5PJ1vIMLSoOQ__VC8n6Uog",
    "ksqlServiceId": "default_",
    "serverStatus": "RUNNING"
  }
*/
func (rk *RestKsql) Init(host string) (err error) {
	rk.info = ServerInfo{}

	rk.host, err = url.ParseRequestURI(host)
	if err != nil {
		err = errors.Wrapf(err, "failed to parse host uri: %s", host)
		return
	}

	getInfoPath, err := rk.host.Parse("/info")
	if err != nil {
		err = errors.Wrapf(err, "failed to parse /info url")
		return
	}
	infoResponse, err := http.Get(getInfoPath.String())
	if err != nil {
		err = errors.Wrapf(err, "failed to Get info from ksqldb")
		return
	}

	infoBody, err := io.ReadAll(infoResponse.Body)
	infoResponse.Body.Close()
	if err != nil {
		err = errors.Wrapf(err, "reading response body failed")
		return
	}

	type InfoResult struct {
		KsqlServerInfo ServerInfo
	}
	info := &InfoResult{}

	err = json.Unmarshal(infoBody, info)
	if err != nil {
		err = errors.Wrapf(err, "failed to unmarshal info response, body: %s", string(infoBody))
		return
	}
	rk.info = info.KsqlServerInfo
	return
}

func (rk *RestKsql) IsReady() bool {
	return rk.info.IsRunning()
}

func (rk *RestKsql) RunQuery(queryString string) (result *KsqlResult, err error) {
	getInfoPath, err := rk.host.Parse("/query")
	if err != nil {
		err = errors.Wrapf(err, "failed to parse /query url")
		return
	}

	type QueryObject struct {
		Ksql              string            `json:"ksql"`
		StreamsProperties map[string]string `json:"streamsProperties"`
	}

	query := QueryObject{Ksql: queryString}
	queryBytes, err := json.Marshal(query)
	if err != nil {
		err = errors.Wrapf(err, "failed to json marshal query object (query string: %s)", queryString)
		return
	}

	req, err := http.NewRequest("POST", getInfoPath.String(), bytes.NewReader(queryBytes))
	if err != nil {
		err = errors.Wrapf(err, "failed preparing new POST request")
		return
	}

	req.Header.Add("Accept", "application/json")
	req.Header.Add("Content-Type", "application/json")

	client := &http.Client{
		Timeout: 7 * time.Second,
	}

	resp, err := client.Do(req)
	if err != nil {
		err = errors.Wrap(err, "http client failed")
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		respBytes, _ := io.ReadAll(resp.Body)
		err = errors.Errorf("received http response status %d, %s\nresponse string: %s", resp.StatusCode, resp.Status, string(respBytes))
		return
	}

	if resp.Header.Get("Content-Type") != "application/json" {
		err = errors.Errorf("received unsupported Content-Type: %s in response", resp.Header.Get("application/json"))
		return
	}

	decoder := json.NewDecoder(resp.Body)
	result = &KsqlResult{}
	err = decoder.Decode(&result.Result)
	if err != nil {
		err = errors.Wrap(err, "json decoder (result response) failed")
	}

	return
}
