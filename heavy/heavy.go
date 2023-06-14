package heavy

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"net"
	"net/http"
	"time"
	"errors"

	log "github.com/inconshreveable/log15"
	"github.com/openrelayxyz/cardinal-rpc"
)

var genericError error = errors.New("failed to retrieve data")

type MockError struct {
	err    string
	Method string
	Params []interface{}
}

func (me *MockError) Error() string {
	return me.err
}

var client = &http.Client{Transport: &http.Transport{
	Proxy: http.ProxyFromEnvironment,
	DialContext: (&net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}).DialContext,
	MaxIdleConnsPerHost:   16,
	MaxIdleConns:          16,
	IdleConnTimeout:       90 * time.Second,
	TLSHandshakeTimeout:   10 * time.Second,
	ExpectContinueTimeout: 1 * time.Second,
}}

func CallHeavy[T any](ctx context.Context, backendURL string, method string, params ...interface{}) (*T, error) {

	if backendURL == "mock" {
		return nil, &MockError{
			err:    "mock response",
			Method: method,
			Params: params,
		}
	}

	log.Debug("call heavy arg params", "params", params)

	call, _ := rpc.NewCall(method, params...)
	callBytes, _ := json.Marshal(call)

	request, _ := http.NewRequestWithContext(ctx, "POST", backendURL, bytes.NewReader(callBytes))
	request.Header.Add("Content-Type", "application/json")

	log.Debug("call heavy request", "method", "POST", "url", backendURL, "headers", request.Header)

	resp, err := client.Do(request)
	if err != nil {
		log.Error("callHeavy connection error", "err", err)
		return nil, rpc.NewRPCError(-32503, genericError.Error())
	}
	defer resp.Body.Close()
	result, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Error("callHeavy response read error", "err", err)
		return nil, rpc.NewRPCError(-32504, genericError.Error())
	}
	response := &rpc.RawResponse{}
	if err := json.Unmarshal(result, &response); err != nil {
		log.Error("callHeavy result unmarshalling error", "err", err)
		return nil, rpc.NewRPCError(-32500, genericError.Error())
	}
	if response.Error != nil {
		log.Error("callHeavy response error", "err", response.Error)
		return nil, genericError
	}
	ret := new(T)
	if err := json.Unmarshal(response.Result, ret); err != nil {
		log.Error("callHeavy response unmarshalling error", "err", err)
		return nil, rpc.NewRPCError(-32500, genericError.Error())
	}
	return ret, nil
}
