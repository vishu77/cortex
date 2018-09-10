// Copyright 2016 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Mostly lifted from prometheus/web/api/v1/api.go.

package frontend

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/go-kit/kit/log/level"
	jsoniter "github.com/json-iterator/go"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/util/stats"
	"github.com/weaveworks/cortex/pkg/util"
)

type queryRangeMiddleware interface {
	Do(context.Context, queryRangeRequest) (*queryRangeResponse, error)
}

type queryRangeRoundTripper struct {
	downstream           http.RoundTripper
	queryRangeMiddleware queryRangeMiddleware
}

func (q queryRangeRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	if !strings.HasSuffix(r.URL.Path, "/query_range") {
		return q.downstream.RoundTrip(r)
	}

	request, err := parseQueryRangeRequest(r)
	if err != nil {
		return nil, err
	}

	response, err := q.queryRangeMiddleware.Do(r.Context(), request)
	if err != nil {
		return nil, err
	}

	return response.toHTTPResponse()
}

type queryRangeTerminator struct {
	downstream http.RoundTripper
}

func (q queryRangeTerminator) Do(ctx context.Context, r queryRangeRequest) (*queryRangeResponse, error) {
	request, err := r.toHTTPRequest()
	if err != nil {
		return nil, err
	}

	response, err := q.downstream.RoundTrip(request)
	if err != nil {
		return nil, err
	}

	var resp queryRangeResponse
	if err := json.NewDecoder(response.Body).Decode(&resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

type queryRangeRequest struct {
	start, end int64 // Milliseconds since epoch.
	step       int64 // Milliseconds.
	timeout    time.Duration
	query      string
}

func parseQueryRangeRequest(r *http.Request) (queryRangeRequest, error) {
	var result queryRangeRequest
	var err error

	result.start, err = parseTime(r.FormValue("start"))
	if err != nil {
		return result, err
	}

	result.end, err = parseTime(r.FormValue("end"))
	if err != nil {
		return result, err
	}

	if result.end < result.start {
		err := errors.New("end timestamp must not be before start time")
		return result, err
	}

	result.step, err = parseDurationMs(r.FormValue("step"))
	if err != nil {
		return result, err
	}

	if result.step <= 0 {
		err := errors.New("zero or negative query resolution step widths are not accepted. Try a positive integer")
		return result, err
	}

	// For safety, limit the number of returned points per timeseries.
	// This is sufficient for 60s resolution for a week or 1h resolution for a year.
	if (result.end-result.start)/result.step > 11000 {
		err := errors.New("exceeded maximum resolution of 11,000 points per timeseries. Try decreasing the query resolution (?step=XX)")
		return result, err
	}

	result.query = r.FormValue("query")
	return result, nil
}

func (q queryRangeRequest) toHTTPRequest() (*http.Request, error) {
	params := url.Values{
		"start": []string{encodeTime(q.start)},
		"end":   []string{encodeTime(q.end)},
		"step":  []string{encodeDurationMs(q.step)},
		"query": []string{q.query},
	}
	u := url.URL{
		Path:     "/query_range",
		RawQuery: params.Encode(),
	}
	return http.NewRequest("GET", u.String(), nil)
}

func parseTime(s string) (int64, error) {
	if t, err := strconv.ParseFloat(s, 64); err == nil {
		s, ns := math.Modf(t)
		tm := time.Unix(int64(s), int64(ns*float64(time.Second)))
		return tm.UnixNano() / int64(time.Millisecond/time.Nanosecond), nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t.UnixNano() / int64(time.Millisecond/time.Nanosecond), nil
	}
	return 0, fmt.Errorf("cannot parse %q to a valid timestamp", s)
}

func parseDuration(s string) (time.Duration, error) {
	if d, err := strconv.ParseFloat(s, 64); err == nil {
		ts := d * float64(time.Second)
		if ts > float64(math.MaxInt64) || ts < float64(math.MinInt64) {
			return 0, fmt.Errorf("cannot parse %q to a valid duration. It overflows int64", s)
		}
		return time.Duration(ts), nil
	}
	if d, err := model.ParseDuration(s); err == nil {
		return time.Duration(d), nil
	}
	return 0, fmt.Errorf("cannot parse %q to a valid duration", s)
}

func parseDurationMs(s string) (int64, error) {
	if d, err := strconv.ParseFloat(s, 64); err == nil {
		ts := d * float64(time.Second/time.Millisecond)
		if ts > float64(math.MaxInt64) || ts < float64(math.MinInt64) {
			return 0, fmt.Errorf("cannot parse %q to a valid duration. It overflows int64", s)
		}
		return int64(ts), nil
	}
	if d, err := model.ParseDuration(s); err == nil {
		return int64(d) / int64(time.Millisecond/time.Nanosecond), nil
	}
	return 0, fmt.Errorf("cannot parse %q to a valid duration", s)
}

func encodeTime(t int64) string {
	f := float64(t) / 1.0e3
	return strconv.FormatFloat(f, 'f', -1, 64)
}

func encodeDurationMs(d int64) string {
	return strconv.FormatFloat(float64(d)/float64(time.Second/time.Millisecond), 'f', -1, 64)
}

// queryRangeResponse contains result data for a query_range.
type queryRangeResponse struct {
	Type   model.ValueType   `json:"resultType"`
	Stats  *stats.QueryStats `json:"stats,omitempty"`
	Result model.Value       `json:"result"`
}

func (q *queryRangeResponse) UnmarshalJSON(b []byte) error {
	v := struct {
		Type   model.ValueType   `json:"resultType"`
		Stats  *stats.QueryStats `json:"stats,omitempty"`
		Result json.RawMessage   `json:"result"`
	}{}

	err := json.Unmarshal(b, &v)
	if err != nil {
		return err
	}

	q.Type = v.Type
	q.Stats = v.Stats

	switch v.Type {
	case model.ValVector:
		var vv model.Vector
		err = json.Unmarshal(v.Result, &vv)
		q.Result = vv

	case model.ValMatrix:
		var mv model.Matrix
		err = json.Unmarshal(v.Result, &mv)
		q.Result = mv

	default:
		err = fmt.Errorf("unexpected value type %q", v.Type)
	}
	return err
}

func (q queryRangeResponse) toHTTPResponse() (*http.Response, error) {
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	b, err := json.Marshal(&apiResponse{
		Status: "success",
		Data:   q,
	})
	if err != nil {
		level.Error(util.Logger).Log("msg", "error marshalling json response", "err", err)
		return nil, err
	}
	resp := http.Response{
		Header:     http.Header{},
		Body:       ioutil.NopCloser(bytes.NewBuffer(b)),
		StatusCode: http.StatusOK,
	}
	resp.Header.Set("Content-Type", "application/json")
	return &resp, nil
}

type apiResponse struct {
	Status    string             `json:"status"`
	Data      queryRangeResponse `json:"data,omitempty"`
	ErrorType string             `json:"errorType,omitempty"`
	Error     string             `json:"error,omitempty"`
}
