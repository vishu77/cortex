package frontend

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/prometheus/common/model"
)

const millisecondPerDay = int64(24 * time.Hour / time.Millisecond)

type splitByDay struct {
	downstream queryRangeMiddleware
}

type response struct {
	req  queryRangeRequest
	resp *queryRangeResponse
	err  error
}

func (s splitByDay) Do(ctx context.Context, r queryRangeRequest) (*queryRangeResponse, error) {
	// First we're going to build new requests, one for each day, taking care
	// to line up the boundaries with step.
	reqs := splitQuery(r)

	// Next, do the requests in parallel.
	// If one of the requests fail, we want to be a  ble to cancel the rest of them.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	resps := make(chan response)
	for _, req := range reqs {
		go func(req queryRangeRequest) {
			resp, err := s.downstream.Do(ctx, req)
			resps <- response{
				req:  req,
				resp: resp,
				err:  err,
			}
		}(req)
	}

	// Gather up the responses and errors.
	var responses []response
	var firstErr error
	for range reqs {
		select {
		case resp := <-resps:
			if resp.err != nil {
				if firstErr != nil {
					firstErr = resp.err
					cancel()
				}
				continue
			}

			responses = append(responses, resp)
		}
	}

	if firstErr != nil {
		return nil, firstErr
	}

	// Merge the responses.
	sort.Sort(byFirstTime(responses))

	switch responses[0].resp.Result.(type) {
	case model.Vector:
		return vectorMerge(responses)
	case model.Matrix:
		return matrixMerge(responses)
	default:
		return nil, fmt.Errorf("unexpected response type")
	}
}

func splitQuery(r queryRangeRequest) []queryRangeRequest {
	reqs := []queryRangeRequest{}
	for start := r.start; start < r.end; start = nextDayBoundary(start, r.step) + r.step {
		end := nextDayBoundary(start, r.step)
		if end+r.step >= r.end {
			end = r.end
		}

		reqs = append(reqs, queryRangeRequest{
			start: start,
			end:   end,
			step:  r.step,
			query: r.query,
		})
	}
	return reqs
}

// Round up to the step before the next day boundary.
func nextDayBoundary(t, step int64) int64 {
	offsetToDayBoundary := step - (t % millisecondPerDay % step)
	t = ((t / millisecondPerDay) + 1) * millisecondPerDay
	return t - offsetToDayBoundary
}

type byFirstTime []response

func (a byFirstTime) Len() int           { return len(a) }
func (a byFirstTime) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byFirstTime) Less(i, j int) bool { return a[i].req.start < a[j].req.start }

func vectorMerge(resps []response) (*queryRangeResponse, error) {
	var output model.Vector
	for _, resp := range resps {
		output = append(output, resp.resp.Result.(model.Vector)...)
	}
	return &queryRangeResponse{
		Type:   model.ValVector,
		Result: output,
	}, nil
}

func matrixMerge(resps []response) (*queryRangeResponse, error) {
	output := map[string]*model.SampleStream{}
	for _, resp := range resps {
		matrix := resp.resp.Result.(model.Matrix)
		for _, stream := range matrix {
			metric := stream.Metric.String()
			existing, ok := output[metric]
			if !ok {
				output[metric] = stream
				continue
			}
			existing.Values = append(existing.Values, stream.Values...)
		}
	}

	result := make(model.Matrix, len(output))
	for _, stream := range output {
		result = append(result, stream)
	}
	return &queryRangeResponse{
		Type:   model.ValMatrix,
		Result: result,
	}, nil
}
