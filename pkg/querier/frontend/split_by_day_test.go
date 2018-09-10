package frontend

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

const seconds = 1e3 // 1e3 milliseconds per second.

func TestNextDayBoundary(t *testing.T) {
	for i, tc := range []struct {
		in, step, out int64
	}{
		{0, 1, millisecondPerDay - 1},
		{0, 15 * seconds, millisecondPerDay - 15*seconds},
		{1 * seconds, 15 * seconds, millisecondPerDay - (15-1)*seconds},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			require.Equal(t, tc.out, nextDayBoundary(tc.in, tc.step))
		})
	}
}

func TestSplitQuery(t *testing.T) {
	for i, tc := range []struct {
		input    queryRangeRequest
		expected []queryRangeRequest
	}{
		{
			input: queryRangeRequest{
				start: 0,
				end:   60 * 60 * seconds,
				step:  15 * seconds,
				query: "foo",
			},
			expected: []queryRangeRequest{
				{
					start: 0,
					end:   60 * 60 * seconds,
					step:  15 * seconds,
					query: "foo",
				},
			},
		},
		{
			input: queryRangeRequest{
				start: 0,
				end:   24 * 3600 * seconds,
				step:  15 * seconds,
				query: "foo",
			},
			expected: []queryRangeRequest{
				{
					start: 0,
					end:   24 * 3600 * seconds,
					step:  15 * seconds,
					query: "foo",
				},
			},
		},
		{
			input: queryRangeRequest{
				start: 0,
				end:   2 * 24 * 3600 * seconds,
				step:  15 * seconds,
				query: "foo",
			},
			expected: []queryRangeRequest{
				{
					start: 0,
					end:   (24 * 3600 * seconds) - (15 * seconds),
					step:  15 * seconds,
					query: "foo",
				},
				{
					start: 24 * 3600 * seconds,
					end:   2 * 24 * 3600 * seconds,
					step:  15 * seconds,
					query: "foo",
				},
			},
		},
		{
			input: queryRangeRequest{
				start: 3 * 3600 * seconds,
				end:   3 * 24 * 3600 * seconds,
				step:  15 * seconds,
				query: "foo",
			},
			expected: []queryRangeRequest{
				{
					start: 3 * 3600 * seconds,
					end:   (24 * 3600 * seconds) - (15 * seconds),
					step:  15 * seconds,
					query: "foo",
				},
				{
					start: 24 * 3600 * seconds,
					end:   (2 * 24 * 3600 * seconds) - (15 * seconds),
					step:  15 * seconds,
					query: "foo",
				},
				{
					start: 2 * 24 * 3600 * seconds,
					end:   3 * 24 * 3600 * seconds,
					step:  15 * seconds,
					query: "foo",
				},
			},
		},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			require.Equal(t, tc.expected, splitQuery(tc.input))
		})
	}
}
