package chunk

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/prometheus/common/model"
)

// This module exists to fix entries erroneously written by the v9 schema
// into hash buckets owned by the v6 schema.
type Fixer struct {
	storage StorageClient

	// SchemaConfig gives us the dailyBuckets function, which tells us what
	// table to hit etc.
	schemaConfig SchemaConfig

	entries, seriesChunk, metricSeries, metricLabelSeries int

	rewrite WriteBatch
}

func NewFixer(storage StorageClient, schemaConfig SchemaConfig) (*Fixer, error) {
	return &Fixer{
		storage:      storage,
		schemaConfig: schemaConfig,
	}, nil
}

func (f *Fixer) Fix(ctx context.Context, userID string, from, through model.Time, limit int) {
	fmt.Printf("Finding offending entries for user %s from %s through %s\n", userID, from.Time(), through.Time())

	buckets := f.schemaConfig.dailyBuckets(from, through, userID)
	if len(buckets) != 1 {
		log.Fatalf("expected 1 bucket, got %d", len(buckets))
	}

	nextBuckets := f.schemaConfig.dailyBuckets(through+1, through+1001, userID)
	if len(nextBuckets) != 1 {
		log.Fatalf("expected 1 bucket, got %d", len(nextBuckets))
	}

	bucket, nextBucket := buckets[0], nextBuckets[0]
	fmt.Println("Bucket", bucket)
	fmt.Println("Next bucket", nextBucket)

	if bucket.hashKey == nextBucket.hashKey {
		log.Fatalln("expected to get a different bucket", bucket.hashKey, nextBucket.hashKey)
	}
	if bucket.tableName != nextBucket.tableName {
		log.Fatalln("expected to get same table name ", bucket.tableName, nextBucket.tableName)
	}

	f.rewrite = f.storage.NewDeleteBatch()
	rewrites := 0

	// For each row in the range we want, find all the entries.
	f.storage.QueryRows(ctx, bucket.tableName, bucket.hashKey+":", func(result ReadBatch) (shouldContinue bool) {
		for i := 0; i < result.Len(); i++ {
			f.entries++
			if limit > 0 && f.entries > limit {
				return false
			}
			if f.entries%10000 == 0 {
				f.report()
			}

			hashValue := result.HashValue(i)
			rangeValue := result.RangeValue(i)
			value := result.Value(i)

			if f.processResult(bucket.tableName, hashValue, rangeValue, value, nextBucket.hashKey) {
				rewrites++
			}
		}

		if rewrites > 100 {
			if err := f.storage.BatchWrite(ctx, f.rewrite); err != nil {
				log.Fatalf("Error writing entries: %v", err)
			}
			f.rewrite = f.storage.NewDeleteBatch()
			rewrites = 0
		}

		return true
	})

	if rewrites > 100 {
		if err := f.storage.BatchWrite(ctx, f.rewrite); err != nil {
			log.Fatalf("Error writing entries: %v", err)
		}
		f.rewrite = f.storage.NewDeleteBatch()
		rewrites = 0
	}
	f.report()
	fmt.Println("Done.")
}

func (f *Fixer) report() {
	fmt.Println("Found:")
	fmt.Println("   ", f.entries, "entries")
	fmt.Println("   ", f.seriesChunk, "series -> chunks mappings")
	fmt.Println("   ", f.metricSeries, "metric -> series mappings")
	fmt.Println("   ", f.metricLabelSeries, "metric, label -> series mappings")
}

func (f *Fixer) processResult(tableName, hashValue string, rangeValue, value []byte, nextBucket string) bool {
	hashValueParts := strings.SplitN(hashValue, ":", 4)
	if len(hashValueParts) < 3 {
		log.Fatalf("invalid hash value: %x", rangeValue)
	}

	rangeValueParts := decodeRangeKey(rangeValue)
	if len(rangeValueParts) < 4 {
		log.Fatalf("invalid range value: %x", rangeValue)
	}

	version := rangeValueParts[3]

	switch {

	// v9 schema wrote out 3 types of keys:
	//
	// 1. Entry for series ID -> chunk ID
	//      HashValue:  userid : bucket : series ID
	//      RangeValue: through bytes : nil : chunkID : chunkTimeRangeKeyV3
	//
	// 2. Entry for metric name -> series ID
	//      HashValue  - userid : bucket : metric name
	//      RangeValue - seriesID : nil : nil :  seriesRangeKeyV1
	//
	// 3. Entry for metric name, label name -> seriesID
	//      HashValue:  userid : bucket : metric name : label name
	//      RangeValue: value hash : seriesID : nil : labelSeriesRangeKeyV1
	//
	// All we need to do is identify these, and rewrite the bucket.
	// chunkTimeRangeKeyV3 is overloaded, and was written by v6 schema also.
	// We identify it by the hash key, as it contains a series ID and not a metric name.

	case bytes.Equal(version, chunkTimeRangeKeyV3):
		// See if hashValueParts[1] is a base65 encoded series ID (256 bits)
		bytes, err := decodeBase64Value([]byte(hashValueParts[2]))
		if err != nil {
			return false
		}
		if len(bytes) != 32 {
			return false
		}

		f.seriesChunk++
		//newHashValue := fmt.Sprintf("%s:%s", nextBucket, hashValueParts[2])
		//fmt.Printf("Rewrite series -> chunk entry from %s -> %s (%s)\n", hashValue, newHashValue, rangeValue)
		f.rewrite.Add(tableName, hashValue, rangeValue, value)
		return true

	case bytes.Equal(version, seriesRangeKeyV1):
		f.metricSeries++
		//newHashValue := fmt.Sprintf("%s:%s", nextBucket, hashValueParts[2])
		//fmt.Printf("Rewrite metric -> series entry from %s -> %s (%s)\n", hashValue, newHashValue, rangeValue)
		f.rewrite.Add(tableName, hashValue, rangeValue, value)
		return true

	case bytes.Equal(version, labelSeriesRangeKeyV1):
		f.metricLabelSeries++
		//newHashValue := fmt.Sprintf("%s:%s:%s", nextBucket, hashValueParts[2], hashValueParts[3])
		//fmt.Printf("Rewrite metric, label -> series entry from %s -> %s (%s)\n", hashValue, newHashValue, rangeValue)
		f.rewrite.Add(tableName, hashValue, rangeValue, value)
		return true

	default:
		return false
	}
}
