package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

var (
	stream       = flag.String("stream", "your-stream", "your stream name")
	region       = flag.String("region", "ap-northeast-1", "the AWS region where your Kinesis Stream is.")
	iteratorType = flag.String("iterator-type", "LATEST", "iterator type. Choose from TRIM_HORIZON(default), AT_SEQUENCE_NUMBER, AT_TIMESTAMP or LATEST.")
	interval     = flag.Duration("interval", 3*time.Second, "seconds for waiting next GetRecords request.")
	startTime    = flag.String("start-time", "", "timestamp to start reading. only enable when iterator type is AT_TIMESTAMP. acceptable format is YYYY-MM-DDThh:mm:ss.sssTZD (RFC3339 format). For example, 2016-04-20T12:00:00+09:00 is acceptable.")
)

func main() {
	flag.Parse()

	c := getKinesisClient()

	var start time.Time
	if *iteratorType == "AT_TIMESTAMP" && *startTime != "" {
		t, err := time.Parse(time.RFC3339, *startTime)
		if err != nil {
			fail(fmt.Sprintf("parse time failed. -start-time format should be RFC3339 format.: %s", err))
		}
		start = t
	}

	streamName := aws.String(*stream)

	streams, err := c.DescribeStream(&kinesis.DescribeStreamInput{StreamName: streamName})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Cannot describe stream. Please verify your stream is accessible.: %s", err)
		os.Exit(1)
	}

	records := make(chan string)

	for _, shard := range streams.StreamDescription.Shards {
		iterator, err := getIterator(c, streamName, shard, &start)
		if err != nil {
			fail(fmt.Sprintf("Could not get iterator for shard %s: %v", *shard.ShardId, err))
		}

		go func(s *kinesis.Shard) {
			iter := iterator.ShardIterator
			for {
				iter, err = fetchRecords(c, iter, records)
				if err != nil {
					fail(fmt.Sprintf("Failed reading from shard %s: %v\n", *s.ShardId, err))
				}
				time.Sleep(*interval)
			}
		}(shard)
	}

	for record := range records {
		fmt.Println(record)
	}
}

func getIterator(k *kinesis.Kinesis, stream *string, shard *kinesis.Shard, t *time.Time) (*kinesis.GetShardIteratorOutput, error) {
	return k.GetShardIterator(&kinesis.GetShardIteratorInput{
		ShardId:           shard.ShardId,
		ShardIteratorType: aws.String(*iteratorType),
		StreamName:        stream,
		Timestamp:         t,
	})
}

func getKinesisClient() *kinesis.Kinesis {
	s, err := session.NewSession(&aws.Config{Region: aws.String(*region)})
	if err != nil {
		fail(fmt.Sprintf("Could not create AWS session: %v", err))
	}

	return kinesis.New(s)
}

func fail(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(1)
}

// iter fetch records.
func fetchRecords(k *kinesis.Kinesis, shardIterator *string, out chan string) (*string, error) {
	records, err := k.GetRecords(&kinesis.GetRecordsInput{
		ShardIterator: shardIterator,
	})

	if err != nil {
		return nil, err
	}
	for _, r := range records.Records {
		out <- string(r.Data)
	}
	return records.NextShardIterator, nil
}
