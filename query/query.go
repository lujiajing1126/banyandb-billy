package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/lujiajing1126/banyandb-billy/api/proto/banyandb/common/v1"
	measurev1 "github.com/lujiajing1126/banyandb-billy/api/proto/banyandb/measure/v1"
	modelv1 "github.com/lujiajing1126/banyandb-billy/api/proto/banyandb/model/v1"
)

var (
	startDateTimeStr = flag.String("starttime", "2019-01-01 00:00", "Datetime to start sweep YYYY-MM-DD hh:mm")
	endDateTimeStr   = flag.String("endtime", "2019-01-01 01:00", "Datetime to end sweep YYYY-MM-DD hh:mm")
	sink             = flag.String("sink", "localhost:17912", "gRPC target for the data ingestion endpoint.")
	topN             = flag.Int("topn", 10, "TopN rank")
)

func main() {
	flag.Parse()

	startTimestamp := mustParseDate(*startDateTimeStr, "startdate")
	endTimestamp := mustParseDate(*endDateTimeStr, "enddate")

	if startTimestamp > endTimestamp {
		log.Fatalf("-starttime=%s cannot exceed -endtime=%s", *startDateTimeStr, *endDateTimeStr)
	}

	conn, err := grpc.Dial(*sink, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("cannot establish a connection to %q: %s", *sink, err)
	}
	defer conn.Close()

	tc := &topNClient{c: measurev1.NewMeasureServiceClient(conn)}
	resp, err := tc.fullScanTopN(startTimestamp, endTimestamp, int32(*topN))
	if err != nil {
		log.Fatalf("fail to query TopN via full-scan: %v", err)
	}
	fmt.Printf("expect TopN(%d): actual is %d. The first elem is %v", *topN, len(resp), resp[0])
}

type topNClient struct {
	c measurev1.MeasureServiceClient
}

func (tc *topNClient) fullScanTopN(startTS, endTS int64, topN int32) ([]*measurev1.DataPoint, error) {
	resp, err := tc.c.Query(context.Background(), &measurev1.QueryRequest{
		Metadata: &commonv1.Metadata{
			Group: "sw_metric",
			Name:  "temperature",
		},
		TimeRange: &modelv1.TimeRange{
			Begin: &timestamppb.Timestamp{Seconds: startTS / 1000},
			End:   &timestamppb.Timestamp{Seconds: endTS / 1000},
		},
		Top: &measurev1.QueryRequest_Top{
			Number:         topN,
			FieldName:      "value",
			FieldValueSort: modelv1.Sort_SORT_DESC,
		},
		TagProjection: &modelv1.TagProjection{
			TagFamilies: []*modelv1.TagProjection_TagFamily{
				{
					Name: "default",
					Tags: []string{"sensor_id"},
				},
				{
					Name: "storage-only",
					Tags: []string{"entity_id"},
				},
			},
		},
		FieldProjection: &measurev1.QueryRequest_FieldProjection{
			Names: []string{"value"},
		},
		GroupBy: &measurev1.QueryRequest_GroupBy{
			TagProjection: &modelv1.TagProjection{
				TagFamilies: []*modelv1.TagProjection_TagFamily{
					{
						Name: "storage-only",
						Tags: []string{"entity_id"},
					},
				},
			},
			FieldName: "value",
		},
	})
	if err != nil {
		return nil, err
	}
	return resp.GetDataPoints(), nil
}

func mustParseDate(dateStr, flagName string) int64 {
	startTime, err := time.Parse("2006-01-02 03:04", dateStr)
	if err != nil {
		log.Fatalf("cannot parse -%s=%q: %s", flagName, dateStr, err)
	}
	return startTime.UnixNano() / 1e6
}
