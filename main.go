package main

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"flag"
	"log"
	"math"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/multierr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/lujiajing1126/banyandb-billy/api/proto/banyandb/common/v1"
	measurev1 "github.com/lujiajing1126/banyandb-billy/api/proto/banyandb/measure/v1"
	modelv1 "github.com/lujiajing1126/banyandb-billy/api/proto/banyandb/model/v1"
)

const (
	measureGroup = "sw_metric"
	measureName  = "temperature"
)

var (
	startDateTimeStr = flag.String("starttime", "2019-01-01 00:00", "Datetime to start sweep YYYY-MM-DD hh:mm")
	endDateTimeStr   = flag.String("endtime", "2019-01-01 01:00", "Datetime to end sweep YYYY-MM-DD hh:mm")
	startKey         = flag.Int("startkey", 1, "First sensor ID")
	endKey           = flag.Int("endkey", 2, "Last sensor ID")
	workers          = flag.Int("workers", 1, "The number of concurrent workers used for data ingestion. By default, measure is written serially.")
	sink             = flag.String("sink", "localhost:17912", "gRPC target for the data ingestion endpoint.")
	digits           = flag.Int("digits", 2, "The number of decimal digits after the point in the generated temperature. The original benchmark from ScyllaDB uses 2 decimal digits after the point. See query results at https://www.scylladb.com/2019/12/12/how-scylla-scaled-to-one-billion-rows-a-second/")
	reportInterval   = flag.Duration("report-interval", 10*time.Second, "Stats reporting interval")
)

var (
	metadata = &commonv1.Metadata{Group: measureGroup, Name: measureName}
)

func main() {
	flag.Parse()

	startTimestamp := mustParseDate(*startDateTimeStr, "startdate")
	endTimestamp := mustParseDate(*endDateTimeStr, "enddate")
	if startTimestamp > endTimestamp {
		log.Fatalf("-starttime=%s cannot exceed -endtime=%s", *startDateTimeStr, *endDateTimeStr)
	}
	endTimestamp += 24 * 3600 * 1000
	// rowsCount is minutes between the startTime and endTime
	rowsCount := int((endTimestamp - startTimestamp) / (60 * 1000))
	if *startKey > *endKey {
		log.Fatalf("-startkey=%d cannot exceed -endkey=%d", *startKey, *endKey)
	}

	workCh := make(chan work)
	var workersWg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		workersWg.Add(1)
		go func() {
			defer workersWg.Done()
			worker(workCh)
		}()
	}
	statsReporterStopCh := make(chan struct{})
	var statsReporterWG sync.WaitGroup
	statsReporterWG.Add(1)
	go func() {
		defer statsReporterWG.Done()
		statsReporter(statsReporterStopCh)
	}()
	keysCount := *endKey - *startKey + 1
	startTime = time.Now()
	rowsTotal = rowsCount * keysCount
	for startTimestamp < endTimestamp {
		w := work{
			startKey:       *startKey,
			endKey:         *endKey,
			startTimestamp: startTimestamp,
			// a work is for a minute
			rowsCount: 1,
		}
		workCh <- w
		// -> next day
		startTimestamp += 1 * 60 * 1000
	}
	close(workCh)
	workersWg.Wait()

	close(statsReporterStopCh)
	statsReporterWG.Wait()
}

var rowsTotal int
var rowsGenerated uint64
var startTime time.Time

func statsReporter(stopCh <-chan struct{}) {
	prevTime := time.Now()
	nPrev := uint64(0)
	ticker := time.NewTicker(*reportInterval)
	mustStop := false
	for !mustStop {
		select {
		case <-ticker.C:
		case <-stopCh:
			mustStop = true
		}
		t := time.Now()
		dAll := t.Sub(startTime).Seconds()
		dLast := t.Sub(prevTime).Seconds()
		nAll := atomic.LoadUint64(&rowsGenerated)
		nLast := nAll - nPrev
		log.Printf("created %d out of %d rows in %.3f seconds at %.0f rows/sec; instant speed %.0f rows/sec",
			nAll, rowsTotal, dAll, float64(nAll)/dAll, float64(nLast)/dLast)
		prevTime = t
		nPrev = nAll
	}
}

type work struct {
	startKey       int
	endKey         int
	startTimestamp int64
	rowsCount      int
}

func (w *work) do(msc measurev1.MeasureServiceClient, r *rand.Rand) {
	if err := writeSeriesBanyanDB(msc, r, w.startKey, w.endKey, w.rowsCount+1, w.startTimestamp); err != nil {
		log.Printf("fail to write series %v", err)
	}
	atomic.AddUint64(&rowsGenerated, uint64(w.rowsCount*(w.endKey-w.startKey+1)))
}

func worker(workCh <-chan work) {
	for w := range workCh {
		workerSingleRequest(workCh, w)
	}
}

func workerSingleRequest(workCh <-chan work, wk work) {
	conn, err := grpc.Dial(*sink, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("cannot establish a connection to %q: %s", *sink, err)
	}
	defer conn.Close()
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	ok := true
	// create a measure write stub
	msc := measurev1.NewMeasureServiceClient(conn)
	for ok {
		wk.do(msc, r)
		wk, ok = <-workCh
	}
}

func writeSeriesBanyanDB(msc measurev1.MeasureServiceClient, r *rand.Rand, startKey, endKey int, rowsCount int, startTimestamp int64) error {
	wc, err := msc.Write(context.Background())
	if err != nil {
		return err
	}
	min := 68 + r.ExpFloat64()/3.0
	e := math.Pow10(*digits)

	t := generateTemperature(r, min, e)
	timestamp := startTimestamp
	var sendError error
	byteSlice := make([]byte, 8)
	for i := 0; i < rowsCount-1; i++ {
		for key := startKey; key <= endKey; key++ {
			sendError = multierr.Append(sendError, wc.Send(&measurev1.WriteRequest{
				Metadata: metadata,
				DataPoint: &measurev1.DataPointValue{
					Timestamp: &timestamppb.Timestamp{
						Seconds: timestamp / 1000,
					},
					TagFamilies: []*modelv1.TagFamilyForWrite{
						{
							Tags: []*modelv1.TagValue{
								{
									// sensor_id <Str>
									Value: &modelv1.TagValue_Str{
										Str: &modelv1.Str{Value: strconv.Itoa(key)},
									},
								},
							},
						},
						{
							Tags: []*modelv1.TagValue{
								{
									// entity_id <ID>
									Value: &modelv1.TagValue_Id{
										Id: &modelv1.ID{Value: encodeBase64(byteSlice, key)},
									},
								},
							},
						},
					},
					Fields: []*modelv1.FieldValue{
						{
							// temperature
							Value: &modelv1.FieldValue_Int{
								Int: &modelv1.Int{Value: int64(math.Round(t))},
							},
						},
					},
				},
			}))
			t = generateTemperature(r, min, e)
		}
		//log.Printf("measure written for %v", time.Unix(timestamp/1000, 0))
		// -> next minute
		timestamp = startTimestamp + int64(i+1)*60*1000
	}
	return sendError
}

func encodeBase64(dst []byte, sensorID int) string {
	defer func() {
		// reset buffer
		dst = dst[:0]
	}()
	binary.BigEndian.PutUint64(dst, uint64(sensorID))
	return base64.StdEncoding.EncodeToString(dst)
}

func generateTemperature(r *rand.Rand, min, e float64) float64 {
	t := r.ExpFloat64()/1.5 + min
	return math.Round(t*e) / e
}

func mustParseDate(dateStr, flagName string) int64 {
	startTime, err := time.Parse("2006-01-02 03:04", dateStr)
	if err != nil {
		log.Fatalf("cannot parse -%s=%q: %s", flagName, dateStr, err)
	}
	return startTime.UnixNano() / 1e6
}
