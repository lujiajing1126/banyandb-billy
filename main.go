package main

import (
	"bufio"
	"flag"
	"io"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"net/http"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/lujiajing1126/banyandb-billy/api/proto/banyandb/common/v1"
	measurev1 "github.com/lujiajing1126/banyandb-billy/api/proto/banyandb/measure/v1"
	modelv1 "github.com/lujiajing1126/banyandb-billy/api/proto/banyandb/model/v1"
)

const (
	measureGroup = "measure"
	measureName  = "temperature"
)

var (
	startDateStr   = flag.String("startdate", "2019-01-01", "Date to start sweep YYYY-MM-DD")
	endDateStr     = flag.String("enddate", "2019-01-31", "Date to end sweep YYYY-MM-DD")
	startKey       = flag.Int("startkey", 1, "First sensor ID")
	endKey         = flag.Int("endkey", 2, "Last sensor ID")
	workers        = flag.Int("workers", runtime.GOMAXPROCS(-1), "The number of concurrent workers used for data ingestion")
	sink           = flag.String("sink", "http://localhost:8428/api/v1/import", "HTTP address for the data ingestion sink. It depends on the `-format`")
	digits         = flag.Int("digits", 2, "The number of decimal digits after the point in the generated temperature. The original benchmark from ScyllaDB uses 2 decimal digits after the point. See query results at https://www.scylladb.com/2019/12/12/how-scylla-scaled-to-one-billion-rows-a-second/")
	reportInterval = flag.Duration("report-interval", 10*time.Second, "Stats reporting interval")
)

func main() {
	flag.Parse()

	startTimestamp := mustParseDate(*startDateStr, "startdate")
	endTimestamp := mustParseDate(*endDateStr, "enddate")
	if startTimestamp > endTimestamp {
		log.Fatalf("-startdate=%s cannot exceed -enddate=%s", *startDateStr, *endDateStr)
	}
	endTimestamp += 24 * 3600 * 1000
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
		for key := *startKey; key <= *endKey; key++ {
			w := work{
				key:            key,
				startTimestamp: startTimestamp,
				rowsCount:      24 * 60,
			}
			workCh <- w
		}
		startTimestamp += 24 * 3600 * 1000
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
	key            int
	startTimestamp int64
	rowsCount      int
}

func (w *work) do(bw *bufio.Writer, r *rand.Rand) {
	writeSeriesBanyanDB(bw, r, w.key, w.rowsCount, w.startTimestamp)
	atomic.AddUint64(&rowsGenerated, uint64(w.rowsCount))
}

func worker(workCh <-chan work) {
	for w := range workCh {
		workerSingleRequest(workCh, w)
	}
}

func workerSingleRequest(workCh <-chan work, wk work) {
	pr, pw := io.Pipe()
	req, err := http.NewRequest("POST", *sink, pr)
	if err != nil {
		log.Fatalf("cannot create request to %q: %s", *sink, err)
	}
	w := io.Writer(pw)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Fatalf("unexpected error when performing request to %q: %s", *sink, err)
		}
		if resp.StatusCode != http.StatusNoContent {
			log.Printf("unexpected response code from %q: %d", *sink, resp.StatusCode)
			data, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				log.Fatalf("cannot read response body: %s", err)
			}
			log.Fatalf("response body:\n%s", data)
		}
	}()
	bw := bufio.NewWriterSize(w, 16*1024)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	blocks := 0
	ok := true
	for ok {
		wk.do(bw, r)
		blocks++
		if *blocksPerRequest > 0 && blocks >= *blocksPerRequest {
			break
		}
		wk, ok = <-workCh
	}
	_ = bw.Flush()
	_ = pw.Close()
	wg.Wait()
}

func writeSeriesBanyanDB(r *rand.Rand, sensorID, rowsCount int, startTimestamp int64) []*measurev1.WriteRequest {
	min := 68 + r.ExpFloat64()/3.0
	e := math.Pow10(*digits)
	metadata := &commonv1.Metadata{Group: measureGroup, Name: measureName}
	requests := make([]*measurev1.WriteRequest, rowsCount)

	t := generateTemperature(r, min, e)
	timestamp := startTimestamp
	for i := 0; i < rowsCount-1; i++ {
		requests = append(requests, &measurev1.WriteRequest{
			Metadata: metadata,
			DataPoint: &measurev1.DataPointValue{
				Timestamp: &timestamppb.Timestamp{
					Seconds: timestamp / 1000,
				},
				TagFamilies: []*modelv1.TagFamilyForWrite{
					{
						Tags: []*modelv1.TagValue{
							{
								// sensorID <string>
								Value: &modelv1.TagValue_Str{
									Str: &modelv1.Str{Value: strconv.Itoa(sensorID)},
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
		})
		t = generateTemperature(r, min, e)
		timestamp = startTimestamp + int64(i+1)*60*1000
	}
	return requests
}

func generateTemperature(r *rand.Rand, min, e float64) float64 {
	t := r.ExpFloat64()/1.5 + min
	return math.Round(t*e) / e
}

func mustParseDate(dateStr, flagName string) int64 {
	startTime, err := time.Parse("2006-01-02", dateStr)
	if err != nil {
		log.Fatalf("cannot parse -%s=%q: %s", flagName, dateStr, err)
	}
	return startTime.UnixNano() / 1e6
}
