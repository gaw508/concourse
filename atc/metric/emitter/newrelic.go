package emitter

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"github.com/concourse/flag"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"

	"code.cloudfoundry.org/lager"
	"github.com/concourse/concourse/atc/metric"
	"github.com/pkg/errors"
)

const (
	NEWRELIC_PAYLOAD_MAX_SIZE            = 1024 * 1024
	NEWRELIC_FLUSH_CHECK_INTERVAL_IN_SEC = 30
)

type (
	stats struct {
		created interface{}
		deleted interface{}
	}

	NewRelicEmitter struct {
		client      *http.Client
		url         string
		apikey      string
		prefix      string
		containers  *stats
		volumes     *stats
		compression bool
		batchBuffer *batchBuffer
	}

	NewRelicConfig struct {
		AccountID          string `long:"newrelic-account-id" description:"New Relic Account ID"`
		APIKey             string `long:"newrelic-api-key" description:"New Relic Insights API Key"`
		ServicePrefix      string `long:"newrelic-service-prefix" default:"" description:"An optional prefix for emitted New Relic events"`
		CompressionEnabled bool   `long:"newrelic-metric-compression" description:"New Relic payload compression flag"`
		FlushInterval      int    `long:"newrelic-flush-interval" default:"60" description:"New Relic metric flush interval in seconds"`
	}

	singlePayload map[string]interface{}
	fullPayload   []singlePayload
	batchPayload  []byte

	BatchEmitter interface {
		metric.Emitter
		Flush(logger lager.Logger)
	}

	batchBuffer struct {
		payloadQueue   batchPayload
		lastUpdateTime time.Time
		compressed     bool
		dataLock       *sync.Mutex
		flushInterval  int
	}
)

func init() {
	metric.RegisterEmitter(&NewRelicConfig{})
}

func (config *NewRelicConfig) Description() string { return "NewRelic" }
func (config *NewRelicConfig) IsConfigured() bool {
	return config.AccountID != "" && config.APIKey != ""
}

func (config *NewRelicConfig) NewEmitter() (metric.Emitter, error) {
	client := &http.Client{
		Transport: &http.Transport{},
		Timeout:   time.Minute,
	}

	emitter := &NewRelicEmitter{
		client:      client,
		url:         fmt.Sprintf("https://insights-collector.newrelic.com/v1/accounts/%s/events", config.AccountID),
		apikey:      config.APIKey,
		prefix:      config.ServicePrefix,
		containers:  new(stats),
		volumes:     new(stats),
		compression: config.CompressionEnabled,
		batchBuffer: &batchBuffer{compressed: config.CompressionEnabled,
			dataLock:      new(sync.Mutex),
			flushInterval: config.FlushInterval,
		},
	}

	// start a routine for data flush
	// if the buffer is not full for a specific time, flush the buffer to emit
	logger, _ := flag.Lager{LogLevel: "debug"}.Logger("newrelic-batch-flush")
	go func() {
		for {
			time.Sleep(NEWRELIC_FLUSH_CHECK_INTERVAL_IN_SEC * time.Second)
			emitter.Flush(logger)
		}
	}()

	return emitter, nil
}

func (emitter *NewRelicEmitter) setClient(client *http.Client) *NewRelicEmitter {
	emitter.client = client
	return emitter
}

func (emitter *NewRelicEmitter) setEmitUrl(url string) *NewRelicEmitter {
	emitter.url = url
	return emitter
}

func (emitter *NewRelicEmitter) simplePayload(logger lager.Logger, event metric.Event, nameOverride string) singlePayload {
	name := nameOverride
	if name == "" {
		name = strings.Replace(event.Name, " ", "_", -1)
	}

	eventType := fmt.Sprintf("%s%s", emitter.prefix, name)

	payload := singlePayload{
		"eventType": eventType,
		"value":     event.Value,
		"state":     string(event.State),
		"host":      event.Host,
		"timestamp": event.Time.Unix(),
	}

	for k, v := range event.Attributes {
		payload[fmt.Sprintf("_%s", k)] = v
	}
	return payload
}

func (emitter *NewRelicEmitter) emitBatch(logger lager.Logger, payload batchPayload) {
	var (
		payloadReader io.Reader
		err           error
	)

	payload = append(append([]byte{'['}, payload...), ']')

	if emitter.compression {
		payloadReader, err = gZipBuffer(payload)
		if err != nil {
			logger.Error("failed-to-zip-payload", errors.Wrap(metric.ErrFailedToEmit, err.Error()))
			return
		}
	} else {
		payloadReader = bytes.NewBuffer(payload)
	}

	req, err := http.NewRequest("POST", emitter.url, payloadReader)
	if err != nil {
		logger.Error("failed-to-construct-request", err)
	}

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("X-Insert-Key", emitter.apikey)
	if emitter.compression {
		req.Header.Add("Content-Encoding", "gzip")
	}

	resp, err := emitter.client.Do(req)

	if err != nil {
		logger.Error("failed-to-send-request",
			errors.Wrap(metric.ErrFailedToEmit, err.Error()))
		return
	}
	
	resp.Body.Close()
	emitter.batchBuffer.updateLastTime()
}

func (emitter *NewRelicEmitter) Emit(logger lager.Logger, event metric.Event) {
	payload := emitter.payload(logger, event)

	batchPayload := emitter.batchBuffer.enqueue(logger, payload)
	// check the buffer size (should less then 1 MB)
	if batchPayload != nil {
		//emit the batch payload now
		emitter.emitBatch(logger, batchPayload)
	}
}

func (emitter *NewRelicEmitter) payload(logger lager.Logger, event metric.Event) fullPayload {
	payload := make(fullPayload, 0)

	switch event.Name {

	// These are the simple ones that only need a small name transformation
	case "build started",
		"build finished",
		"worker containers",
		"worker volumes",
		"http response time",
		"database queries",
		"database connections":
		payload = append(payload, emitter.simplePayload(logger, event, ""))

	// These are periodic metrics that are consolidated and only emitted once
	// per cycle (the emit trigger is chosen because it's currently last in the
	// periodic list, so we should have a coherent view). We do this because
	// new relic has a hard limit on the total number of metrics in a 24h
	// period, so batching similar data where possible makes sense.
	case "containers deleted":
		emitter.containers.deleted = event.Value
	case "containers created":
		emitter.containers.created = event.Value
	case "failed containers":
		newPayload := emitter.simplePayload(logger, event, "containers")
		newPayload["failed"] = newPayload["value"]
		newPayload["created"] = emitter.containers.created
		newPayload["deleted"] = emitter.containers.deleted
		delete(newPayload, "value")
		payload = append(payload, newPayload)

	case "volumes deleted":
		emitter.volumes.deleted = event.Value
	case "volumes created":
		emitter.volumes.created = event.Value
	case "failed volumes":
		newPayload := emitter.simplePayload(logger, event, "volumes")
		newPayload["failed"] = newPayload["value"]
		newPayload["created"] = emitter.volumes.created
		newPayload["deleted"] = emitter.volumes.deleted
		delete(newPayload, "value")
		payload = append(payload, newPayload)

	// And a couple that need a small rename (new relic doesn't like some chars)
	case "scheduling: full duration (ms)":
		payload = append(payload, emitter.simplePayload(logger, event, "scheduling_full_duration_ms"))
	case "scheduling: loading versions duration (ms)":
		payload = append(payload, emitter.simplePayload(logger, event, "scheduling_load_duration_ms"))
	case "scheduling: job duration (ms)":
		payload = append(payload, emitter.simplePayload(logger, event, "scheduling_job_duration_ms"))
	default:
		// Ignore the rest
	}

	// But also log any metric that's not EventStateOK, even if we're not
	// otherwise recording it. (This won't be easily graphable, that's okay,
	// this is more for monitoring synthetics)
	if event.State != metric.EventStateOK {
		singlePayload := emitter.simplePayload(logger, event, "alert")
		// We don't have friendly names for all the metrics, and part of the
		// point of this alert is to catch events we should be logging but
		// didn't; therefore, be consistently inconsistent and use the
		// concourse metric names, not our translation layer.
		singlePayload["metric"] = event.Name
		payload = append(payload, singlePayload)
	}
	return payload
}

func (emitter *NewRelicEmitter) Flush(logger lager.Logger) {
	if emitter.batchBuffer.flushThreshold() {
		batchPayload := emitter.batchBuffer.flush()
		if batchPayload != nil && len(batchPayload) > 0 {
			emitter.emitBatch(logger, batchPayload)
		}
	}
}

func (db *batchBuffer) enqueue(logger lager.Logger, payload fullPayload) batchPayload {
	// enqueue the current payload.
	// if it exceeds the max limit(1Mb), return the existed payload, and enqueue the current payload
	// lock and unlock should be applied.
	newPayloadData, err := json.Marshal(payload)
	if err != nil {
		logger.Error("failed-to-serialize-new-payload", err)
		return nil
	}

	var (
		tempBuff []byte
		buff     batchPayload
	)

	if len(db.payloadQueue) != 0 {
		tempBuff = append(db.payloadQueue, ',')
	}
	tempBuff = append(tempBuff, newPayloadData...)

	db.dataLock.Lock()
	defer db.dataLock.Unlock()

	var payloadSize int
	if db.compressed {
		zippedBuffer, err := gZipBuffer(tempBuff)
		if err != nil {
			logger.Error("failed-to-gzip-payload", err)
			return nil
		}

		for {
			zipedData, err := ioutil.ReadAll(zippedBuffer)
			payloadSize = payloadSize + len(zipedData)
			if err == io.EOF {
				break
			}
			if err != nil {
				logger.Error("failed-to-use-gzip-buffer-payload", err)
				return nil
			}
		}
	} else {
		payloadSize = len(tempBuff)
	}

	if payloadSize > NEWRELIC_PAYLOAD_MAX_SIZE-2 { // consider '[' and ']' for the json payload
		buff = db.payloadQueue
		db.payloadQueue = newPayloadData
	} else {
		db.payloadQueue = tempBuff
	}
	return buff
}

func (db *batchBuffer) flush() batchPayload {
	db.dataLock.Lock()
	buff := db.payloadQueue
	db.payloadQueue = nil
	db.dataLock.Unlock()
	return buff
}

func (db *batchBuffer) updateLastTime() {
	db.lastUpdateTime = time.Now()
}

func (db *batchBuffer) flushThreshold() bool {
	return time.Now().Sub(db.lastUpdateTime) > time.Duration(db.flushInterval)*time.Second
}

func gZipBuffer(body []byte) (io.Reader, error) {
	var err error

	readBuffer := bufio.NewReader(bytes.NewReader(body))
	buffer := bytes.NewBuffer([]byte{})
	writer := gzip.NewWriter(buffer)

	_, err = readBuffer.WriteTo(writer)
	if err != nil {
		return nil, err
	}

	err = writer.Close()
	if err != nil {
		return nil, err
	}

	return buffer, nil
}
