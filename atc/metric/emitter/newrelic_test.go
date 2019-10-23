package emitter

import (
	"code.cloudfoundry.org/lager"

	"github.com/concourse/concourse/atc/metric"
	"github.com/concourse/flag"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/ghttp"
	"time"
)

var _ = Describe("newrelic metric", func() {
	OKResponse := `{"success":true,"uuid":"12345678-1234-5678-9012-123456789012"}`
	var (
		newrelicServer *ghttp.Server
		newrelicConfig *NewRelicConfig
		emitter        metric.Emitter
		logger         lager.Logger
	)

	BeforeEach(func() {
		logger, _ = flag.Lager{LogLevel: "debug"}.Logger("newrelic-test")
		newrelicServer = ghttp.NewServer()
		newrelicServer.AppendHandlers(ghttp.CombineHandlers(
			ghttp.VerifyRequest("POST", "/EVENTS"),
			ghttp.RespondWithJSONEncoded(200, OKResponse),
		))
		newrelicConfig = &NewRelicConfig{
			AccountID:          "ACCOUNT-1",
			APIKey:             "INSERT-API-KEY-1",
			ServicePrefix:      "",
			CompressionEnabled: false,
			FlushInterval:      60,
		}
		var err error
		emitter, err = newrelicConfig.NewEmitter()
		if err != nil {
			Fail("failed to create emitter from new relic configuration")
		}

	})
	AfterEach(func() {
		newrelicServer.Close()
	})
	Context("when compression is disabled", func() {

	})
	Context("when compression is enabled", func() {

	})
	Context("when batch buffer is less than 1MB", func() {
		It("enqueue to the batch buffer", func() {
			emitter.Emit(logger, metric.Event{
				Name:  "build started",
				Value: "",
				State: metric.EventStateOK,
				Host:  "test-client-1",
				Time:  time.Now(),
			})

			if newrelicEmitter, OK := emitter.(*NewRelicEmitter); OK {
				Expect(newrelicEmitter.batchBuffer.payloadQueue).To(HaveLen(101))
			}
		})
	})

	Context("when batch buffer is great than 1MB", func() {
		BeforeEach(func() {
			if newrelicEmitter, OK := emitter.(*NewRelicEmitter); OK {
				newrelicEmitter.setEmitUrl(newrelicServer.URL() + "/EVENTS")
			} else {
				Fail("failed to convert the emitter to NewRelicEmitter")
			}
		})

		It("emit the existed metrics, enqueue the current metric", func() {
			for i := 0; i < 5000; i++ {
				emitter.Emit(logger, metric.Event{
					Name:  "build started",
					Value: "",
					State: metric.EventStateOK,
					Host:  "test-client-1",
					Time:  time.Now(),
				})
			}
		})
	})
})
