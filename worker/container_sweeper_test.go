package worker_test

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/concourse/concourse/worker/workerfakes"

	"code.cloudfoundry.org/lager/lagertest"
	"github.com/concourse/concourse/atc/worker/gclient"
	"github.com/concourse/concourse/worker"
	"github.com/onsi/gomega/ghttp"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Container Sweeper", func() {
	var (
		garden *ghttp.Server

		testLogger = lagertest.NewTestLogger("healthchecker")

		sweepInterval time.Duration
		maxInFlight   uint16

		fakeTSAClient workerfakes.FakeTSAClient
		gardenClient  gclient.Client

		sweeper *worker.ContainerSweeper

		gardenClientRequestTimeout time.Duration

		osSignal      chan os.Signal
		readyChan     chan struct{}
		gardenContext context.Context
		gardenCancel  context.CancelFunc

		err error
	)

	BeforeEach(func() {
		sweepInterval = 1 * time.Second
		maxInFlight = 1

		gardenClientRequestTimeout = 1 * time.Second

		garden = ghttp.NewServer()

		osSignal = make(chan os.Signal)
		readyChan = make(chan struct{})
		gardenContext, gardenCancel = context.WithCancel(context.Background())

		gardenClient = gclient.BasicGardenClientWithRequestTimeout(testLogger, gardenClientRequestTimeout, garden.Addr())

		fakeTSAClient.ReportContainersReturns()

		sweeper = worker.NewContainerSweeper(testLogger, sweepInterval, &fakeTSAClient, gardenClient, maxInFlight)
		fmt.Println(sweeper)

	})

	AfterEach(func() {
		garden.Close()
	})

	Context("On Run", func() {

		JustBeforeEach(func() {
			err = sweeper.Run(osSignal, readyChan)
		})

		BeforeEach(func() {
			garden.AppendHandlers(
				ghttp.CombineHandlers(
					ghttp.VerifyRequest("GET", "/containers"),
					ghttp.RespondWithJSONEncoded(200, map[string]string{}),
				),
				ghttp.CombineHandlers(
					ghttp.VerifyRequest("DELETE", "/containers/some-handle-1"),
					func(w http.ResponseWriter, r *http.Request) {
						<-gardenContext.Done()
					},
				),
				ghttp.CombineHandlers(
					ghttp.VerifyRequest("DELETE", "/containers/some-handle-2"),
					func(w http.ResponseWriter, r *http.Request) {
						<-gardenContext.Done()
					},
				),
				ghttp.CombineHandlers(
					ghttp.VerifyRequest("DELETE", "/containers/some-handle-3"),
					func(w http.ResponseWriter, r *http.Request) {
						<-gardenContext.Done()
					},
				),
				ghttp.CombineHandlers(
					ghttp.VerifyRequest("DELETE", "/containers/some-handle-4"),
					func(w http.ResponseWriter, r *http.Request) {
						<-gardenContext.Done()
					},
				),
			)

		})
		AfterEach(func() {
			close(osSignal)
		})

		It("makes an underlying request to baggageclaim", func() {
			Expect(baggageclaim.ReceivedRequests()).To(HaveLen(1))
		})

		It("makes an underlying request to garden", func() {
			Expect(garden.ReceivedRequests()).To(HaveLen(1))
		})

		Context("having a very slow baggaclaim", func() {
			BeforeEach(func() {
				baggageclaim.Reset()
				baggageclaim.AppendHandlers(func(w http.ResponseWriter, r *http.Request) {
					time.Sleep(1 * time.Second)
				})
			})

			It("doesn't wait forever", func() {
				Expect(resp.StatusCode).To(Equal(503))
			})
		})

		Context("having baggageclaim down", func() {
			BeforeEach(func() {
				baggageclaim.Close()
			})

			It("returns 503", func() {
				Expect(resp.StatusCode).To(Equal(503))
			})
		})

		Context("having garden down", func() {
			BeforeEach(func() {
				garden.Close()
			})

			It("returns 503", func() {
				Expect(resp.StatusCode).To(Equal(503))
			})
		})

		Context("having baggageclaim AND garden up", func() {
			It("returns 200", func() {
				Expect(resp.StatusCode).To(Equal(200))
			})
		})
	})
})
