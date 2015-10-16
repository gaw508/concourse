package atcclient_test

import (
	"fmt"
	"net/http"

	"github.com/concourse/atc"
	"github.com/concourse/fly/atcclient"
	"github.com/concourse/fly/rc"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/ghttp"
)

var _ = Describe("ATC Handler Builds", func() {
	var (
		handler   atcclient.AtcHandler
		atcServer *ghttp.Server
		client    atcclient.Client
	)

	BeforeEach(func() {
		var err error
		atcServer = ghttp.NewServer()

		client, err = atcclient.NewClient(
			rc.NewTarget(atcServer.URL(), "", "", "", false),
		)
		Expect(err).NotTo(HaveOccurred())

		handler = atcclient.NewAtcHandler(client)
	})

	AfterEach(func() {
		atcServer.Close()
	})

	Describe("CreateBuild", func() {
		var (
			plan          atc.Plan
			expectedBuild atc.Build
		)
		BeforeEach(func() {
			plan = atc.Plan{
				OnSuccess: &atc.OnSuccessPlan{
					Step: atc.Plan{
						Aggregate: &atc.AggregatePlan{},
					},
					Next: atc.Plan{
						Location: &atc.Location{
							ID:       4,
							ParentID: 0,
						},
						Task: &atc.TaskPlan{
							Name:       "one-off",
							Privileged: true,
							Config:     &atc.TaskConfig{},
						},
					},
				},
			}

			expectedBuild = atc.Build{
				ID:      123,
				Name:    "mybuild",
				Status:  "succeeded",
				JobName: "myjob",
				URL:     "/pipelines/mypipeline/jobs/myjob/builds/mybuild",
				ApiUrl:  "api/v1/builds/123",
			}
			expectedURL := "/api/v1/builds"

			atcServer.AppendHandlers(
				ghttp.CombineHandlers(
					ghttp.VerifyRequest("POST", expectedURL),
					ghttp.VerifyJSONRepresenting(plan),
					ghttp.RespondWithJSONEncoded(http.StatusCreated, expectedBuild, http.Header{}),
				),
			)
		})

		It("takes a plan and creates the build", func() {
			build, err := handler.CreateBuild(plan)
			Expect(err).NotTo(HaveOccurred())
			Expect(build).To(Equal(expectedBuild))
		})
	})

	Describe("JobBuild", func() {
		var (
			expectedBuild        atc.Build
			expectedURL          string
			expectedPipelineName string
		)

		Describe("when build exists", func() {
			JustBeforeEach(func() {
				expectedBuild = atc.Build{
					ID:      123,
					Name:    "mybuild",
					Status:  "succeeded",
					JobName: "myjob",
					URL:     fmt.Sprint("/pipelines/", expectedPipelineName, "/jobs/myjob/builds/mybuild"),
					ApiUrl:  "api/v1/builds/123",
				}

				expectedURL = fmt.Sprint("/api/v1/pipelines/", expectedPipelineName, "/jobs/myjob/builds/mybuild")

				atcServer.AppendHandlers(
					ghttp.CombineHandlers(
						ghttp.VerifyRequest("GET", expectedURL),
						ghttp.RespondWithJSONEncoded(http.StatusOK, expectedBuild, http.Header{}),
					),
				)
			})

			Context("when provided a pipline name", func() {
				BeforeEach(func() {
					expectedPipelineName = "mypipeline"
				})

				It("returns the given build", func() {
					build, err := handler.JobBuild("mypipeline", "myjob", "mybuild")
					Expect(err).NotTo(HaveOccurred())
					Expect(build).To(Equal(expectedBuild))
				})
			})

			Context("when not provided a pipeline name", func() {
				BeforeEach(func() {
					expectedPipelineName = "main"
				})

				It("returns the given build for the default pipeline 'main'", func() {
					build, err := handler.JobBuild("", "myjob", "mybuild")
					Expect(err).NotTo(HaveOccurred())
					Expect(build).To(Equal(expectedBuild))
				})
			})
		})

		Describe("when build does not exists", func() {
			BeforeEach(func() {
				expectedURL = "/api/v1/pipelines/mypipeline/jobs/myjob/builds/mybuild"

				atcServer.AppendHandlers(
					ghttp.CombineHandlers(
						ghttp.VerifyRequest("GET", expectedURL),
						ghttp.RespondWithJSONEncoded(http.StatusNotFound, nil, http.Header{}),
					),
				)
			})

			It("returns an error containing 'build not found'", func() {
				_, err := handler.JobBuild("mypipeline", "myjob", "mybuild")
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("build not found"))
			})
		})
	})

	Describe("Build", func() {
		Describe("when build exists", func() {
			expectedBuild := atc.Build{
				ID:      123,
				Name:    "mybuild",
				Status:  "succeeded",
				JobName: "myjob",
				URL:     "/pipelines/mypipeline/jobs/myjob/builds/mybuild",
				ApiUrl:  "api/v1/builds/123",
			}
			expectedURL := "/api/v1/builds/123"

			BeforeEach(func() {
				atcServer.AppendHandlers(
					ghttp.CombineHandlers(
						ghttp.VerifyRequest("GET", expectedURL),
						ghttp.RespondWithJSONEncoded(http.StatusOK, expectedBuild, http.Header{}),
					),
				)
			})

			It("returns the given build", func() {
				build, err := handler.Build("123")
				Expect(err).NotTo(HaveOccurred())
				Expect(build).To(Equal(expectedBuild))
			})
		})

		Describe("when build does not exists", func() {
			BeforeEach(func() {
				expectedURL := "/api/v1/builds/123"

				atcServer.AppendHandlers(
					ghttp.CombineHandlers(
						ghttp.VerifyRequest("GET", expectedURL),
						ghttp.RespondWithJSONEncoded(http.StatusNotFound, nil, http.Header{}),
					),
				)
			})

			It("returns an error containing 'build not found'", func() {
				_, err := handler.Build("123")
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("build not found"))
			})
		})
	})

	Describe("AllBuilds", func() {
		expectedBuilds := []atc.Build{
			{
				ID:      123,
				Name:    "mybuild1",
				Status:  "succeeded",
				JobName: "myjob",
				URL:     "/pipelines/mypipeline/jobs/myjob/builds/mybuild1",
				ApiUrl:  "api/v1/builds/123",
			},
			{
				ID:      124,
				Name:    "mybuild2",
				Status:  "succeeded",
				JobName: "myjob",
				URL:     "/pipelines/mypipeline/jobs/myjob/builds/mybuild2",
				ApiUrl:  "api/v1/builds/124",
			},
		}
		expectedURL := "/api/v1/builds"

		BeforeEach(func() {
			atcServer.AppendHandlers(
				ghttp.CombineHandlers(
					ghttp.VerifyRequest("GET", expectedURL),
					ghttp.RespondWithJSONEncoded(http.StatusOK, expectedBuilds, http.Header{}),
				),
			)
		})

		It("returns the all the builds", func() {
			build, err := handler.AllBuilds()
			Expect(err).NotTo(HaveOccurred())
			Expect(build).To(Equal(expectedBuilds))
		})
	})

	Describe("AbortBuild", func() {
		BeforeEach(func() {
			expectedURL := "/api/v1/builds/123/abort"

			atcServer.AppendHandlers(
				ghttp.CombineHandlers(
					ghttp.VerifyRequest("POST", expectedURL),
					ghttp.RespondWith(http.StatusNoContent, ""),
				),
			)
		})

		It("sends an abort request to ATC", func() {
			err := handler.AbortBuild("123")
			Expect(err).NotTo(HaveOccurred())
			Expect(atcServer.ReceivedRequests()).To(HaveLen(1))
		})
	})
})
