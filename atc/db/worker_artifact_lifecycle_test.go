package db_test

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/concourse/concourse/atc"
	"github.com/concourse/concourse/atc/creds"
	"github.com/concourse/concourse/atc/db"
)

var _ = Describe("WorkerArtifactLifecycle", func() {
	var workerArtifactLifecycle db.WorkerArtifactLifecycle

	BeforeEach(func() {
		workerArtifactLifecycle = db.NewArtifactLifecycle(dbConn)
	})

	Describe("RemoveExpiredArtifacts", func() {
		JustBeforeEach(func() {
			err := workerArtifactLifecycle.RemoveExpiredArtifacts()
			Expect(err).ToNot(HaveOccurred())
		})

		Context("removes artifacts created more than 12 hours ago", func() {

			BeforeEach(func() {
				_, err := dbConn.Exec("INSERT INTO worker_artifacts(name, created_at, worker_name) VALUES('artifact-with-association', NOW() - '13 hours'::interval, $1)", defaultWorker.Name())
				Expect(err).ToNot(HaveOccurred())
			})

			It("removes the record", func() {
				var count int
				err := dbConn.QueryRow("SELECT count(*) from worker_artifacts").Scan(&count)
				Expect(err).ToNot(HaveOccurred())
				Expect(count).To(Equal(0))
			})
		})

		Context("keeps artifacts for 12 hours", func() {

			BeforeEach(func() {
				_, err := dbConn.Exec("INSERT INTO worker_artifacts(name, created_at, worker_name) VALUES('artifact-with-association', NOW() - '13 hours'::interval, $1)", defaultWorker.Name())
				Expect(err).ToNot(HaveOccurred())

				_, err = dbConn.Exec("INSERT INTO worker_artifacts(name, created_at, worker_name) VALUES('unassociated-artifact', NOW(), $1)", defaultWorker.Name())
				Expect(err).ToNot(HaveOccurred())
			})

			It("does not remove the record", func() {
				var count int
				err := dbConn.QueryRow("SELECT count(*) from worker_artifacts").Scan(&count)
				Expect(err).ToNot(HaveOccurred())
				Expect(count).To(Equal(1))
			})
		})

		Describe("RemoveUnassociatedWorkerArtifacts", func() {
			JustBeforeEach(func() {
				err := workerArtifactLifecycle.RemoveUnassociatedArtifacts()
				Expect(err).ToNot(HaveOccurred())
			})

			TestInitialized := func(initialized bool, expectedArtifactNames []string) {
				Context("when the worker is in 'stalling' state", func() {

					BeforeEach(func() {
						stallingWorkerPayload := atc.Worker{
							ResourceTypes:   []atc.WorkerResourceType{defaultWorkerResourceType},
							Name:            "stalling-worker",
							GardenAddr:      "2.1.2.1:7777",
							BaggageclaimURL: "3.4.3.4:7878",
						}

						stallingWorker, err := workerFactory.SaveWorker(stallingWorkerPayload, -5*time.Minute)
						Expect(err).ToNot(HaveOccurred())

						_, err = dbConn.Exec("INSERT INTO worker_artifacts(name, created_at, worker_name, initialized) VALUES('artifact-with-association', NOW() - '1 hour'::interval, $1, $2)", stallingWorker.Name(), initialized)
						Expect(err).ToNot(HaveOccurred())

						_, err = dbConn.Exec("INSERT INTO worker_artifacts(name, created_at, worker_name, initialized) VALUES('unassociated-artifact', NOW() - '1 hour'::interval, $1, $2)", defaultWorker.Name(), initialized)
						Expect(err).ToNot(HaveOccurred())

						_, err = workerLifecycle.StallUnresponsiveWorkers()
						Expect(err).ToNot(HaveOccurred())

					})

					It("only removes initialized artifacts on non-stalled workers", func() {
						rows, err := dbConn.Query("SELECT name from worker_artifacts")
						Expect(err).ToNot(HaveOccurred())

						var artifactNames []string
						var artifactName string

						for rows.Next() {
							err = rows.Scan(&artifactName)
							Expect(err).ToNot(HaveOccurred())
							artifactNames = append(artifactNames, artifactName)
						}
						Expect(artifactNames).Should(ConsistOf(expectedArtifactNames))
					})

				})

				Context("worker resource certs", func() {
					BeforeEach(func() {
						tx, err := dbConn.Begin()
						Expect(err).NotTo(HaveOccurred())

						workerResourceCerts, err := db.WorkerResourceCerts{
							WorkerName: defaultWorker.Name(),
							CertsPath:  "/etc/blah/blah/certs",
						}.FindOrCreate(tx)
						Expect(err).ToNot(HaveOccurred())

						_, err = tx.Exec("INSERT INTO worker_artifacts(name, created_at, worker_name, worker_resource_certs_id, initialized) VALUES('artifact-with-association', NOW() - '1 hour'::interval, $1, $2, $3)",
							defaultWorker.Name(), workerResourceCerts.ID, initialized)
						Expect(err).ToNot(HaveOccurred())

						_, err = tx.Exec("INSERT INTO worker_artifacts(name, created_at, worker_name, initialized) VALUES('unassociated-artifact', NOW() - '1 hour'::interval, $1, $2)", defaultWorker.Name(), initialized)
						Expect(err).ToNot(HaveOccurred())

						err = tx.Commit()
						Expect(err).ToNot(HaveOccurred())

					})

					It("only removes initialized artifacts that have no associations", func() {
						rows, err := dbConn.Query("SELECT name from worker_artifacts")
						Expect(err).ToNot(HaveOccurred())

						var artifactNames []string
						var artifactName string

						for rows.Next() {
							err = rows.Scan(&artifactName)
							Expect(err).ToNot(HaveOccurred())
							artifactNames = append(artifactNames, artifactName)
						}
						Expect(artifactNames).Should(ConsistOf(expectedArtifactNames))
					})
				})

				Context("base resource types", func() {
					BeforeEach(func() {
						baseResourceType, found, err := workerBaseResourceTypeFactory.Find(
							"some-base-resource-type",
							defaultWorker,
						)
						Expect(err).ToNot(HaveOccurred())
						Expect(found).To(BeTrue())
						_, err = dbConn.Exec("INSERT INTO worker_artifacts(name, created_at, worker_name, worker_base_resource_type_id, initialized) VALUES('artifact-with-association', NOW() - '1 hour'::interval, $1, $2, $3)", defaultWorker.Name(), baseResourceType.ID, initialized)
						Expect(err).ToNot(HaveOccurred())

						_, err = dbConn.Exec("INSERT INTO worker_artifacts(name, created_at, worker_name, initialized) VALUES('unassociated-artifact', NOW() - '1 hour'::interval, $1, $2)", defaultWorker.Name(), initialized)
						Expect(err).ToNot(HaveOccurred())

					})

					It("only removes initialized artifacts that have no associations", func() {
						rows, err := dbConn.Query("SELECT name from worker_artifacts")
						Expect(err).ToNot(HaveOccurred())

						var artifactNames []string
						var artifactName string

						for rows.Next() {
							err = rows.Scan(&artifactName)
							Expect(err).ToNot(HaveOccurred())
							artifactNames = append(artifactNames, artifactName)
						}
						Expect(artifactNames).Should(ConsistOf(expectedArtifactNames))
					})
				})

				Context("task caches", func() {
					BeforeEach(func() {
						usedTaskCache, err := workerTaskCacheFactory.FindOrCreate(
							defaultJob.ID(),
							"somestep",
							"/some/task/cache",
							defaultWorker.Name(),
						)
						Expect(err).ToNot(HaveOccurred())

						_, err = dbConn.Exec("INSERT INTO worker_artifacts(name, created_at, worker_name, worker_task_cache_id, initialized) VALUES('artifact-with-association', NOW() - '1 hour'::interval, $1, $2, $3)",
							defaultWorker.Name(), usedTaskCache.ID, initialized)
						Expect(err).ToNot(HaveOccurred())

						_, err = dbConn.Exec("INSERT INTO worker_artifacts(name, created_at, worker_name, initialized) VALUES('unassociated-artifact', NOW() - '1 hour'::interval, $1, $2)", defaultWorker.Name(), initialized)
						Expect(err).ToNot(HaveOccurred())

					})

					It("only removes initialized artifacts that have no associations", func() {
						rows, err := dbConn.Query("SELECT name from worker_artifacts")
						Expect(err).ToNot(HaveOccurred())

						var artifactNames []string
						var artifactName string

						for rows.Next() {
							err = rows.Scan(&artifactName)
							Expect(err).ToNot(HaveOccurred())
							artifactNames = append(artifactNames, artifactName)
						}
						Expect(artifactNames).Should(ConsistOf(expectedArtifactNames))
					})
				})

				Context("resource caches", func() {
					BeforeEach(func() {
						build, err := defaultTeam.CreateOneOffBuild()
						Expect(err).ToNot(HaveOccurred())

						resourceCache, err := resourceCacheFactory.FindOrCreateResourceCache(
							logger,
							db.ForBuild(build.ID()),
							"some-base-resource-type",
							atc.Version{"some": "version"},
							atc.Source{"some": "source"},
							atc.Params{},
							creds.VersionedResourceTypes{},
						)
						Expect(err).ToNot(HaveOccurred())

						workerResourceCache := db.WorkerResourceCache{
							ResourceCache: resourceCache,
							WorkerName:    defaultWorker.Name(),
						}

						tx, err := dbConn.Begin()
						Expect(err).ToNot(HaveOccurred())
						defer tx.Rollback()

						usedCache, err := workerResourceCache.FindOrCreate(tx)
						Expect(err).ToNot(HaveOccurred())

						_, err = tx.Exec("INSERT INTO worker_artifacts(name, created_at, worker_name, worker_resource_cache_id, initialized) VALUES('artifact-with-association', NOW() - '1 hour'::interval, $1, $2, $3)", defaultWorker.Name(), usedCache.ID, initialized)
						Expect(err).ToNot(HaveOccurred())

						_, err = tx.Exec("INSERT INTO worker_artifacts(name, created_at, worker_name, initialized) VALUES('unassociated-artifact', NOW() - '1 hour'::interval, $1, $2)", defaultWorker.Name(), initialized)
						Expect(err).ToNot(HaveOccurred())

						err = tx.Commit()
						Expect(err).ToNot(HaveOccurred())
					})

					It("only removes initialized artifacts that have no associations", func() {
						rows, err := dbConn.Query("SELECT name from worker_artifacts")
						Expect(err).ToNot(HaveOccurred())

						var artifactNames []string
						var artifactName string

						for rows.Next() {
							err = rows.Scan(&artifactName)
							Expect(err).ToNot(HaveOccurred())
							artifactNames = append(artifactNames, artifactName)
						}
						Expect(artifactNames).Should(ConsistOf(expectedArtifactNames))
					})
				})

				Context("builds", func() {
					BeforeEach(func() {
						build, err := defaultTeam.CreateOneOffBuild()
						Expect(err).ToNot(HaveOccurred())

						tx, err := dbConn.Begin()
						Expect(err).ToNot(HaveOccurred())
						defer tx.Rollback()

						_, err = tx.Exec("INSERT INTO worker_artifacts(name, created_at, worker_name, build_id, initialized) VALUES('artifact-with-association', NOW() - '1 hour'::interval, $1, $2, $3)",
							defaultWorker.Name(), build.ID(), initialized)
						Expect(err).ToNot(HaveOccurred())

						_, err = tx.Exec("INSERT INTO worker_artifacts(name, created_at, worker_name, initialized) VALUES('unassociated-artifact', NOW() - '1 hour'::interval, $1, $2)", defaultWorker.Name(), initialized)
						Expect(err).ToNot(HaveOccurred())

						err = tx.Commit()
						Expect(err).ToNot(HaveOccurred())
					})

					It("only removes initialized artifacts that have no associations", func() {
						rows, err := dbConn.Query("SELECT name from worker_artifacts")
						Expect(err).ToNot(HaveOccurred())

						var artifactNames []string
						var artifactName string

						for rows.Next() {
							err = rows.Scan(&artifactName)
							Expect(err).ToNot(HaveOccurred())
							artifactNames = append(artifactNames, artifactName)
						}
						Expect(artifactNames).Should(ConsistOf(expectedArtifactNames))
					})
				})
			}

			Context("artifacts are initialized", func() {
				TestInitialized(true, []string{"artifact-with-association"})

				Context("when the associated build has terminated", func() {

					Context("errored build status", func() {
						BeforeEach(func() {
							erroredBuild, err := defaultTeam.CreateOneOffBuild()
							Expect(err).ToNot(HaveOccurred())
							err = erroredBuild.FinishWithError(fmt.Errorf("oh O"))
							Expect(err).ToNot(HaveOccurred())

							succeededBuild, err := defaultTeam.CreateOneOffBuild()
							Expect(err).ToNot(HaveOccurred())
							err = succeededBuild.Finish(db.BuildStatusSucceeded)
							Expect(err).ToNot(HaveOccurred())

							abortedBuild, err := defaultTeam.CreateOneOffBuild()
							Expect(err).ToNot(HaveOccurred())
							err = abortedBuild.MarkAsAborted()
							Expect(err).ToNot(HaveOccurred())

							failedBuild, err := defaultTeam.CreateOneOffBuild()
							Expect(err).ToNot(HaveOccurred())
							err = failedBuild.Finish(db.BuildStatusFailed)
							Expect(err).ToNot(HaveOccurred())

							pendingBuild, err := defaultTeam.CreateOneOffBuild()
							Expect(err).ToNot(HaveOccurred())

							startedBuild, err := defaultTeam.CreateOneOffBuild()
							Expect(err).ToNot(HaveOccurred())

							tx, err := dbConn.Begin()
							Expect(err).ToNot(HaveOccurred())
							defer tx.Rollback()

							_, err = tx.Exec("UPDATE builds SET status='started' where id=$1", startedBuild.ID())
							Expect(err).ToNot(HaveOccurred())

							_, err = tx.Exec("INSERT INTO worker_artifacts(name, created_at, worker_name, build_id, initialized) VALUES('artifact-with-errored-build', NOW() - '1 hour'::interval, $1, $2, $3)", defaultWorker.Name(), erroredBuild.ID(), true)
							Expect(err).ToNot(HaveOccurred())

							_, err = tx.Exec("INSERT INTO worker_artifacts(name, created_at, worker_name, build_id, initialized) VALUES('artifact-with-succeeded-build', NOW() - '1 hour'::interval, $1, $2, $3)", defaultWorker.Name(), succeededBuild.ID(), true)
							Expect(err).ToNot(HaveOccurred())

							_, err = tx.Exec("INSERT INTO worker_artifacts(name, created_at, worker_name, build_id, initialized) VALUES('artifact-with-started-build', NOW() - '1 hour'::interval, $1, $2, $3)", defaultWorker.Name(), startedBuild.ID(), true)
							Expect(err).ToNot(HaveOccurred())

							_, err = tx.Exec("INSERT INTO worker_artifacts(name, created_at, worker_name, build_id, initialized) VALUES('artifact-with-aborted-build', NOW() - '1 hour'::interval, $1, $2, $3)", defaultWorker.Name(), abortedBuild.ID(), true)
							Expect(err).ToNot(HaveOccurred())

							_, err = tx.Exec("INSERT INTO worker_artifacts(name, created_at, worker_name, build_id, initialized) VALUES('artifact-with-failed-build', NOW() - '1 hour'::interval, $1, $2, $3)", defaultWorker.Name(), failedBuild.ID(), true)
							Expect(err).ToNot(HaveOccurred())

							_, err = tx.Exec("INSERT INTO worker_artifacts(name, created_at, worker_name, build_id, initialized) VALUES('artifact-with-pending-build', NOW() - '1 hour'::interval, $1, $2, $3)", defaultWorker.Name(), pendingBuild.ID(), true)
							Expect(err).ToNot(HaveOccurred())

							err = tx.Commit()
							Expect(err).ToNot(HaveOccurred())

						})

						// NOTE: This test does not yet cover builds that might be hijacked
						It("does not remove artifacts owned by in-progress builds ", func() {
							rows, err := dbConn.Query("SELECT name from worker_artifacts")
							Expect(err).ToNot(HaveOccurred())

							var artifactNames []string
							var artifactName string

							for rows.Next() {
								err = rows.Scan(&artifactName)
								Expect(err).ToNot(HaveOccurred())
								artifactNames = append(artifactNames, artifactName)
							}
							Expect(artifactNames).Should(ConsistOf([]string{"artifact-with-started-build", "artifact-with-pending-build"}))

						})
					})
				})
			})
			Context("artifacts are not initialized", func() {
				TestInitialized(false, []string{"artifact-with-association", "unassociated-artifact"})
			})
		})
	})
})
