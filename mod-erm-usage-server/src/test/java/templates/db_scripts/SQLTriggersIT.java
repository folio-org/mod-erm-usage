package templates.db_scripts;

import static org.assertj.core.api.Assertions.assertThat;
import static org.folio.rest.util.Constants.TABLE_NAME_COUNTER_REPORTS;
import static org.folio.rest.util.Constants.TABLE_NAME_UDP;

import com.google.common.io.Resources;
import io.vertx.core.CompositeFuture;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Repeat;
import io.vertx.ext.unit.junit.RepeatRule;
import io.vertx.ext.unit.junit.Timeout;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.nio.charset.StandardCharsets;
import java.time.YearMonth;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang3.RandomUtils;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.postgres.testing.PostgresTesterContainer;
import org.folio.rest.RestVerticle;
import org.folio.rest.impl.TenantReferenceAPI;
import org.folio.rest.jaxrs.model.Aggregator;
import org.folio.rest.jaxrs.model.AggregatorSetting;
import org.folio.rest.jaxrs.model.CounterReport;
import org.folio.rest.jaxrs.model.Parameter;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.jaxrs.model.UsageDataProvider;
import org.folio.rest.jaxrs.model.UsageDataProvider.HasFailedReport;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.interfaces.Results;
import org.folio.rest.tools.utils.NetworkUtils;
import org.folio.rest.util.ModuleVersion;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class SQLTriggersIT {

  @Rule public RepeatRule repeatRule = new RepeatRule();

  private static final String TENANT = "testtenant";
  private static final String TABLE_AGGREGATOR = "aggregator_settings";
  private static Vertx vertx;
  private static int port;
  private static final List<Parameter> parameters =
      Arrays.asList(
          new Parameter().withKey("loadSample").withValue("true"),
          new Parameter().withKey("loadReference").withValue("true"));
  private static TenantAttributes tenantAttributes;
  private static AggregatorSetting sampleAggregator;
  private static UsageDataProvider sampleUDP;
  private static CounterReport sampleReport;
  private static CounterReport reportFailed;
  @Rule public Timeout timeout = Timeout.seconds(10);

  static boolean start = true;

  @BeforeClass
  public static void init(TestContext context) {
    vertx = Vertx.vertx();
    try {
      PostgresClient.setPostgresTester(new PostgresTesterContainer());
      port = NetworkUtils.nextFreePort();
      tenantAttributes =
          new TenantAttributes()
              .withModuleTo(ModuleVersion.getModuleVersion())
              .withParameters(parameters);

      String aggregatorJSON =
          Resources.toString(
              Resources.getResource(
                  "sample-data/aggregator-settings/german_national_statistics_server.json"),
              StandardCharsets.UTF_8);
      sampleAggregator = Json.decodeValue(aggregatorJSON, AggregatorSetting.class);
      String udpJSON =
          Resources.toString(
              Resources.getResource("sample-data/usage-data-providers/ACSO.json"),
              StandardCharsets.UTF_8);
      sampleUDP = Json.decodeValue(udpJSON, UsageDataProvider.class);
      String reportJSON =
          Resources.toString(
              Resources.getResource("sample-data/counter-reports/2018-01.json"),
              StandardCharsets.UTF_8);
      sampleReport = Json.decodeValue(reportJSON, CounterReport.class);
      reportFailed =
          Json.decodeValue(reportJSON, CounterReport.class)
              .withId(UUID.randomUUID().toString())
              .withYearMonth("2018-02")
              .withFailedAttempts(3)
              .withFailedReason("This is a failed report")
              .withReport(null);
    } catch (Exception e) {
      e.printStackTrace();
      context.fail(e);
    }

    DeploymentOptions options =
        new DeploymentOptions().setConfig(new JsonObject().put("http.port", port));
    vertx.deployVerticle(RestVerticle.class.getName(), options, context.asyncAssertSuccess());
  }

  @AfterClass
  public static void tearDown(TestContext context) {
    vertx.close(context.asyncAssertSuccess(res -> PostgresClient.stopPostgresTester()));
  }

  private static PostgresClient getPGClient() {
    return PostgresClient.getInstance(vertx, TENANT);
  }

  private static Future<Void> validateSampleData(TestContext context) {
    Promise<Results<UsageDataProvider>> providerPromise = Promise.promise();
    getPGClient().get(TABLE_NAME_UDP, new UsageDataProvider(), false, providerPromise);

    Promise<Results<CounterReport>> reportPromise = Promise.promise();
    getPGClient().get(TABLE_NAME_COUNTER_REPORTS, new CounterReport(), false, reportPromise);

    Promise<Results<AggregatorSetting>> aggregatorPromise = Promise.promise();
    getPGClient().get(TABLE_AGGREGATOR, new AggregatorSetting(), false, aggregatorPromise);

    return CompositeFuture.all(
            providerPromise.future(), reportPromise.future(), aggregatorPromise.future())
        .compose(
            cf -> {
              context.verify(
                  v -> {
                    assertThat(providerPromise.future().result().getResults().size()).isEqualTo(4);
                    assertThat(reportPromise.future().result().getResults().size()).isEqualTo(4);
                    assertThat(aggregatorPromise.future().result().getResults().size())
                        .isEqualTo(1);
                  });
              return Future.succeededFuture();
            });
  }

  @Before
  public void before(TestContext context) {
    deleteSampleData()
        .compose(r -> loadSampleData())
        .compose(r -> validateSampleData(context))
        .onComplete(context.asyncAssertSuccess());
  }

  private Future<Integer> truncateTable(String tableName) {
    Promise<Integer> promise = Promise.promise();
    getPGClient()
        .delete(
            tableName,
            new Criterion(),
            ar -> promise.complete((ar.succeeded()) ? ar.result().rowCount() : 0));
    return promise.future();
  }

  private Future<Integer> deleteSampleData() {
    if (start) {
      start = false;
      return Future.succeededFuture();
    }
    return truncateTable(TABLE_NAME_UDP)
        .compose(i -> truncateTable(TABLE_AGGREGATOR))
        .compose(i -> truncateTable(TABLE_NAME_COUNTER_REPORTS));
  }

  private Future<Void> loadSampleData() {
    Promise<Void> promise = Promise.promise();
    try {
      new TenantReferenceAPI()
          .postTenant(
              tenantAttributes,
              Map.of(
                  XOkapiHeaders.TENANT.toLowerCase(),
                  TENANT,
                  XOkapiHeaders.URL,
                  "http://localhost:" + port),
              res -> {
                if (res.result().getStatus() == 204) {
                  promise.complete();
                } else {
                  promise.fail(
                      String.format(
                          "Tenantloading returned %s %s",
                          res.result().getStatus(),
                          res.result().getStatusInfo().getReasonPhrase()));
                }
              },
              vertx.getOrCreateContext());
    } catch (Exception e) {
      promise.fail(e);
    }
    return promise.future();
  }

  @SuppressWarnings("unchecked")
  private <T> T clone(T o) {
    return Json.decodeValue(Json.encode(o), (Class<T>) o.getClass());
  }

  private UsageDataProvider changeAggregatorId(UsageDataProvider p, String id) {
    UsageDataProvider clone = clone(p);
    Aggregator aggregator = clone.getHarvestingConfig().getAggregator();
    aggregator.setId(id);
    return clone;
  }

  @Test
  public void updateAggregatorLabelReferencesAfterUpdate(TestContext context) {
    AggregatorSetting aggregatorSetting = clone(sampleAggregator);
    aggregatorSetting.setLabel("Some other label");

    getPGClient()
        .upsert(
            TABLE_AGGREGATOR,
            aggregatorSetting.getId(),
            aggregatorSetting,
            context.asyncAssertSuccess(
                h ->
                    getPGClient()
                        .getById(
                            TABLE_NAME_UDP,
                            "e67924ee-aa00-454e-8fd0-c3f81339d20e",
                            UsageDataProvider.class,
                            context.asyncAssertSuccess(
                                h2 ->
                                    context.verify(
                                        v ->
                                            assertThat(
                                                    h2.getHarvestingConfig()
                                                        .getAggregator()
                                                        .getName())
                                                .isEqualTo(aggregatorSetting.getLabel()))))));
  }

  @Test
  public void resolveAggregatorLabelBeforeInsert(TestContext context) {
    getPGClient()
        .getById(
            TABLE_NAME_UDP,
            "e67924ee-aa00-454e-8fd0-c3f81339d20e",
            UsageDataProvider.class,
            context.asyncAssertSuccess(
                udp ->
                    context.verify(
                        v ->
                            assertThat(udp.getHarvestingConfig().getAggregator().getName())
                                .isEqualTo("German National Statistics Server"))));
  }

  @Test
  public void resolveAggregatorLabelBeforeUpdate(TestContext context) {
    String id = "98027fd7-b6d8-45b2-8f29-f3029f98f20a";
    String aggregatorName = "Aggregator2";
    getPGClient()
        .save( // save a new aggregator
            TABLE_AGGREGATOR,
            id,
            clone(sampleAggregator).withId(id).withLabel(aggregatorName),
            context.asyncAssertSuccess(
                s ->
                    getPGClient()
                        .update( // change udp to use new aggregator
                            TABLE_NAME_UDP,
                            changeAggregatorId(sampleUDP, id),
                            sampleUDP.getId(),
                            context.asyncAssertSuccess(
                                ur ->
                                    getPGClient()
                                        .getById( // test if name got changed
                                            TABLE_NAME_UDP,
                                            sampleUDP.getId(),
                                            UsageDataProvider.class,
                                            context.asyncAssertSuccess(
                                                udp ->
                                                    context.verify(
                                                        v ->
                                                            assertThat(
                                                                    udp.getHarvestingConfig()
                                                                        .getAggregator()
                                                                        .getName())
                                                                .isEqualTo(aggregatorName))))))));
  }

  @Repeat(10)
  @Test
  public void updateProviderOnInsertConcurrent(TestContext context) {
    Async async = context.async();
    final YearMonth reportStart = YearMonth.of(2018, 1);
    final int reportCount = 12;

    UsageDataProvider udp = new UsageDataProvider().withId(UUID.randomUUID().toString());
    Promise<String> createUDPPromise = Promise.promise();
    getPGClient().upsert(TABLE_NAME_UDP, udp.getId(), udp, createUDPPromise);

    List<CounterReport> reports =
        IntStream.rangeClosed(1, reportCount)
            .mapToObj(
                i ->
                    new CounterReport()
                        .withReportName("report" + (i % 4 + 1))
                        .withProviderId(udp.getId())
                        .withYearMonth(reportStart.plusMonths(i - 1).toString()))
            .collect(Collectors.toList());

    createUDPPromise
        .future()
        .compose(
            s ->
                CompositeFuture.all(
                    reports.stream()
                        .unordered()
                        .parallel()
                        .map(
                            cr -> {
                              Promise<String> promise = Promise.promise();
                              getPGClient().save(TABLE_NAME_COUNTER_REPORTS, cr, promise);
                              return promise.future();
                            })
                        .collect(Collectors.toList())))
        .compose(
            cf -> {
              Promise<UsageDataProvider> result = Promise.promise();
              getPGClient().getById(TABLE_NAME_UDP, udp.getId(), UsageDataProvider.class, result);
              return result.future();
            })
        .onSuccess(
            result -> {
              context.verify(
                  v -> {
                    assertThat(result.getLatestReport())
                        .isEqualTo(reportStart.plusMonths(reportCount - 1).toString());
                    assertThat(result.getEarliestReport()).isEqualTo(reportStart.toString());
                    assertThat(result.getHasFailedReport()).isEqualTo(HasFailedReport.NO);
                    assertThat(result.getReportErrorCodes()).isEmpty();
                    assertThat(result.getReportTypes())
                        .containsExactlyInAnyOrder("report1", "report2", "report3", "report4");
                  });
              async.complete();
            })
        .onFailure(context::fail);
  }

  @Repeat(10)
  @Test
  public void updateProviderErrorCodesOnInsertConcurrent(TestContext context) {
    Async async = context.async();
    final YearMonth reportStart = YearMonth.of(2018, 1);
    final int reportCount = 52;
    final List<String> errorCodes = List.of("3030", "3020", "3060", "3070", "other");
    final List<String> errorMsgs = List.of("Number=", "\"Code\":", "\"Code\": ", "error message");

    String udpId = "5edd37a4-6789-4f43-bb9d-850180470631";
    UsageDataProvider udp = new UsageDataProvider().withId(udpId);

    List<CounterReport> reports =
        IntStream.rangeClosed(1, reportCount)
            .mapToObj(i -> reportStart.plusMonths(i - 1))
            .map(
                ym ->
                    new CounterReport()
                        .withId(null)
                        .withProviderId(udpId)
                        .withYearMonth(ym.toString())
                        .withReportName("JR1")
                        .withRelease("4")
                        .withFailedAttempts(1)
                        .withFailedReason(
                            errorMsgs
                                .get(RandomUtils.nextInt(0, errorMsgs.size()))
                                .concat(
                                    errorCodes.get(RandomUtils.nextInt(0, errorCodes.size() - 1)))))
            .collect(Collectors.toList());

    Promise<String> saveUDPPromise = Promise.promise();
    getPGClient().save(TABLE_NAME_UDP, udpId, udp, saveUDPPromise);
    saveUDPPromise
        .future()
        .compose(
            s ->
                CompositeFuture.all(
                    reports.stream()
                        .map(
                            cr -> {
                              Promise<String> promise = Promise.promise();
                              getPGClient().save(TABLE_NAME_COUNTER_REPORTS, cr, promise);
                              return promise.future();
                            })
                        .collect(Collectors.toList())))
        .compose(
            cf -> {
              Promise<UsageDataProvider> result = Promise.promise();
              getPGClient().getById(TABLE_NAME_UDP, udpId, UsageDataProvider.class, result);
              return result.future();
            })
        .onSuccess(
            result -> {
              context.verify(
                  v -> {
                    assertThat(result).isNotNull();
                    assertThat(result.getHasFailedReport()).isEqualTo(HasFailedReport.YES);
                    assertThat(result.getReportErrorCodes())
                        .doesNotHaveDuplicates()
                        .isSubsetOf(errorCodes);
                    assertThat(result.getEarliestReport()).isNull();
                    assertThat(result.getLatestReport()).isNull();
                  });
              async.complete();
            })
        .onFailure(context::fail);
  }

  @Test
  public void testThatLatestReportAndEarliestReportAreNull(TestContext context) {
    Async async = context.async();
    String udpId = "c74d2bef-330a-43ea-8463-1080e22838cf";
    UsageDataProvider udp = new UsageDataProvider().withId(udpId);

    CounterReport failedReport =
        new CounterReport()
            .withProviderId(udpId)
            .withYearMonth("2018-01")
            .withRelease("4")
            .withReportName("JR1")
            .withFailedAttempts(1);

    Promise<String> saveUDPPromise = Promise.promise();
    getPGClient().save(TABLE_NAME_UDP, udpId, udp, saveUDPPromise);

    saveUDPPromise
        .future()
        .compose(
            s -> {
              Promise<String> promise = Promise.promise();
              getPGClient().save(TABLE_NAME_COUNTER_REPORTS, failedReport, promise);
              return promise.future();
            })
        .compose(
            s -> {
              Promise<UsageDataProvider> promise = Promise.promise();
              getPGClient().getById(TABLE_NAME_UDP, udpId, UsageDataProvider.class, promise);
              return promise.future();
            })
        .onSuccess(
            result -> {
              context.verify(
                  v -> {
                    assertThat(result).isNotNull();
                    assertThat(result.getLatestReport()).isNull();
                    assertThat(result.getEarliestReport()).isNull();
                  });
              async.complete();
            })
        .onFailure(context::fail);
  }

  @Test
  public void updateProviderReportDateOnInsertOrUpdate(TestContext context) {
    Async async = context.async();
    getPGClient()
        .upsert(
            TABLE_NAME_COUNTER_REPORTS,
            sampleReport.getId(),
            clone(sampleReport).withYearMonth("2017-01"),
            ar -> {
              if (ar.succeeded()) {
                getPGClient()
                    .getById(
                        TABLE_NAME_UDP,
                        sampleUDP.getId(),
                        UsageDataProvider.class,
                        ar2 -> {
                          if (ar2.succeeded()) {
                            context.verify(
                                v -> {
                                  assertThat(ar2.result().getLatestReport()).isEqualTo("2018-04");
                                  assertThat(ar2.result().getEarliestReport()).isEqualTo("2017-01");
                                });
                            async.complete();
                          } else {
                            context.fail(ar2.cause());
                          }
                        });
              } else {
                context.fail(ar.cause());
              }
            });

    async.await();
    Async async2 = context.async();
    getPGClient()
        .upsert(
            TABLE_NAME_COUNTER_REPORTS,
            sampleReport.getId(),
            clone(sampleReport).withYearMonth("2019-03"),
            ar -> {
              if (ar.succeeded()) {
                getPGClient()
                    .getById(
                        TABLE_NAME_UDP,
                        sampleUDP.getId(),
                        UsageDataProvider.class,
                        ar2 -> {
                          if (ar2.succeeded()) {
                            context.verify(
                                v -> {
                                  assertThat(ar2.result().getLatestReport()).isEqualTo("2019-03");
                                  assertThat(ar2.result().getEarliestReport()).isEqualTo("2018-02");
                                });
                            async2.complete();
                          } else {
                            context.fail(ar2.cause());
                          }
                        });
              } else {
                context.fail(ar.cause());
              }
            });

    // report with failedAttempts should be excluded
    async2.await();
    Async async3 = context.async();
    getPGClient()
        .upsert(
            TABLE_NAME_COUNTER_REPORTS,
            sampleReport.getId(),
            clone(sampleReport).withFailedAttempts(1),
            ar -> {
              if (ar.succeeded()) {
                getPGClient()
                    .getById(
                        TABLE_NAME_UDP,
                        sampleUDP.getId(),
                        UsageDataProvider.class,
                        ar2 -> {
                          if (ar2.succeeded()) {
                            context.verify(
                                v -> {
                                  assertThat(ar2.result().getLatestReport()).isEqualTo("2018-04");
                                  assertThat(ar2.result().getEarliestReport()).isEqualTo("2018-02");
                                });
                            async3.complete();
                          } else {
                            context.fail(ar2.cause());
                          }
                        });
              } else {
                context.fail(ar.cause());
              }
            });
  }

  @Test
  public void updateProviderReportDateOnDelete(TestContext context) {
    Async async = context.async();
    getPGClient()
        .delete(
            TABLE_NAME_COUNTER_REPORTS,
            "c07aa46b-fbca-45c8-bd44-c7f9a3648586", // Report 2018-04
            ar -> {
              if (ar.succeeded()) {
                getPGClient()
                    .getById(
                        TABLE_NAME_UDP,
                        sampleUDP.getId(),
                        UsageDataProvider.class,
                        ar2 -> {
                          if (ar2.succeeded()) {
                            context.verify(
                                v -> {
                                  assertThat(ar2.result().getLatestReport()).isEqualTo("2018-03");
                                  assertThat(ar2.result().getEarliestReport()).isEqualTo("2018-01");
                                });
                            async.complete();
                          } else {
                            context.fail(ar2.cause());
                          }
                        });
              } else {
                context.fail(ar.cause());
              }
            });

    async.await();
    Async async2 = context.async();
    getPGClient()
        .delete(
            TABLE_NAME_COUNTER_REPORTS,
            sampleReport.getId(), // Report 2018-01
            ar -> {
              if (ar.succeeded()) {
                getPGClient()
                    .getById(
                        TABLE_NAME_UDP,
                        sampleUDP.getId(),
                        UsageDataProvider.class,
                        ar2 -> {
                          if (ar2.succeeded()) {
                            context.verify(
                                v -> {
                                  assertThat(ar2.result().getLatestReport()).isEqualTo("2018-03");
                                  assertThat(ar2.result().getEarliestReport()).isEqualTo("2018-02");
                                });
                            async2.complete();
                          } else {
                            context.fail(ar2.cause());
                          }
                        });
              } else {
                context.fail(ar.cause());
              }
            });
  }

  @Test
  public void updateProviderHasFailedReportOnInsertOrUpdate(TestContext context) {
    Async async = context.async();
    getPGClient()
        .upsert(
            TABLE_NAME_COUNTER_REPORTS,
            sampleReport.getId(),
            sampleReport,
            ar -> {
              if (ar.succeeded()) {
                getPGClient()
                    .getById(
                        TABLE_NAME_UDP,
                        sampleUDP.getId(),
                        UsageDataProvider.class,
                        ar2 -> {
                          if (ar2.succeeded()) {
                            context.verify(
                                v ->
                                    assertThat(ar2.result().getHasFailedReport())
                                        .isEqualTo(HasFailedReport.NO));
                            async.complete();
                          } else {
                            context.fail(ar2.cause());
                          }
                        });
              } else {
                context.fail(ar.cause());
              }
            });

    async.await();
    Async async2 = context.async();
    getPGClient()
        .upsert(
            TABLE_NAME_COUNTER_REPORTS,
            reportFailed.getId(),
            reportFailed,
            ar -> {
              if (ar.succeeded()) {
                getPGClient()
                    .getById(
                        TABLE_NAME_UDP,
                        sampleUDP.getId(),
                        UsageDataProvider.class,
                        ar2 -> {
                          if (ar2.succeeded()) {
                            context.verify(
                                v ->
                                    assertThat(ar2.result().getHasFailedReport())
                                        .isEqualTo(HasFailedReport.YES));
                            getPGClient()
                                .delete(
                                    TABLE_NAME_COUNTER_REPORTS,
                                    reportFailed.getId(),
                                    ar3 -> {
                                      if (ar3.succeeded()) {
                                        getPGClient()
                                            .getById(
                                                TABLE_NAME_UDP,
                                                sampleUDP.getId(),
                                                UsageDataProvider.class,
                                                ar4 -> {
                                                  if (ar4.succeeded()) {
                                                    context.verify(
                                                        v ->
                                                            assertThat(
                                                                    ar4.result()
                                                                        .getHasFailedReport())
                                                                .isEqualTo(HasFailedReport.NO));
                                                    async2.complete();
                                                  } else {
                                                    context.fail(ar4.cause());
                                                  }
                                                });
                                      } else {
                                        context.fail(ar3.cause());
                                      }
                                    });
                          } else {
                            context.fail(ar2.cause());
                          }
                        });
              } else {
                context.fail(ar.cause());
              }
            });
  }

  @Test
  public void updateProviderReportTypeOnInsertOrUpdate(TestContext context) {
    Async async = context.async();
    getPGClient()
        .upsert(
            TABLE_NAME_COUNTER_REPORTS,
            sampleReport.getId(),
            clone(sampleReport).withReportName("DR"),
            ar -> {
              if (ar.succeeded()) {
                getPGClient()
                    .getById(
                        TABLE_NAME_UDP,
                        sampleUDP.getId(),
                        UsageDataProvider.class,
                        ar2 -> {
                          if (ar2.succeeded()) {
                            context.verify(
                                v ->
                                    assertThat(ar2.result().getReportTypes())
                                        .containsExactlyInAnyOrder("JR1", "DR"));
                            async.complete();
                          } else {
                            context.fail(ar2.cause());
                          }
                        });
              } else {
                context.fail(ar.cause());
              }
            });

    async.await();
    Async async2 = context.async();
    getPGClient()
        .upsert(
            TABLE_NAME_COUNTER_REPORTS,
            sampleReport.getId(),
            clone(sampleReport).withReportName("TR"),
            ar -> {
              if (ar.succeeded()) {
                getPGClient()
                    .getById(
                        TABLE_NAME_UDP,
                        sampleUDP.getId(),
                        UsageDataProvider.class,
                        ar2 -> {
                          if (ar2.succeeded()) {
                            context.verify(
                                v ->
                                    assertThat(ar2.result().getReportTypes())
                                        .containsExactlyInAnyOrder("JR1", "TR"));
                            async2.complete();
                          } else {
                            context.fail(ar2.cause());
                          }
                        });
              } else {
                context.fail(ar.cause());
              }
            });
  }

  @Test
  public void updateProviderReportTypeOnDelete(TestContext context) {
    String id = UUID.randomUUID().toString();
    Async async = context.async();
    getPGClient()
        .upsert(
            TABLE_NAME_COUNTER_REPORTS,
            id,
            clone(sampleReport).withReportName("PR").withId(id),
            ar -> {
              if (ar.succeeded()) {
                getPGClient()
                    .getById(
                        TABLE_NAME_UDP,
                        sampleUDP.getId(),
                        UsageDataProvider.class,
                        ar2 -> {
                          if (ar2.succeeded()) {
                            context.verify(
                                v ->
                                    assertThat(ar2.result().getReportTypes())
                                        .containsExactlyInAnyOrder("JR1", "PR"));
                            async.complete();
                          } else {
                            context.fail(ar2.cause());
                          }
                        });
              } else {
                context.fail(ar.cause());
              }
            });
    async.await();

    Async async2 = context.async();
    getPGClient()
        .delete(
            TABLE_NAME_COUNTER_REPORTS,
            id,
            ar -> {
              if (ar.succeeded()) {
                async2.complete();
              } else {
                context.fail(ar.cause());
              }
            });
    async2.await();

    Async async3 = context.async();
    getPGClient()
        .getById(
            TABLE_NAME_UDP,
            sampleUDP.getId(),
            UsageDataProvider.class,
            ar2 -> {
              if (ar2.succeeded()) {
                context.verify(
                    v -> assertThat(ar2.result().getReportTypes()).containsExactly("JR1"));
                async3.complete();
              } else {
                context.fail(ar2.cause());
              }
            });
  }
}
