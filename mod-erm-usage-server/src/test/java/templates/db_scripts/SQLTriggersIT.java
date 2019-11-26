package templates.db_scripts;

import com.google.common.io.Resources;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.rest.RestVerticle;
import org.folio.rest.client.TenantClient;
import org.folio.rest.jaxrs.model.*;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.NetworkUtils;
import org.folio.rest.util.ModuleVersion;
import org.junit.*;
import org.junit.runner.RunWith;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.folio.rest.util.Constants.TABLE_NAME_COUNTER_REPORTS;
import static org.folio.rest.util.Constants.TABLE_NAME_UDP;

@RunWith(VertxUnitRunner.class)
public class SQLTriggersIT {

  private static final String TENANT = "testtenant";
  private static final String TABLE_AGGREGATOR = "aggregator_settings";
  private static Vertx vertx;
  private static int port;
  private static List<Parameter> parameters =
      Arrays.asList(
          new Parameter().withKey("loadSample").withValue("true"),
          new Parameter().withKey("loadReference").withValue("true"));
  private static TenantClient tenantClient;
  private static TenantAttributes tenantAttributes;
  private static AggregatorSetting sampleAggregator;
  private static UsageDataProvider sampleUDP;
  private static CounterReport sampleReport;
  private static CounterReport reportFailed;
  @Rule public Timeout timeout = Timeout.seconds(5);

  @BeforeClass
  public static void init(TestContext context) {
    vertx = Vertx.vertx();
    try {
      PostgresClient.setIsEmbedded(true);
      PostgresClient.getInstance(vertx).startEmbeddedPostgres();
      port = NetworkUtils.nextFreePort();
      tenantAttributes =
          new TenantAttributes()
              .withModuleTo(ModuleVersion.getModuleVersion())
              .withParameters(parameters);
      tenantClient = new TenantClient("http://localhost:" + port, TENANT, TENANT);

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
        new DeploymentOptions().setConfig(new JsonObject().put("http.port", port)).setWorker(true);
    vertx.deployVerticle(RestVerticle.class.getName(), options, context.asyncAssertSuccess());
  }

  @AfterClass
  public static void tearDown(TestContext context) {
    vertx.close(context.asyncAssertSuccess(res -> PostgresClient.stopEmbeddedPostgres()));
  }

  private static PostgresClient getPGClient() {
    return PostgresClient.getInstance(vertx, TENANT);
  }

  // FIXME: run in a sequence to prevent exception in StatsTracker.java
  private static Future<Void> validateSampleData() {
    Future<Void> providerFuture = Future.future();
    Future<Void> reportFuture = Future.future();
    Future<Void> aggregatorFuture = Future.future();
    getPGClient()
        .get(
            TABLE_NAME_UDP,
            new UsageDataProvider(),
            false,
            ar -> {
              if (ar.succeeded()) {
                assertThat(ar.result().getResults().size()).isEqualTo(4);
                providerFuture.complete();
              } else {
                providerFuture.fail(ar.cause());
              }
            });

    providerFuture.compose(
        v ->
            getPGClient()
                .get(
                    TABLE_NAME_COUNTER_REPORTS,
                    new CounterReport(),
                    false,
                    ar -> {
                      if (ar.succeeded()) {
                        assertThat(ar.result().getResults().size()).isEqualTo(4);
                        reportFuture.complete();
                      } else {
                        reportFuture.fail(ar.cause());
                      }
                    }),
        reportFuture);

    reportFuture.compose(
        v ->
            getPGClient()
                .get(
                    TABLE_AGGREGATOR,
                    new AggregatorSetting(),
                    false,
                    ar -> {
                      if (ar.succeeded()) {
                        assertThat(ar.result().getResults().size()).isEqualTo(1);
                        aggregatorFuture.complete();
                      } else {
                        aggregatorFuture.fail(ar.cause());
                      }
                    }),
        aggregatorFuture);
    return aggregatorFuture;
  }

  @Before
  public void before(TestContext context) {
    deleteSampleData()
        .compose(r -> loadSampleData())
        .compose(r -> validateSampleData())
        .setHandler(context.asyncAssertSuccess());
  }

  private Future<Integer> truncateTable(String tableName) {
    Future<Integer> future = Future.future();
    getPGClient()
        .delete(
            tableName,
            new Criterion(),
            ar -> future.complete((ar.succeeded()) ? ar.result().getUpdated() : null));
    return future;
  }

  private Future<Integer> deleteSampleData() {
    // return CompositeFuture.all(
    //    truncateTable(TABLE_NAME_UDP), truncateTable(TABLE_AGGREGATOR),
    // truncateTable(TABLE_NAME_COUNTER_REPORTS));

    // FIXME: run in a sequence to prevent exception in StatsTracker.java
    return truncateTable(TABLE_NAME_UDP)
        .compose(i -> truncateTable(TABLE_AGGREGATOR))
        .compose(i -> truncateTable(TABLE_NAME_COUNTER_REPORTS));
  }

  private Future<Void> loadSampleData() {
    Future<Void> future = Future.future();
    try {
      tenantClient.postTenant(
          tenantAttributes,
          res -> {
            if (res.statusCode() / 200 == 1) {
              future.complete();
            } else {
              future.fail(
                  String.format(
                      "Tenantloading returned %s %s", res.statusCode(), res.statusMessage()));
            }
          });
    } catch (Exception e) {
      future.fail(e);
    }
    return future;
  }

  private <T extends Object> T clone(T o) {
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
  public void deleteCounterReports(TestContext context) {
    getPGClient()
        .delete(
            TABLE_NAME_UDP,
            "e67924ee-aa00-454e-8fd0-c3f81339d20e",
            context.asyncAssertSuccess(
                ur ->
                    getPGClient()
                        .get(
                            TABLE_NAME_COUNTER_REPORTS,
                            new CounterReport(),
                            false,
                            context.asyncAssertSuccess(
                                res ->
                                    context.verify(
                                        v -> assertThat(res.getResults().size()).isEqualTo(0))))));
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
                            assertThat(ar2.result().getLatestReport()).isEqualTo("2018-04");
                            assertThat(ar2.result().getEarliestReport()).isEqualTo("2017-01");
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
                            assertThat(ar2.result().getLatestReport()).isEqualTo("2019-03");
                            assertThat(ar2.result().getEarliestReport()).isEqualTo("2018-02");
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
                            assertThat(ar2.result().getLatestReport()).isEqualTo("2018-04");
                            assertThat(ar2.result().getEarliestReport()).isEqualTo("2018-02");
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
                            assertThat(ar2.result().getLatestReport()).isEqualTo("2018-03");
                            assertThat(ar2.result().getEarliestReport()).isEqualTo("2018-01");
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
                            assertThat(ar2.result().getLatestReport()).isEqualTo("2018-03");
                            assertThat(ar2.result().getEarliestReport()).isEqualTo("2018-02");
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
                            assertThat(ar2.result().getHasFailedReport())
                                .isEqualTo(UsageDataProvider.HasFailedReport.NO);
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
                            assertThat(ar2.result().getHasFailedReport())
                                .isEqualTo(UsageDataProvider.HasFailedReport.YES);
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
                                                    assertThat(ar4.result().getHasFailedReport())
                                                        .isEqualTo(
                                                            UsageDataProvider.HasFailedReport.NO);
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
}
