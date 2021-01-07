package org.folio.rest.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.folio.rest.util.Constants.TABLE_NAME_COUNTER_REPORTS;

import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Spliterator;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.Stream.Builder;
import java.util.stream.StreamSupport;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.rest.jaxrs.model.CounterReport;
import org.folio.rest.persist.Criteria.Criteria;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.cql.CQLWrapper;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class PgHelperIT {

  private static final String providerId = "81932a44-82ef-437e-8f53-c3fa508c0fb1";
  private static final String providerId2 = "6c0b057b-0bad-4559-93b8-b4d9b1062f40";
  private static final String tenant = "tenant1";
  private static final Map<String, String> okapiHeaders =
      Collections.singletonMap(XOkapiHeaders.TENANT.toLowerCase(), tenant);

  private static Vertx vertx = Vertx.vertx();
  @ClassRule public static EmbeddedPostgresRule pgRule = new EmbeddedPostgresRule(vertx, tenant);

  private static List<CounterReport> reports1;
  private static List<CounterReport> reports2;
  private static List<CounterReport> reports3;
  private static List<String> uuids;

  static {
    reports1 =
        Stream.of("2020-01", "2020-02", "2020-03", "2020-04")
            .map(
                s ->
                    new CounterReport()
                        .withProviderId(providerId)
                        .withRelease("4")
                        .withReportName("JR1")
                        .withYearMonth(s))
            .collect(Collectors.toList());
    reports2 =
        Stream.of("2019-01", "2019-02")
            .map(
                s ->
                    new CounterReport()
                        .withProviderId(providerId2)
                        .withRelease("4")
                        .withReportName("JR1")
                        .withYearMonth(s))
            .collect(Collectors.toList());
    reports3 =
        Stream.of("2019-01", "2019-02")
            .map(
                s ->
                    new CounterReport()
                        .withProviderId(providerId2)
                        .withRelease("4")
                        .withReportName("PR1")
                        .withYearMonth(s))
            .collect(Collectors.toList());
  }

  private static Stream<Row> getRowSetResults(RowSet<Row> rowset) {
    Builder<Spliterator<Row>> sb = Stream.builder();
    while (rowset != null) {
      sb.add(rowset.spliterator());
      rowset = rowset.next();
    }
    return sb.build().flatMap(spliterator -> StreamSupport.stream(spliterator, false));
  }

  @BeforeClass
  public static void setUp(TestContext context) {
    Async async = context.async();

    PostgresClient.getInstance(vertx, tenant)
        .saveBatch(
            TABLE_NAME_COUNTER_REPORTS,
            Stream.concat(reports1.stream(), reports2.stream()).collect(Collectors.toList()),
            ar -> {
              if (ar.succeeded()) {
                uuids =
                    getRowSetResults(ar.result())
                        .map(row -> row.getUUID(0))
                        .filter(Objects::nonNull)
                        .map(Objects::toString)
                        .collect(Collectors.toList());

                CQLWrapper cql = new CQLWrapper(new Criterion());
                PostgresClient.getInstance(vertx, tenant)
                    .get(
                        TABLE_NAME_COUNTER_REPORTS,
                        CounterReport.class,
                        cql,
                        false,
                        ar2 -> {
                          if (ar2.succeeded()) {
                            assertThat(ar2.result().getResults()).hasSize(6);
                            assertThat(ar2.result().getResults().stream().map(CounterReport::getId))
                                .containsExactlyInAnyOrder(uuids.toArray(new String[0]));
                            async.complete();
                          } else {
                            context.fail(ar2.cause());
                          }
                        });

              } else {
                context.fail(ar.cause());
              }
            });
  }

  /*  @AfterClass
  public static void tearDown(TestContext context) {
    Async async = context.async();
    PostgresClient.getInstance(vertx, tenant)
        .delete(
            TABLE_NAME_COUNTER_REPORTS,
            new Criterion(),
            ar -> {
              if (ar.succeeded()) {
                PostgresClient.getInstance(vertx, tenant)
                    .get(
                        TABLE_NAME_COUNTER_REPORTS,
                        CounterReport.class,
                        new Criterion(),
                        false,
                        ar2 -> {
                          if (ar2.succeeded()) {
                            assertThat(ar2.result().getResults()).isEmpty();
                            async.complete();
                          } else {
                            context.fail(ar2.cause());
                          }
                        });
              } else {
                context.fail(ar.cause());
              }
            });
  }*/

  @Test
  public void testGetExistingReports(TestContext context) {
    Async async = context.async();

    PgHelper.getExistingReports(
            vertx.getOrCreateContext(),
            okapiHeaders,
            providerId,
            "JR1",
            "4",
            Arrays.asList("2019-01", "2019-02", "2020-01", "2020-02", "2020-03"))
        .onComplete(
            ar -> {
              if (ar.succeeded()) {
                context.verify(v -> assertThat(ar.result()).hasSize(3));
                async.complete();
              } else {
                context.fail(ar.cause());
              }
            });
  }

  @Test
  public void testGetExistingReports2(TestContext context) {
    Async async = context.async();

    PgHelper.getExistingReports(
            vertx.getOrCreateContext(),
            okapiHeaders,
            providerId2,
            "JR1",
            "4",
            Arrays.asList("2019-01", "2019-02", "2020-01", "2020-02", "2020-03"))
        .onComplete(
            ar -> {
              if (ar.succeeded()) {
                context.verify(v -> assertThat(ar.result()).hasSize(2));
                async.complete();
              } else {
                context.fail(ar.cause());
              }
            });
  }

  @Test
  public void testSaveReportsToDb(TestContext context) {
    Async async = context.async();
    reports2.forEach(li -> li.withFailedAttempts(5));
    CQLWrapper cql = createGetCounterReportCQL(providerId2, "4", "JR1");

    PgHelper.saveCounterReportsToDb(vertx.getOrCreateContext(), okapiHeaders, reports2, false)
        .onComplete(
            ar -> {
              if (ar.succeeded()) {
                context.fail("Should fail");
              } else {
                assertThat(ar.cause().getMessage()).contains("2019-01", "2019-02");

                PostgresClient.getInstance(vertx, tenant)
                    .get(
                        TABLE_NAME_COUNTER_REPORTS,
                        CounterReport.class,
                        cql,
                        false,
                        result -> {
                          if (result.succeeded()) {
                            context.verify(
                                v -> {
                                  assertThat(result.result().getResults()).hasSize(2);
                                  assertThat(result.result().getResults())
                                      .allSatisfy(
                                          cr -> assertThat(cr.getFailedAttempts()).isNull());
                                });
                            async.complete();
                          } else {
                            context.fail(result.cause());
                          }
                        });
              }
            });
    async.awaitSuccess(3000);

    Async async2 = context.async();
    reports2.forEach(li -> li.withFailedAttempts(5));
    PgHelper.saveCounterReportsToDb(vertx.getOrCreateContext(), okapiHeaders, reports2, true)
        .onComplete(
            ar -> {
              if (ar.succeeded()) {
                assertThat(ar.result()).hasSize(2);
                PostgresClient.getInstance(vertx, tenant)
                    .get(
                        TABLE_NAME_COUNTER_REPORTS,
                        CounterReport.class,
                        cql,
                        false,
                        result -> {
                          if (result.succeeded()) {
                            context.verify(
                                v -> {
                                  assertThat(result.result().getResults()).hasSize(2);
                                  assertThat(
                                          result.result().getResults().stream()
                                              .map(CounterReport::getId))
                                      .containsExactlyInAnyOrder(
                                          ar.result().toArray(new String[0]));
                                  assertThat(result.result().getResults())
                                      .allSatisfy(
                                          cr -> assertThat(cr.getFailedAttempts()).isEqualTo(5));
                                });
                            async2.complete();
                          } else {
                            context.fail(result.cause());
                          }
                        });
              } else {
                context.fail(ar.cause());
              }
            });
  }

  @Test
  public void testSaveReportsToDb2(TestContext context) {
    Async async = context.async();
    CQLWrapper cql = createGetCounterReportCQL(providerId2, "4", "PR1");

    PgHelper.saveCounterReportsToDb(vertx.getOrCreateContext(), okapiHeaders, reports3, false)
        .onComplete(
            ar -> {
              if (ar.succeeded()) {
                assertThat(ar.result()).hasSize(2);
                PostgresClient.getInstance(vertx, tenant)
                    .get(
                        TABLE_NAME_COUNTER_REPORTS,
                        CounterReport.class,
                        cql,
                        false,
                        result -> {
                          if (result.succeeded()) {
                            context.verify(
                                v -> {
                                  assertThat(result.result().getResults()).hasSize(2);
                                  assertThat(
                                          result.result().getResults().stream()
                                              .map(CounterReport::getId))
                                      .containsExactlyInAnyOrder(
                                          ar.result().toArray(new String[0]));
                                });
                            async.complete();
                          } else {
                            context.fail(result.cause());
                          }
                        });
              } else {
                context.fail(ar.cause());
              }
            });
  }

  private CQLWrapper createGetCounterReportCQL(
      String providerId, String release, String reportName) {
    Criteria idCrit =
        new Criteria()
            .addField(Constants.FIELD_NAME_PROVIDER_ID)
            .setJSONB(true)
            .setOperation("=")
            .setVal(providerId);
    Criteria releaseCrit =
        new Criteria().addField(Constants.FIELD_NAME_RELEASE).setOperation("=").setVal(release);
    Criteria reportNameCrit =
        new Criteria()
            .addField(Constants.FIELD_NAME_REPORT_NAME)
            .setOperation("=")
            .setVal(reportName);
    Criterion criterion =
        new Criterion().addCriterion(idCrit).addCriterion(releaseCrit).addCriterion(reportNameCrit);
    return new CQLWrapper(criterion);
  }
}
