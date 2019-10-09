package org.folio.rest.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.folio.rest.util.Constants.TABLE_NAME_COUNTER_REPORTS;

import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.folio.rest.jaxrs.model.CounterReport;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.PostgresClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class PgHelperIT {

  private static final String providerId = "81932a44-82ef-437e-8f53-c3fa508c0fb1";
  private static final String providerId2 = "6c0b057b-0bad-4559-93b8-b4d9b1062f40";
  private static final String tenant = "tenant1";

  @ClassRule public static EmbeddedPostgresRule pgRule = new EmbeddedPostgresRule(tenant);

  private static Vertx vertx = pgRule.vertx;

  @BeforeClass
  public static void setUp(TestContext context) {
    Async async = context.async();

    List<CounterReport> reports =
        Stream.of("2020-01", "2020-02", "2020-03", "2020-04")
            .map(
                s ->
                    new CounterReport()
                        .withId(UUID.nameUUIDFromBytes((providerId + s).getBytes()).toString())
                        .withProviderId(providerId)
                        .withRelease("4")
                        .withReportName("JR1")
                        .withYearMonth(s))
            .collect(Collectors.toList());
    reports.addAll(
        Stream.of("2019-01", "2019-02")
            .map(
                s ->
                    new CounterReport()
                        .withId(UUID.nameUUIDFromBytes((providerId2 + s).getBytes()).toString())
                        .withProviderId(providerId2)
                        .withRelease("4")
                        .withReportName("JR1")
                        .withYearMonth(s))
            .collect(Collectors.toList()));

    PostgresClient.getInstance(vertx, tenant)
        .saveBatch(
            TABLE_NAME_COUNTER_REPORTS,
            reports,
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
                            assertThat(ar2.result().getResults()).hasSize(6);
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

  @AfterClass
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
  }

  @Test
  public void testGetExistingReports(TestContext context) {
    Async async = context.async();

    PgHelper.getExistingReports(
            vertx.getOrCreateContext(),
            tenant,
            providerId,
            "JR1",
            "4",
            Arrays.asList("2019-01", "2019-02", "2020-01", "2020-02", "2020-03"))
        .setHandler(
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
            tenant,
            providerId2,
            "JR1",
            "4",
            Arrays.asList("2019-01", "2019-02", "2020-01", "2020-02", "2020-03"))
        .setHandler(
            ar -> {
              if (ar.succeeded()) {
                context.verify(v -> assertThat(ar.result()).hasSize(2));
                async.complete();
              } else {
                context.fail(ar.cause());
              }
            });
  }
}
