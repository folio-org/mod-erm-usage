package org.folio.rest.impl;

import static io.restassured.RestAssured.get;
import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.equalTo;

import com.google.common.net.HttpHeaders;
import com.google.common.net.MediaType;
import com.google.gson.Gson;
import io.restassured.RestAssured;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.filter.log.RequestLoggingFilter;
import io.restassured.filter.log.ResponseLoggingFilter;
import io.restassured.parsing.Parser;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.rest.RestVerticle;
import org.folio.rest.client.TenantClient;
import org.folio.rest.jaxrs.model.CounterReport;
import org.folio.rest.jaxrs.model.CounterReports;
import org.folio.rest.jaxrs.model.CounterReportsSorted;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.NetworkUtils;
import org.folio.rest.util.Constants;
import org.folio.rest.util.ModuleVersion;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class CounterReportSortedIT {

  private static final String TENANT = "diku";
  private static Vertx vertx;

  private static CounterReport baseReport;
  private static List<CounterReport> counterReports;

  @Rule public Timeout timeout = Timeout.seconds(10);

  @BeforeClass
  public static void setUp(TestContext context) {
    vertx = Vertx.vertx();

    try {
      String reportStr =
          new String(Files.readAllBytes(Paths.get("../ramls/examples/counterreport.sample")));
      baseReport = Json.decodeValue(reportStr, CounterReport.class);
      counterReports = createReportsForYearsFrom(baseReport);
    } catch (Exception ex) {
      context.fail(ex);
    }

    try {
      PostgresClient.setIsEmbedded(true);
      PostgresClient instance = PostgresClient.getInstance(vertx);
      instance.startEmbeddedPostgres();
    } catch (Exception e) {
      e.printStackTrace();
      context.fail(e);
      return;
    }

    Async async = context.async();
    int port = NetworkUtils.nextFreePort();

    RestAssured.reset();
    RestAssured.baseURI = "http://localhost";
    RestAssured.basePath = "/counter-reports";
    RestAssured.port = port;
    RestAssured.defaultParser = Parser.JSON;
    RestAssured.filters(new ResponseLoggingFilter(), new RequestLoggingFilter());
    RestAssured.requestSpecification =
        new RequestSpecBuilder()
            .addHeader(XOkapiHeaders.TENANT, TENANT)
            .addHeader(HttpHeaders.CONTENT_TYPE, MediaType.JSON_UTF_8.toString())
            .build();

    TenantClient tenantClient = new TenantClient("http://localhost:" + port, TENANT, TENANT);
    DeploymentOptions options =
        new DeploymentOptions().setConfig(new JsonObject().put("http.port", port)).setWorker(true);
    vertx.deployVerticle(
        RestVerticle.class.getName(),
        options,
        res -> {
          try {
            tenantClient.postTenant(
                new TenantAttributes().withModuleTo(ModuleVersion.getModuleVersion()),
                res2 -> async.complete());
          } catch (Exception e) {
            context.fail(e);
          }
        });
  }

  @AfterClass
  public static void teardown(TestContext context) {
    RestAssured.reset();
    Async async = context.async();
    vertx.close(
        context.asyncAssertSuccess(
            res -> {
              PostgresClient.stopEmbeddedPostgres();
              async.complete();
            }));
  }

  private static List<CounterReport> createReportsForYearsFrom(CounterReport base) {
    Gson gson = new Gson();
    List<CounterReport> result = new ArrayList<>();
    for (int y = 2018; y <= 2019; y++) {
      for (int m = 1; m <= 12; m++) {
        CounterReport JR1 = gson.fromJson(gson.toJson(base), CounterReport.class);
        String month = m < 10 ? "0" + m : String.valueOf(m);
        JR1.setYearMonth(y + "-" + month);
        JR1.setId(UUID.randomUUID().toString());
        result.add(JR1);

        CounterReport TR1 = gson.fromJson(gson.toJson(base), CounterReport.class);
        TR1.setReportName("TR1");
        TR1.setYearMonth(y + "-" + month);
        TR1.setId(UUID.randomUUID().toString());
        result.add(TR1);
      }
    }
    return result;
  }

  @Before
  public void before(TestContext ctx) {
    Async async = ctx.async();
    PostgresClient.getInstance(vertx, TENANT)
        .delete(
            Constants.TABLE_NAME_COUNTER_REPORTS,
            new Criterion(),
            ar -> {
              if (ar.failed()) ctx.fail(ar.cause());
              async.complete();
            });
    async.await();

    testThatDBIsEmpty();
  }

  private void testThatDBIsEmpty() {
    int size =
        get().then().statusCode(200).extract().as(CounterReports.class).getCounterReports().size();
    assertThat(size).isEqualTo(0);
  }

  @Test
  public void testGetSorted() {
    counterReports.forEach(cr -> given().body(cr).post().then().statusCode(201));

    CounterReportsSorted sortedResult =
        given()
            .pathParam("id", baseReport.getProviderId())
            .get("/sorted/{id}")
            .then()
            .statusCode(200)
            .contentType(equalTo("application/json"))
            .extract()
            .as(CounterReportsSorted.class);
    sortedResult
        .getCounterReportsPerYear()
        .forEach(
            counterReportsPerYear -> {
              Integer currentYear = counterReportsPerYear.getYear();
              counterReportsPerYear
                  .getReportsPerType()
                  .forEach(
                      reportsPerType -> {
                        String reportType = reportsPerType.getReportType();
                        reportsPerType
                            .getCounterReports()
                            .forEach(
                                counterReport -> {
                                  assertThat(counterReport.getReportName()).isEqualTo(reportType);
                                  assertThat(counterReport.getYearMonth().substring(0, 4))
                                      .isEqualTo(currentYear.toString());
                                });
                      });
            });
  }
}
