package org.folio.mod_erm_usage_test;

import static io.restassured.RestAssured.get;
import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.rest.RestVerticle;
import org.folio.rest.client.TenantClient;
import org.folio.rest.jaxrs.model.CounterReport;
import org.folio.rest.jaxrs.model.CounterReports;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.NetworkUtils;
import org.joda.time.LocalDate;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.google.common.net.HttpHeaders;
import com.google.common.net.MediaType;
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

@RunWith(VertxUnitRunner.class)
public class CounterReportCsvIT {

  private static final String TENANT = "diku";
  private static Vertx vertx;

  private static CounterReport counterReport;
  private static String expected;

  @Rule public Timeout timeout = Timeout.seconds(10);

  @BeforeClass
  public static void setUp(TestContext context) {
    vertx = Vertx.vertx();

    try {
      String reportStr =
          new String(Files.readAllBytes(Paths.get("../ramls/examples/counterreport.sample")));
      counterReport = Json.decodeValue(reportStr, CounterReport.class);
      expected =
          Resources.toString(Resources.getResource("JR1/counterreport.csv"), Charsets.UTF_8)
              .replace("$$$date_run$$$", LocalDate.now().toString());
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
                null,
                res2 -> {
                  async.complete();
                });
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

  private void testThatDBIsEmpty() {
    int size =
        get().then().statusCode(200).extract().as(CounterReports.class).getCounterReports().size();
    assertThat(size).isEqualTo(0);
  }

  @Test
  public void testGetCSVOkJR1() {
    given().body(counterReport).post().then().statusCode(201);

    String csvResult =
        given()
            .pathParam("id", counterReport.getId())
            .get("/csv/{id}")
            .then()
            .statusCode(200)
            .contentType(equalTo("text/csv"))
            .extract()
            .asString();
    assertThat(csvResult).isEqualToNormalizingNewlines(expected);

    given().pathParam("id", counterReport.getId()).delete("/{id}").then().statusCode(204);
    testThatDBIsEmpty();
  }

  @Test
  public void testGetCSVNoMapper() {
    CounterReport badReleaseNo =
        Json.decodeValue(Json.encode(counterReport), CounterReport.class).withRelease("1");
    given().body(badReleaseNo).post().then().statusCode(201);

    given()
        .pathParam("id", badReleaseNo.getId())
        .get("/csv/{id}")
        .then()
        .statusCode(500)
        .body(containsString("no mapper"));

    given().pathParam("id", badReleaseNo.getId()).delete("/{id}");
    testThatDBIsEmpty();
  }

  @Test
  public void testGetCSVNoReport() {
    CounterReport noReport =
        Json.decodeValue(Json.encode(counterReport), CounterReport.class).withReport(null);
    given().body(noReport).post().then().statusCode(201);

    given()
        .pathParam("id", noReport.getId())
        .get("/csv/{id}")
        .then()
        .statusCode(500)
        .body(containsString("No report"));

    given().pathParam("id", noReport.getId()).delete("/{id}");
    testThatDBIsEmpty();
  }
}
