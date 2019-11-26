package org.folio.rest.impl;

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
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.rest.RestVerticle;
import org.folio.rest.client.TenantClient;
import org.folio.rest.jaxrs.model.CounterReport;
import org.folio.rest.jaxrs.model.CounterReports;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.NetworkUtils;
import org.folio.rest.util.Constants;
import org.folio.rest.util.ModuleVersion;
import org.joda.time.LocalDate;
import org.junit.*;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import static io.restassured.RestAssured.get;
import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

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
                new TenantAttributes().withModuleTo(ModuleVersion.getModuleVersion()),
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

  private String resourceToString(String path) {
    try {
      return Resources.asCharSource(Resources.getResource(path), StandardCharsets.UTF_8).read();
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
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
  }

  @Test
  public void testGetCSVOkJR1MultipleMonths() throws IOException {
    String json1 = resourceToString("JR1/jr1_1.json");
    String json2 = resourceToString("JR1/jr1_2.json");
    String json3 = resourceToString("JR1/jr1_3.json");

    given().body(json1).post().then().statusCode(201);
    given().body(json2).post().then().statusCode(201);
    given().body(json3).post().then().statusCode(201);

    given()
        .pathParam("id", "4b659cb9-e4bb-493d-ae30-5f5690c54802")
        .pathParam("name", "JR1")
        .pathParam("version", "4")
        .pathParam("begin", "2018-12")
        .pathParam("end", "2019-03")
        .get("/csv/provider/{id}/report/{name}/version/{version}/from/{begin}/to/{end}")
        .then()
        .statusCode(200)
        .body(
            containsString(
                "19th-Century Music,University of California Press,Ithaka,,,0148-2076,1533-8606,6,3,3,2,2,2"));
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
  }
}
