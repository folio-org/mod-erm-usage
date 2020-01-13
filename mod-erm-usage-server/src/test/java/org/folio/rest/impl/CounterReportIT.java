package org.folio.rest.impl;

import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import io.restassured.parsing.Parser;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.rest.RestVerticle;
import org.folio.rest.client.TenantClient;
import org.folio.rest.jaxrs.model.CounterReport;
import org.folio.rest.jaxrs.model.ErrorCodes;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.jaxrs.model.UsageDataProvider;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.NetworkUtils;
import org.folio.rest.util.ModuleVersion;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.equalTo;

@RunWith(VertxUnitRunner.class)
public class CounterReportIT {

  private static final String APPLICATION_JSON = "application/json";
  private static final String BASE_URI = "/counter-reports";
  private static final String TENANT = "diku";
  private static Vertx vertx;
  private static CounterReport report;
  private static CounterReport reportChanged;
  private static CounterReport reportFailedReason3000;
  private static CounterReport reportFailedReason3031;
  private static CounterReport reportFailedReasonUnkownHost;

  @Rule public Timeout timeout = Timeout.seconds(10);

  @BeforeClass
  public static void setUp(TestContext context) {
    vertx = Vertx.vertx();

    try {
      String reportStr =
          new String(Files.readAllBytes(Paths.get("../ramls/examples/counterreport.sample")));
      report = Json.decodeValue(reportStr, CounterReport.class);
      reportChanged = Json.decodeValue(reportStr, CounterReport.class).withRelease("5");

      String reportFailedReasonStr_3000 =
          new String(
              Files.readAllBytes(Paths.get("../ramls/examples/counterreport_failed_3000.sample")));
      reportFailedReason3000 = Json.decodeValue(reportFailedReasonStr_3000, CounterReport.class);

      String reportFailedReasonStr_3031 =
          new String(
              Files.readAllBytes(Paths.get("../ramls/examples/counterreport_failed_3031.sample")));
      reportFailedReason3031 = Json.decodeValue(reportFailedReasonStr_3031, CounterReport.class);

      String reportFailedReasonStr_unknownHost =
          new String(
              Files.readAllBytes(
                  Paths.get("../ramls/examples/counterreport_failed_unknown_host.sample")));
      reportFailedReasonUnkownHost =
          Json.decodeValue(reportFailedReasonStr_unknownHost, CounterReport.class);
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
    RestAssured.port = port;
    RestAssured.defaultParser = Parser.JSON;

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

  @Test
  public void checkThatWeCanAddGetPutAndDeleteCounterReports() {
    // POST
    given()
        .body(Json.encode(report))
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", APPLICATION_JSON)
        .post(BASE_URI)
        .then()
        .statusCode(201)
        .body("release", equalTo(report.getRelease()))
        .body("id", equalTo(report.getId()));

    // GET
    given()
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", APPLICATION_JSON)
        .get(BASE_URI + "/" + report.getId())
        .then()
        .contentType(ContentType.JSON)
        .statusCode(200)
        .body("id", equalTo(report.getId()));

    // PUT
    given()
        .body(Json.encode(reportChanged))
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", "text/plain")
        .put(BASE_URI + "/" + report.getId())
        .then()
        .statusCode(204);

    // GET again
    given()
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", APPLICATION_JSON)
        .get(BASE_URI + "/" + report.getId())
        .then()
        .statusCode(200)
        .body("id", equalTo(reportChanged.getId()))
        .body("release", equalTo(reportChanged.getRelease()));

    // DELETE
    given()
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", "text/plain")
        .delete(BASE_URI + "/" + report.getId())
        .then()
        .statusCode(204);

    // GET again
    given()
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", APPLICATION_JSON)
        .get(BASE_URI + "/" + report.getId())
        .then()
        .statusCode(404);
  }

  @Test
  public void checkThatWeCanSearchByCQL() {
    given()
        .body(Json.encode(report))
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", APPLICATION_JSON)
        .post(BASE_URI)
        .then()
        .statusCode(201)
        .body("id", equalTo(report.getId()));

    String cqlReport = "?query=(report=\"semantico*\")";
    given()
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", APPLICATION_JSON)
        .get(BASE_URI + cqlReport)
        .then()
        .contentType(ContentType.JSON)
        .statusCode(200)
        .body("counterReports.size()", equalTo(1))
        .body("counterReports[0].id", equalTo(report.getId()))
        .body("counterReports[0].release", equalTo(report.getRelease()));

    String cqlReport2 = "?query=(report=\"someStringThatIsNotInTheReport*\")";
    given()
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", APPLICATION_JSON)
        .get(BASE_URI + cqlReport2)
        .then()
        .contentType(ContentType.JSON)
        .statusCode(200)
        .body("counterReports.size()", equalTo(0));

    String cqlReportName = "?query=(reportName==\"JR1\")";
    given()
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", APPLICATION_JSON)
        .get(BASE_URI + cqlReportName)
        .then()
        .contentType(ContentType.JSON)
        .statusCode(200)
        .body("counterReports.size()", equalTo(1))
        .body("counterReports[0].id", equalTo(report.getId()))
        .body("counterReports[0].release", equalTo(report.getRelease()));

    // DELETE
    given()
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", "text/plain")
        .delete(BASE_URI + "/" + report.getId())
        .then()
        .statusCode(204);
  }

  @Test
  public void checkThatInvalidCounterReportIsNotPosted() {
    CounterReport invalidReport =
        Json.decodeValue(Json.encode(report), CounterReport.class).withYearMonth(null);
    given()
        .body(Json.encode(invalidReport))
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", APPLICATION_JSON)
        .post(BASE_URI)
        .then()
        .statusCode(422);
  }

  @Test
  public void checkThatCounterReportFailedReasonTriggerIsExecuted(TestContext context) {
    UsageDataProvider udprovider = null;
    try {
      String udproviderStr =
          new String(Files.readAllBytes(Paths.get("../ramls/examples/udproviders.sample")));
      udprovider = Json.decodeValue(udproviderStr, UsageDataProvider.class);

    } catch (IOException e) {
      context.fail();
    }

    // POST usage data provider
    given()
        .body(Json.encode(udprovider))
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", APPLICATION_JSON)
        .post("/usage-data-providers")
        .then()
        .statusCode(201);

    // POST report
    given()
        .body(Json.encode(reportFailedReason3000))
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", APPLICATION_JSON)
        .post(BASE_URI)
        .then()
        .statusCode(201)
        .body("release", equalTo(reportFailedReason3000.getRelease()))
        .body("id", equalTo(reportFailedReason3000.getId()));

    // Check if hasFailedReport and errorCode of UDP is set
    UsageDataProvider udp =
        given()
            .body(Json.encode(udprovider))
            .header("X-Okapi-Tenant", TENANT)
            .header("content-type", APPLICATION_JSON)
            .header("accept", APPLICATION_JSON)
            .request()
            .get("/usage-data-providers/" + udprovider.getId())
            .thenReturn()
            .as(UsageDataProvider.class);
    assertThat(udp.getLabel()).isEqualTo(udprovider.getLabel());
    assertThat(udp.getId()).isNotEmpty();
    assertThat(udp.getHasFailedReport().value())
        .isEqualTo(UsageDataProvider.HasFailedReport.YES.value());

    List<String> udpErrorCodes = udp.getReportErrorCodes();
    assertThat(udpErrorCodes.size()).isEqualTo(1);
    assertThat(reportFailedReason3000.getFailedReason().contains(udpErrorCodes.get(0)));

    // DELETE report
    given()
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", "text/plain")
        .delete(BASE_URI + "/" + reportFailedReason3000.getId())
        .then()
        .statusCode(204);

    // DELETE udp
    given()
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", "text/plain")
        .delete("/usage-data-providers/" + udprovider.getId())
        .then()
        .statusCode(204);
  }

  @Test
  public void checkThatWeGetErrorCodes(TestContext context) {
    // POST reports
    given()
        .body(Json.encode(reportFailedReason3000))
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", APPLICATION_JSON)
        .post(BASE_URI)
        .then()
        .statusCode(201)
        .body("release", equalTo(reportFailedReason3000.getRelease()))
        .body("id", equalTo(reportFailedReason3000.getId()));

    given()
        .body(Json.encode(reportFailedReason3031))
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", APPLICATION_JSON)
        .post(BASE_URI)
        .then()
        .statusCode(201)
        .body("release", equalTo(reportFailedReason3031.getRelease()))
        .body("id", equalTo(reportFailedReason3031.getId()));

    given()
        .body(Json.encode(reportFailedReasonUnkownHost))
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", APPLICATION_JSON)
        .post(BASE_URI)
        .then()
        .statusCode(201)
        .body("release", equalTo(reportFailedReasonUnkownHost.getRelease()))
        .body("id", equalTo(reportFailedReasonUnkownHost.getId()));

    // GET error codes
    ErrorCodes errorCodes =
        given()
            .header("X-Okapi-Tenant", TENANT)
            .header("content-type", APPLICATION_JSON)
            .header("accept", APPLICATION_JSON)
            .request()
            .get("/counter-reports/errors/codes")
            .thenReturn()
            .as(ErrorCodes.class);

    assertThat(errorCodes.getErrorCodes().size()).isEqualTo(3);
    assertThat(errorCodes.getErrorCodes().containsAll(Arrays.asList("3000", "3031", "other")));

    // DELETE reports
    given()
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", "text/plain")
        .delete(BASE_URI + "/" + reportFailedReason3000.getId())
        .then()
        .statusCode(204);

    given()
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", "text/plain")
        .delete(BASE_URI + "/" + reportFailedReason3031.getId())
        .then()
        .statusCode(204);

    given()
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", "text/plain")
        .delete(BASE_URI + "/" + reportFailedReasonUnkownHost.getId())
        .then()
        .statusCode(204);
  }
}
