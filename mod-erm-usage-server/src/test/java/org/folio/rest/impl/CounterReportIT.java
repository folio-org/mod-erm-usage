package org.folio.rest.impl;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.equalTo;

import com.google.common.io.Resources;
import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import io.restassured.parsing.Parser;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.rest.RestVerticle;
import org.folio.rest.jaxrs.model.CounterReport;
import org.folio.rest.jaxrs.model.ErrorCodes;
import org.folio.rest.jaxrs.model.Report;
import org.folio.rest.jaxrs.model.UsageDataProvider;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.NetworkUtils;
import org.folio.rest.util.Constants;
import org.folio.rest.util.EmbeddedPostgresRule;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.olf.erm.usage.counter41.Counter4Utils;

@RunWith(VertxUnitRunner.class)
public class CounterReportIT {

  private static final String APPLICATION_JSON = "application/json";
  private static final String BASE_URI = "/counter-reports";
  private static final String BASE_URI_DOWNLOAD = BASE_URI.concat("/{id}/download");
  private static final String TENANT = "diku";
  private static final Vertx vertx = Vertx.vertx();
  private static CounterReport report;
  private static CounterReport reportChanged;
  private static CounterReport reportFailedReason3000;
  private static CounterReport reportFailedReason3031;
  private static CounterReport reportFailedReasonUnkownHost;

  @Rule public Timeout timeout = Timeout.seconds(10);

  @ClassRule public static EmbeddedPostgresRule pgRule = new EmbeddedPostgresRule(vertx, TENANT);

  @BeforeClass
  public static void beforeClass(TestContext context) {
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

    int port = NetworkUtils.nextFreePort();
    RestAssured.reset();
    RestAssured.baseURI = "http://localhost";
    RestAssured.port = port;
    RestAssured.defaultParser = Parser.JSON;

    DeploymentOptions options =
        new DeploymentOptions().setConfig(new JsonObject().put("http.port", port)).setWorker(true);
    vertx.deployVerticle(RestVerticle.class.getName(), options, context.asyncAssertSuccess());
  }

  @Before
  public void setUp(TestContext context) {
    PostgresClient.getInstance(vertx, TENANT)
        .delete(
            Constants.TABLE_NAME_COUNTER_REPORTS, new Criterion(), context.asyncAssertSuccess());
  }

  @AfterClass
  public static void afterClass() {
    RestAssured.reset();
  }

  @Test
  public void testGetCounterReportsDownloadById404() {
    given()
        .header(XOkapiHeaders.TENANT, TENANT)
        .pathParam("id", "0c6f1ca0-4ad8-479a-9d99-0dd686fea258")
        .get(BASE_URI_DOWNLOAD)
        .then()
        .statusCode(404);
  }

  @Test
  public void testGetCounterReportsDownloadById200Version4() {
    given()
        .body(report)
        .header(XOkapiHeaders.TENANT, TENANT)
        .header("content-type", APPLICATION_JSON)
        .post(BASE_URI)
        .then()
        .statusCode(201);

    given()
        .header(XOkapiHeaders.TENANT, TENANT)
        .pathParam("id", report.getId())
        .get(BASE_URI_DOWNLOAD)
        .then()
        .statusCode(200)
        .contentType(ContentType.XML)
        .body(equalTo(Counter4Utils.toXML(Json.encode(report.getReport()))));
  }

  @Test
  public void testGetCounterReportsDownloadById200Version5() throws IOException {
    String report =
        Resources.toString(Resources.getResource("TR/TR_1.json"), StandardCharsets.UTF_8);
    String id =
        given()
            .body(report)
            .header(XOkapiHeaders.TENANT, TENANT)
            .header("content-type", APPLICATION_JSON)
            .post(BASE_URI)
            .then()
            .statusCode(201)
            .extract()
            .as(CounterReport.class)
            .getId();

    Report resultReport =
        given()
            .header(XOkapiHeaders.TENANT, TENANT)
            .pathParam("id", id)
            .get(BASE_URI_DOWNLOAD)
            .then()
            .contentType(ContentType.JSON)
            .statusCode(200)
            .extract()
            .as(Report.class);

    assertThat(resultReport)
        .usingRecursiveComparison()
        .isEqualTo(Json.decodeValue(report, CounterReport.class).getReport());
  }

  @Test
  public void testGetCounterReportsDownloadByIdInvalidVersion() {
    given()
        .body(Json.decodeValue(Json.encode(report), CounterReport.class).withRelease("1"))
        .header(XOkapiHeaders.TENANT, TENANT)
        .header("content-type", APPLICATION_JSON)
        .post(BASE_URI)
        .then()
        .statusCode(201);

    String body =
        given()
            .header(XOkapiHeaders.TENANT, TENANT)
            .pathParam("id", report.getId())
            .get(BASE_URI_DOWNLOAD)
            .then()
            .contentType(ContentType.TEXT)
            .statusCode(500)
            .extract()
            .body()
            .asString();

    assertThat(body).contains("Unsupported", "version", "'1'");
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
        .body("id", equalTo(report.getId()))
        .body("reportEditedManually", equalTo(report.getReportEditedManually()))
        .body("editReason", equalTo(report.getEditReason()));

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
        .body("counterReports[0].release", equalTo(report.getRelease()))
        .body("counterReports[0].reportEditedManually", equalTo(report.getReportEditedManually()))
        .body("counterReports[0].editReason", equalTo(report.getEditReason()));

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
    assertThat(reportFailedReason3000.getFailedReason()).contains(udpErrorCodes.get(0));

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
  public void checkThatWeGetErrorCodes() {
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
    assertThat(errorCodes.getErrorCodes()).containsAll(Arrays.asList("3000", "3031", "other"));

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
