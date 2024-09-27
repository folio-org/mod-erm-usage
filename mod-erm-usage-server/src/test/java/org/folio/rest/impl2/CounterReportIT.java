package org.folio.rest.impl2;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.equalTo;

import com.google.common.io.Resources;
import com.google.common.net.HttpHeaders;
import com.google.common.net.MediaType;
import io.restassured.RestAssured;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.http.ContentType;
import io.restassured.parsing.Parser;
import io.restassured.specification.RequestSpecification;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Date;
import java.time.Instant;
import java.time.YearMonth;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.rest.RestVerticle;
import org.folio.rest.jaxrs.model.CounterReport;
import org.folio.rest.jaxrs.model.CounterReports;
import org.folio.rest.jaxrs.model.ErrorCodes;
import org.folio.rest.jaxrs.model.Report;
import org.folio.rest.jaxrs.model.ReportReleases;
import org.folio.rest.jaxrs.model.ReportTypes;
import org.folio.rest.jaxrs.model.UsageDataProvider;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.NetworkUtils;
import org.folio.rest.util.Constants;
import org.folio.rest.util.PostgresContainerRule;
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

  private static final String PATH_BASE = "/counter-reports";
  private static final String PATH_DOWNLOAD = "/{id}/download";
  private static final String PATH_REPORT_RELEASES = "/reports/releases";
  private static final String PATH_REPORT_TYPES = "/reports/types";
  private static final String PATH_ERROR_CODES = "/errors/codes";
  private static final String TENANT = "diku";
  private static final Vertx vertx = Vertx.vertx();
  private static final Map<String, String> defaultHeaders =
      Map.of(
          XOkapiHeaders.TENANT, TENANT, HttpHeaders.CONTENT_TYPE, MediaType.JSON_UTF_8.toString());
  @ClassRule public static PostgresContainerRule pgRule = new PostgresContainerRule(vertx, TENANT);

  private static CounterReport report;
  private static CounterReport reportChanged;
  private static RequestSpecification reportsDeleteReqSpec;
  private static RequestSpecification counterReportsReqSpec;
  private static RequestSpecification defaultHeaderSpec;
  @Rule public Timeout timeout = Timeout.seconds(10);

  @BeforeClass
  public static void beforeClass(TestContext context) {
    try {
      String reportStr =
          new String(Files.readAllBytes(Paths.get("../ramls/examples/counterreport.sample")));
      report = Json.decodeValue(reportStr, CounterReport.class);
      reportChanged = Json.decodeValue(reportStr, CounterReport.class).withRelease("5");
    } catch (Exception ex) {
      context.fail(ex);
    }

    int port = NetworkUtils.nextFreePort();
    RestAssured.reset();
    RestAssured.baseURI = "http://localhost";
    RestAssured.port = port;
    RestAssured.defaultParser = Parser.JSON;

    DeploymentOptions options =
        new DeploymentOptions().setConfig(new JsonObject().put("http.port", port));
    vertx.deployVerticle(RestVerticle.class.getName(), options, context.asyncAssertSuccess());

    defaultHeaderSpec = new RequestSpecBuilder().addHeaders(defaultHeaders).build();

    reportsDeleteReqSpec =
        new RequestSpecBuilder()
            .addHeaders(defaultHeaders)
            .setBasePath(PATH_BASE + "/reports/delete")
            .build();

    counterReportsReqSpec =
        new RequestSpecBuilder().addHeaders(defaultHeaders).setBasePath(PATH_BASE).build();
  }

  @AfterClass
  public static void afterClass() {
    RestAssured.reset();
  }

  @Before
  public void setUp(TestContext context) {
    PostgresClient.getInstance(vertx, TENANT)
        .delete(
            Constants.TABLE_NAME_COUNTER_REPORTS, new Criterion(), context.asyncAssertSuccess());
  }

  @Test
  public void testGetCounterReportsDownloadById404() {
    given(counterReportsReqSpec)
        .pathParam("id", "0c6f1ca0-4ad8-479a-9d99-0dd686fea258")
        .get(PATH_DOWNLOAD)
        .then()
        .statusCode(404);
  }

  @Test
  public void testGetCounterReportsDownloadById200Version4() {
    given(counterReportsReqSpec).body(report).post().then().statusCode(201);

    given(counterReportsReqSpec)
        .pathParam("id", report.getId())
        .get(PATH_DOWNLOAD)
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
        given(counterReportsReqSpec)
            .body(report)
            .post()
            .then()
            .statusCode(201)
            .extract()
            .as(CounterReport.class)
            .getId();

    Report resultReport =
        given(counterReportsReqSpec)
            .pathParam("id", id)
            .get(PATH_DOWNLOAD)
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
    given(counterReportsReqSpec)
        .body(Json.decodeValue(Json.encode(report), CounterReport.class).withRelease("1"))
        .post()
        .then()
        .statusCode(201);

    String body =
        given(counterReportsReqSpec)
            .pathParam("id", report.getId())
            .get(PATH_DOWNLOAD)
            .then()
            .contentType(ContentType.TEXT)
            .statusCode(500)
            .extract()
            .body()
            .asString();

    assertThat(body).contains("Unsupported", "version", "'1'");
  }

  @Test
  public void testDeleteMultipleReportsInvalidBody() {
    given(reportsDeleteReqSpec).post().then().statusCode(400);
    given(reportsDeleteReqSpec).body("{}").post().then().statusCode(400);
    given(reportsDeleteReqSpec).body("[1, 2]").post().then().statusCode(400);
    given(reportsDeleteReqSpec).body("[\"1\", 2]").post().then().statusCode(400);
  }

  @Test
  public void testDeleteMultipleReportsInvalidUUID() {
    given(reportsDeleteReqSpec).body("[\"abc123\"]").post().then().statusCode(400);
  }

  @Test
  public void testDeleteMultipleReports500() {
    given(reportsDeleteReqSpec)
        .header(XOkapiHeaders.TENANT, "")
        .body("[\"0ace645c-f99c-4aaa-a70b-c7b443ea0cef\"]")
        .post()
        .then()
        .statusCode(500);
  }

  @Test
  public void testDeleteMultipleReports() {
    // Create 5 reports
    List<CounterReport> reports =
        IntStream.rangeClosed(1, 5)
            .mapToObj(
                i ->
                    new CounterReport()
                        .withId(UUID.randomUUID().toString())
                        .withDownloadTime(Date.from(Instant.now()))
                        .withRelease("4")
                        .withYearMonth("2012-0" + i)
                        .withReportName("JR1"))
            .collect(Collectors.toList());
    // POST created reports
    reports.forEach(cr -> given(counterReportsReqSpec).body(cr).post().then().statusCode(201));

    // DELETE first 3 reports
    JsonArray idJsonArray =
        reports.subList(0, 3).stream()
            .map(CounterReport::getId)
            .collect(Collector.of(JsonArray::new, JsonArray::add, JsonArray::addAll));
    given(reportsDeleteReqSpec).body(idJsonArray.encode()).post().then().statusCode(204);

    // test that only 2 reports remain
    CounterReports result =
        given(counterReportsReqSpec).get().then().extract().body().as(CounterReports.class);
    assertThat(result.getCounterReports())
        .hasSize(2)
        .extracting("id")
        .containsExactlyInAnyOrder(reports.get(3).getId(), reports.get(4).getId());
  }

  @Test
  public void checkThatWeCanAddGetPutAndDeleteCounterReports() {
    // POST
    given(counterReportsReqSpec)
        .body(Json.encode(report))
        .post()
        .then()
        .statusCode(201)
        .body("release", equalTo(report.getRelease()))
        .body("id", equalTo(report.getId()));

    // GET
    given(counterReportsReqSpec)
        .get("/" + report.getId())
        .then()
        .contentType(ContentType.JSON)
        .statusCode(200)
        .body("id", equalTo(report.getId()))
        .body("reportEditedManually", equalTo(report.getReportEditedManually()))
        .body("editReason", equalTo(report.getEditReason()));

    // PUT
    given(counterReportsReqSpec)
        .body(Json.encode(reportChanged))
        .put("/" + report.getId())
        .then()
        .statusCode(204);

    // GET again
    given(counterReportsReqSpec)
        .get("/" + report.getId())
        .then()
        .statusCode(200)
        .body("id", equalTo(reportChanged.getId()))
        .body("release", equalTo(reportChanged.getRelease()));

    // DELETE
    given(counterReportsReqSpec).delete("/" + report.getId()).then().statusCode(204);

    // GET again
    given(counterReportsReqSpec).get("/" + report.getId()).then().statusCode(404);
  }

  @Test
  public void checkThatWeCanSearchByCQL() {
    given(counterReportsReqSpec)
        .body(Json.encode(report))
        .post()
        .then()
        .statusCode(201)
        .body("id", equalTo(report.getId()));

    given(counterReportsReqSpec)
        .queryParam("query", "(report=\"semantico*\")")
        .get()
        .then()
        .contentType(ContentType.JSON)
        .statusCode(200)
        .body("counterReports.size()", equalTo(1))
        .body("counterReports[0].id", equalTo(report.getId()))
        .body("counterReports[0].release", equalTo(report.getRelease()))
        .body("counterReports[0].reportEditedManually", equalTo(report.getReportEditedManually()))
        .body("counterReports[0].editReason", equalTo(report.getEditReason()));

    given(counterReportsReqSpec)
        .queryParam("query", "(report=\"someStringThatIsNotInTheReport*\")")
        .get()
        .then()
        .contentType(ContentType.JSON)
        .statusCode(200)
        .body("counterReports.size()", equalTo(0));

    given(counterReportsReqSpec)
        .queryParam("query", "(reportName==\"JR1\")")
        .get()
        .then()
        .contentType(ContentType.JSON)
        .statusCode(200)
        .body("counterReports.size()", equalTo(1))
        .body("counterReports[0].id", equalTo(report.getId()))
        .body("counterReports[0].release", equalTo(report.getRelease()));

    // DELETE
    given(counterReportsReqSpec).delete("/" + report.getId()).then().statusCode(204);
  }

  @Test
  public void checkThatInvalidCounterReportIsNotPosted() {
    CounterReport invalidReport =
        Json.decodeValue(Json.encode(report), CounterReport.class).withYearMonth(null);
    given(counterReportsReqSpec).body(Json.encode(invalidReport)).post().then().statusCode(422);
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
    given(defaultHeaderSpec)
        .body(Json.encode(udprovider))
        .post("/usage-data-providers")
        .then()
        .statusCode(201);

    // POST reports
    List<CounterReport> sampleReports =
        createSampleReportsWithFailedReasonForProviderId(udprovider.getId());
    sampleReports.forEach(
        report ->
            given(counterReportsReqSpec).body(Json.encode(report)).post().then().statusCode(201));

    // Check if hasFailedReport and errorCode of UDP is set
    UsageDataProvider udp =
        given(defaultHeaderSpec)
            .body(Json.encode(udprovider))
            .get("/usage-data-providers/" + udprovider.getId())
            .as(UsageDataProvider.class);
    assertThat(udp.getLabel()).isEqualTo(udprovider.getLabel());
    assertThat(udp.getId()).isNotEmpty();
    assertThat(udp.getHasFailedReport().value())
        .isEqualTo(UsageDataProvider.HasFailedReport.YES.value());

    List<String> udpErrorCodes = udp.getReportErrorCodes();
    assertThat(udpErrorCodes).containsExactlyInAnyOrder("3000", "3031", "3060", "3070", "other");

    // DELETE reports
    sampleReports.forEach(
        report -> given(counterReportsReqSpec).delete("/" + report.getId()).then().statusCode(204));

    // DELETE udp
    given(defaultHeaderSpec)
        .delete("/usage-data-providers/" + udprovider.getId())
        .then()
        .statusCode(204);
  }

  @Test
  public void checkThatWeGetErrorCodes() {
    // POST reports
    List<CounterReport> sampleReports =
        createSampleReportsWithFailedReasonForProviderId("bf6c9ddc-ff82-40c4-be64-dd2414bdcd72");
    sampleReports.forEach(
        report ->
            given(counterReportsReqSpec).body(Json.encode(report)).post().then().statusCode(201));

    // GET error codes
    ErrorCodes errorCodes =
        given(counterReportsReqSpec).get(PATH_ERROR_CODES).thenReturn().as(ErrorCodes.class);

    assertThat(errorCodes.getErrorCodes())
        .containsExactlyInAnyOrder("3000", "3031", "3060", "3070", "other");

    // DELETE reports
    sampleReports.forEach(
        r -> given(counterReportsReqSpec).delete("/" + r.getId()).then().statusCode(204));
  }

  @Test
  public void checkThatWeGetReportReleases() {
    // GET release versions
    ReportReleases initialResult =
        given(counterReportsReqSpec).get(PATH_REPORT_RELEASES).as(ReportReleases.class);
    assertThat(initialResult.getReportReleases()).isEmpty();

    // POST sample reports
    Stream.of("5.0", "5", "5.0")
        .map(
            release ->
                new CounterReport()
                    .withId(UUID.randomUUID().toString())
                    .withProviderId(UUID.randomUUID().toString())
                    .withReportName("TR")
                    .withReport(new Report())
                    .withDownloadTime(Date.from(Instant.now()))
                    .withYearMonth("2022-01")
                    .withRelease(release))
        .forEach(
            cr -> given(counterReportsReqSpec).body(Json.encode(cr)).post().then().statusCode(201));

    // GET release versions
    ReportReleases finalResult =
        given(counterReportsReqSpec).get(PATH_REPORT_RELEASES).as(ReportReleases.class);
    assertThat(finalResult.getReportReleases()).containsExactly("5", "5.0");
  }

  @Test
  public void checkThatWeGetReportTypes() throws IOException {
    // POST reports
    String trReport =
        Resources.toString(Resources.getResource("TR/TR_1.json"), StandardCharsets.UTF_8);
    String idTR =
        given(counterReportsReqSpec)
            .body(trReport)
            .post()
            .then()
            .statusCode(201)
            .extract()
            .as(CounterReport.class)
            .getId();

    String firstJr1Report =
        Resources.toString(Resources.getResource("JR1/jr1_1.json"), StandardCharsets.UTF_8);
    String idFirstJR1 =
        given(counterReportsReqSpec)
            .body(firstJr1Report)
            .post()
            .then()
            .statusCode(201)
            .extract()
            .as(CounterReport.class)
            .getId();

    String secondJr1Report =
        Resources.toString(Resources.getResource("JR1/jr1_2.json"), StandardCharsets.UTF_8);
    String idSecondJR1 =
        given(counterReportsReqSpec)
            .body(secondJr1Report)
            .post()
            .then()
            .statusCode(201)
            .extract()
            .as(CounterReport.class)
            .getId();

    // GET report types
    ReportTypes reportTypes =
        given(counterReportsReqSpec).get(PATH_REPORT_TYPES).thenReturn().as(ReportTypes.class);
    assertThat(reportTypes.getReportTypes()).containsExactlyInAnyOrder("TR", "JR1");

    // DELETE reports
    Stream.of(idTR, idFirstJR1, idSecondJR1)
        .forEach(id -> given(counterReportsReqSpec).delete("/" + id).then().statusCode(204));

    // GET report types again
    reportTypes =
        given(counterReportsReqSpec).get(PATH_REPORT_TYPES).thenReturn().as(ReportTypes.class);
    assertThat(reportTypes.getReportTypes()).isEmpty();
  }

  private List<CounterReport> createSampleReportsWithFailedReasonForProviderId(String providerId) {
    YearMonth startMonth = YearMonth.of(2019, 1);
    AtomicLong counter = new AtomicLong(0);
    return Stream.of(
            "Report not valid: Exception{Number=3000, Severity=ERROR, Message=Report Not Supported}",
            "Number=3031",
            "Some other error message 4040",
            "\"Code\": 3060",
            "\"Code\": 3060",
            "\"Code\":3070")
        .map(
            msg ->
                new CounterReport()
                    .withId(UUID.randomUUID().toString())
                    .withProviderId(providerId)
                    .withDownloadTime(Date.from(Instant.now()))
                    .withReportName("JR1")
                    .withRelease("4")
                    .withFailedReason(msg)
                    .withYearMonth(startMonth.plusMonths(counter.getAndIncrement()).toString()))
        .collect(Collectors.toList());
  }
}
