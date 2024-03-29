package org.folio.rest.impl2;

import static io.restassured.RestAssured.get;
import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.folio.rest.util.ReportExportHelper.CREATED_BY_SUFFIX;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.google.common.net.HttpHeaders;
import com.google.common.net.MediaType;
import io.restassured.RestAssured;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.parsing.Parser;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.rest.RestVerticle;
import org.folio.rest.jaxrs.model.CounterReport;
import org.folio.rest.jaxrs.model.CounterReports;
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
import org.olf.erm.usage.counter.common.ExcelUtil;
import org.olf.erm.usage.counter41.Counter4Utils;
import org.olf.erm.usage.counter50.Counter5Utils;
import org.olf.erm.usage.counter50.Counter5Utils.Counter5UtilsException;
import org.openapitools.client.model.COUNTERTitleReport;

@RunWith(VertxUnitRunner.class)
public class CounterReportExportIT {

  private static final String TENANT = "diku";
  private static Vertx vertx = Vertx.vertx();

  private static CounterReport counterReport;
  private static String expected;

  @ClassRule
  public static PostgresContainerRule pgContainerRule = new PostgresContainerRule(vertx, TENANT);

  @Rule public Timeout timeout = Timeout.seconds(10);

  @BeforeClass
  public static void beforeClass(TestContext context) {
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

    int port = NetworkUtils.nextFreePort();
    RestAssured.reset();
    RestAssured.baseURI = "http://localhost";
    RestAssured.basePath = "/counter-reports";
    RestAssured.port = port;
    RestAssured.defaultParser = Parser.JSON;
    // RestAssured.filters(new ResponseLoggingFilter(), new RequestLoggingFilter());
    RestAssured.requestSpecification =
        new RequestSpecBuilder()
            .addHeader(XOkapiHeaders.TENANT, TENANT)
            .addHeader(HttpHeaders.CONTENT_TYPE, MediaType.JSON_UTF_8.toString())
            .build();

    DeploymentOptions options =
        new DeploymentOptions().setConfig(new JsonObject().put("http.port", port));
    vertx.deployVerticle(RestVerticle.class.getName(), options, context.asyncAssertSuccess());
  }

  @AfterClass
  public static void afterClass() {
    RestAssured.reset();
  }

  @Before
  public void before(TestContext context) {
    Async async = context.async();
    clearCounterReports().onComplete(rs -> async.complete());
    async.await(5000);
    testThatDBIsEmpty();
  }

  private Future<RowSet<Row>> clearCounterReports() {
    Promise<RowSet<Row>> promise = Promise.promise();
    PostgresClient.getInstance(vertx, TENANT)
        .delete(Constants.TABLE_NAME_COUNTER_REPORTS, new Criterion(), promise);
    return promise.future();
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
    assertThat(size).isZero();
  }

  @Test
  public void testExportCSVFailsJR1GOA() throws IOException {
    String xml =
        Resources.toString(
            Resources.getResource("fileupload/reportJR1GOA.xml"), StandardCharsets.UTF_8);
    String json = Counter4Utils.toJSON(Counter4Utils.fromString(xml));
    CounterReport counterReport =
        new CounterReport()
            .withId(UUID.randomUUID().toString())
            .withRelease("4")
            .withReportName("JR1 GOA")
            .withDownloadTime(Date.from(Instant.now()))
            .withProviderId("4b659cb9-e4bb-493d-ae30-5f5690c54802")
            .withYearMonth("2020-07")
            .withReport(Json.decodeValue(json, org.folio.rest.jaxrs.model.Report.class));

    given().body(counterReport).post().then().statusCode(201);

    given()
        .pathParam("id", counterReport.getId())
        .get("/export/{id}")
        .then()
        .statusCode(500)
        .body(containsString("no csv mapper"));
  }

  @Test
  public void testExportCSVOkJR1() {
    given().body(counterReport).post().then().statusCode(201);

    String csvResult =
        given()
            .pathParam("id", counterReport.getId())
            .get("/export/{id}")
            .then()
            .statusCode(200)
            .contentType(equalTo("text/csv"))
            .extract()
            .asString();
    assertThat(csvResult).isEqualToNormalizingNewlines(expected);
  }

  @Test
  public void testExportCSVOkTR() throws IOException, Counter5UtilsException {
    String reportStr =
        Resources.toString(Resources.getResource("TR/TR_1.json"), StandardCharsets.UTF_8);
    CounterReport counterReport = Json.decodeValue(reportStr, CounterReport.class);
    counterReport.setId(UUID.randomUUID().toString());
    given().body(counterReport).post().then().statusCode(201);

    String csvResult =
        given()
            .pathParam("id", counterReport.getId())
            .get("/export/{id}")
            .then()
            .statusCode(200)
            .contentType(equalTo("text/csv"))
            .extract()
            .asString();

    COUNTERTitleReport expectedReport =
        (COUNTERTitleReport) Counter5Utils.fromJSON(Json.encode(counterReport.getReport()));
    expectedReport
        .getReportHeader()
        .setCreatedBy(expectedReport.getReportHeader().getCreatedBy() + " " + CREATED_BY_SUFFIX);
    String expectedCsv = Counter5Utils.toCSV(expectedReport);

    List<String> actualLines = csvResult.lines().collect(Collectors.toList());
    List<String> expectedLines = expectedCsv.lines().collect(Collectors.toList());
    // ignore Created
    actualLines.remove(10);
    expectedLines.remove(10);

    assertThat(actualLines).containsExactlyInAnyOrderElementsOf(expectedLines);
  }

  @Test
  public void testExportCSVOkJR1MultipleMonths() {
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
        .get("/export/provider/{id}/report/{name}/version/{version}/from/{begin}/to/{end}")
        .then()
        .statusCode(200)
        .body(
            containsString(
                "19th-Century Music,University of California Press,Ithaka,,,0148-2076,1533-8606,6,3,3,2,2,2"));
  }

  @Test
  public void testExportCSVOkTRMultipleMonths() {
    String json1 = resourceToString("TR/TR_1.json");
    String json2 = resourceToString("TR/TR_2.json");
    String json3 = resourceToString("TR/TR_3.json");

    given().body(json1).post().then().statusCode(201);
    given().body(json2).post().then().statusCode(201);
    given().body(json3).post().then().statusCode(201);

    given()
        .pathParam("id", "4b659cb9-e4bb-493d-ae30-5f5690c54802")
        .pathParam("name", "TR")
        .pathParam("version", "5")
        .pathParam("begin", "2019-09")
        .pathParam("end", "2019-11")
        .get("/export/provider/{id}/report/{name}/version/{version}/from/{begin}/to/{end}")
        .then()
        .statusCode(200)
        .body(
            containsString(
                "Title 1,My Press,my:mypress,My Journals,8910.DOI,my:foo,,0011-1122,0123-4567,,,,,,,Total_Item_Investigations,9,3,0,6"));
  }

  @Test
  public void testExportCSVNoMapper() {
    CounterReport badReleaseNo =
        Json.decodeValue(Json.encode(counterReport), CounterReport.class).withRelease("1");
    given().body(badReleaseNo).post().then().statusCode(201);

    given()
        .pathParam("id", badReleaseNo.getId())
        .get("/export/{id}")
        .then()
        .statusCode(500)
        .body(containsString("no csv mapper"));
  }

  @Test
  public void testExportCSVNoReport() {
    CounterReport noReport =
        Json.decodeValue(Json.encode(counterReport), CounterReport.class).withReport(null);
    given().body(noReport).post().then().statusCode(201);

    given()
        .pathParam("id", noReport.getId())
        .get("/export/{id}")
        .then()
        .statusCode(500)
        .body(containsString("No report"));
  }

  @Test
  public void testExportUnsupportedFormatById() {
    given()
        .pathParam("id", UUID.randomUUID().toString())
        .queryParam("format", "jpg")
        .get("/export/{id}")
        .then()
        .statusCode(400)
        .body(containsString("Requested format \"jpg\" is not supported."));
  }

  @Test
  public void testExportUnsupportedCounterVersionMultipleMonths() {
    given()
        .pathParam("id", UUID.randomUUID().toString())
        .pathParam("name", "JR1")
        .pathParam("version", 1)
        .pathParam("begin", "2018-01")
        .pathParam("end", "2018-12")
        .queryParam("format", "xlsx")
        .get("/export/provider/{id}/report/{name}/version/{version}/from/{begin}/to/{end}")
        .then()
        .statusCode(400)
        .body(equalTo("Requested counter version \"1\" is not supported."));
  }

  @Test
  public void testExportUnsupportedFormatMultipleMonths() {
    given()
        .pathParam("id", UUID.randomUUID().toString())
        .pathParam("name", "JR1")
        .pathParam("version", 4)
        .pathParam("begin", "2018-01")
        .pathParam("end", "2018-12")
        .queryParam("format", "jpg")
        .get("/export/provider/{id}/report/{name}/version/{version}/from/{begin}/to/{end}")
        .then()
        .statusCode(400)
        .body(containsString("Requested format \"jpg\" is not supported."));
  }

  @Test
  public void testExportXLSXOkJR1() throws IOException {
    given().body(counterReport).post().then().statusCode(201);

    InputStream xlsx =
        given()
            .pathParam("id", counterReport.getId())
            .queryParam("format", "xlsx")
            .get("/export/{id}")
            .then()
            .statusCode(200)
            .contentType(equalTo(MediaType.OOXML_SHEET.toString()))
            .extract()
            .asInputStream();
    assertThat(ExcelUtil.toCSV(xlsx)).isEqualToNormalizingNewlines(expected);
  }

  @Test
  public void testExportXLSXOkTRMultipleMonths() throws IOException {
    String json1 = resourceToString("TR/TR_1.json");
    String json2 = resourceToString("TR/TR_2.json");
    String json3 = resourceToString("TR/TR_3.json");

    given().body(json1).post().then().statusCode(201);
    given().body(json2).post().then().statusCode(201);
    given().body(json3).post().then().statusCode(201);

    String csv =
        given()
            .pathParam("id", "4b659cb9-e4bb-493d-ae30-5f5690c54802")
            .pathParam("name", "TR")
            .pathParam("version", "5")
            .pathParam("begin", "2019-09")
            .pathParam("end", "2019-11")
            .get("/export/provider/{id}/report/{name}/version/{version}/from/{begin}/to/{end}")
            .then()
            .statusCode(200)
            .contentType(equalTo("text/csv"))
            .extract()
            .asString();

    InputStream xlsx =
        given()
            .pathParam("id", "4b659cb9-e4bb-493d-ae30-5f5690c54802")
            .pathParam("name", "TR")
            .pathParam("version", "5")
            .pathParam("begin", "2019-09")
            .pathParam("end", "2019-11")
            .queryParam("format", "xlsx")
            .get("/export/provider/{id}/report/{name}/version/{version}/from/{begin}/to/{end}")
            .then()
            .statusCode(200)
            .contentType(equalTo(MediaType.OOXML_SHEET.toString()))
            .extract()
            .asInputStream();
    assertThat(ExcelUtil.toCSV(xlsx)).isEqualToNormalizingNewlines(csv);
  }
}
