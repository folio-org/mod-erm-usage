package org.folio.rest.impl;

import static io.restassured.RestAssured.get;
import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.hamcrest.Matchers.containsString;

import com.google.common.io.Files;
import com.google.common.io.Resources;
import com.google.common.net.HttpHeaders;
import io.restassured.RestAssured;
import io.restassured.builder.RequestSpecBuilder;
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
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.xml.bind.JAXB;
import org.apache.commons.io.IOUtils;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.rest.RestVerticle;
import org.folio.rest.client.TenantClient;
import org.folio.rest.jaxrs.model.CounterReport;
import org.folio.rest.jaxrs.model.CounterReports;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.jaxrs.model.UsageDataProvider;
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
import org.niso.schemas.counter.Metric;
import org.niso.schemas.counter.MetricType;
import org.niso.schemas.counter.Report;
import org.niso.schemas.sushi.counter.CounterReportResponse;
import org.olf.erm.usage.counter41.Counter4Utils;
import org.olf.erm.usage.counter41.Counter4Utils.ReportSplitException;
import org.olf.erm.usage.counter50.Counter5Utils;
import org.olf.erm.usage.counter50.Counter5Utils.Counter5UtilsException;
import org.openapitools.client.model.COUNTERDatabaseReport;
import org.openapitools.client.model.COUNTERDatabaseUsage;
import org.openapitools.client.model.COUNTERItemIdentifiers;
import org.openapitools.client.model.COUNTERTitleReport;
import org.openapitools.client.model.COUNTERTitleUsage;

@RunWith(VertxUnitRunner.class)
public class CounterReportUploadIT {

  private static final String TENANT = "diku";
  private static final String PROVIDER_ID = "4b659cb9-e4bb-493d-ae30-5f5690c54802";
  private static final String PROVIDER_ID2 = "4b659cb9-e4bb-493d-ae30-5f5690c54803";
  private static final File FILE_REPORT_OK =
      new File(Resources.getResource("fileupload/reportJSTOR.xml").getFile());
  private static final File FILE_REPORT_UNSUPPORTED =
      new File(Resources.getResource("fileupload/reportUnsupported.xml").getFile());
  private static final File FILE_REPORT_NSS_OK =
      new File(Resources.getResource("fileupload/reportNSS.xml").getFile());
  private static final File FILE_NO_REPORT =
      new File(Resources.getResource("fileupload/noreport.txt").getFile());
  private static final File FILE_REPORT_MULTI_COP4 =
      new File(Resources.getResource("fileupload/reportJSTORMultiMonth.xml").getFile());
  private static final File FILE_REPORT_MULTI_COP5 =
      new File(Resources.getResource("fileupload/reportCOP5TRMultiMonth.json").getFile());
  private static final File FILE_REPORT5_OK =
      new File(Resources.getResource("fileupload/hwire_trj1.json").getFile());
  private static Vertx vertx;
  @Rule public Timeout timeout = Timeout.seconds(10);

  @BeforeClass
  public static void setUp(TestContext context) {
    vertx = Vertx.vertx();

    try {
      PostgresClient.setIsEmbedded(true);
      PostgresClient instance = PostgresClient.getInstance(vertx);
      instance.startEmbeddedPostgres();
    } catch (Exception e) {
      e.printStackTrace();
      context.fail(e);
      return;
    }

    int port = NetworkUtils.nextFreePort();

    RestAssured.reset();
    RestAssured.baseURI = "http://localhost";
    RestAssured.basePath = "";
    RestAssured.port = port;
    RestAssured.defaultParser = Parser.JSON;
    // RestAssured.filters(new ResponseLoggingFilter(), new RequestLoggingFilter());
    RestAssured.requestSpecification =
        new RequestSpecBuilder().addHeader(XOkapiHeaders.TENANT, TENANT).build();

    TenantClient tenantClient = new TenantClient("http://localhost:" + port, TENANT, TENANT);
    DeploymentOptions options =
        new DeploymentOptions().setConfig(new JsonObject().put("http.port", port)).setWorker(true);

    Async async = context.async();
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
    async.await();
    postProvider(context);
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

  private static void postProvider(TestContext ctx) {
    String str;
    try {
      str =
          Resources.toString(
              Resources.getResource("fileupload/provider.json"), StandardCharsets.UTF_8);
    } catch (Exception e) {
      ctx.fail(e);
      return;
    }

    Async async = ctx.async();
    PostgresClient.getInstance(vertx, TENANT)
        .save(
            Constants.TABLE_NAME_UDP,
            PROVIDER_ID,
            Json.decodeValue(str, UsageDataProvider.class).withId(null),
            ar -> {
              if (ar.succeeded()) {
                async.complete();
              } else {
                ctx.fail(ar.cause());
              }
            });
    async.await();
  }

  @Before
  public void before(TestContext ctx) {
    Async async = ctx.async();
    PostgresClient.getInstance(vertx, TENANT)
        .delete(
            Constants.TABLE_NAME_COUNTER_REPORTS,
            new Criterion(),
            ar -> {
              if (ar.failed()) {
                ctx.fail(ar.cause());
              }
              async.complete();
            });
    async.await();

    testThatDBIsEmpty();
  }

  private void testThatDBSizeIsSize(int i) {
    int size =
        get("/counter-reports")
            .then()
            .statusCode(200)
            .extract()
            .as(CounterReports.class)
            .getCounterReports()
            .size();
    assertThat(size).isEqualTo(i);
  }

  private void testThatDBIsEmpty() {
    testThatDBSizeIsSize(0);
  }

  @Test
  public void testReportR4Ok() {
    String savedReportId =
        given()
            .header(HttpHeaders.CONTENT_TYPE, ContentType.BINARY)
            .body(FILE_REPORT_OK)
            .post("/counter-reports/upload/provider/" + PROVIDER_ID)
            .then()
            .statusCode(200)
            .body(containsString("Saved report"))
            .extract()
            .asString()
            .replace("Saved report with ids: ", "");

    CounterReport savedReport =
        given().get("/counter-reports/" + savedReportId).then().extract().as(CounterReport.class);
    assertThat(savedReport.getProviderId()).isEqualTo(PROVIDER_ID);
    assertThat(savedReport.getRelease()).isEqualTo("4");
    assertThat(savedReport.getYearMonth()).isEqualTo("2018-03");
    assertThat(savedReport.getDownloadTime()).isNotNull();
    assertThat(savedReport.getReportName()).isEqualTo("JR1");

    Report reportFromXML = JAXB.unmarshal(FILE_REPORT_OK, Report.class);
    Report reportFromDB = Counter4Utils.fromJSON(Json.encode(savedReport.getReport()));
    assertThat(reportFromXML).usingRecursiveComparison().isEqualTo(reportFromDB);
  }

  @Test
  public void testReportR4UnsupportedReport() {
    given()
        .header(HttpHeaders.CONTENT_TYPE, ContentType.BINARY)
        .body(FILE_REPORT_UNSUPPORTED)
        .post("/counter-reports/upload/provider/" + PROVIDER_ID)
        .then()
        .statusCode(500)
        .body(containsString("Unsupported report"));
  }

  @Test
  public void testReportR4OkOverwriteTrue() {
    String savedReportId =
        given()
            .header(HttpHeaders.CONTENT_TYPE, ContentType.BINARY)
            .body(FILE_REPORT_OK)
            .queryParam("overwrite", true)
            .post("/counter-reports/upload/provider/" + PROVIDER_ID)
            .then()
            .statusCode(200)
            .extract()
            .asString()
            .replace("Saved report with id ", "");

    String overwriteReportId =
        given()
            .header(HttpHeaders.CONTENT_TYPE, ContentType.BINARY)
            .body(FILE_REPORT_OK)
            .queryParam("overwrite", true)
            .post("/counter-reports/upload/provider/" + PROVIDER_ID)
            .then()
            .statusCode(200)
            .extract()
            .asString()
            .replace("Saved report with id ", "");
    assertThat(savedReportId).isEqualTo(overwriteReportId);
    testThatDBSizeIsSize(1);
  }

  @Test
  public void testReportR4OkOverwriteFalse() {
    given()
        .header(HttpHeaders.CONTENT_TYPE, ContentType.BINARY)
        .body(FILE_REPORT_OK)
        .queryParam("overwrite", false)
        .post("/counter-reports/upload/provider/" + PROVIDER_ID)
        .then()
        .statusCode(200);

    given()
        .header(HttpHeaders.CONTENT_TYPE, ContentType.BINARY)
        .body(FILE_REPORT_OK)
        .queryParam("overwrite", false)
        .post("/counter-reports/upload/provider/" + PROVIDER_ID)
        .then()
        .statusCode(500)
        .body(containsString("Report already existing"))
        .body(containsString("2018-03"));
  }

  @Test
  public void testReportR4NssOk() {
    String savedReportId =
        given()
            .header(HttpHeaders.CONTENT_TYPE, ContentType.BINARY)
            .body(FILE_REPORT_NSS_OK)
            .post("/counter-reports/upload/provider/" + PROVIDER_ID)
            .then()
            .statusCode(200)
            .body(containsString("Saved report"))
            .extract()
            .asString()
            .replace("Saved report with ids: ", "");

    CounterReport savedReport =
        given().get("/counter-reports/" + savedReportId).then().extract().as(CounterReport.class);
    assertThat(savedReport.getProviderId()).isEqualTo(PROVIDER_ID);
    assertThat(savedReport.getRelease()).isEqualTo("4");
    assertThat(savedReport.getYearMonth()).isEqualTo("2018-01");
    assertThat(savedReport.getDownloadTime()).isNotNull();
    assertThat(savedReport.getReportName()).isEqualTo("JR1");

    Report reportFromXML =
        JAXB.unmarshal(FILE_REPORT_NSS_OK, CounterReportResponse.class)
            .getReport()
            .getReport()
            .get(0);
    Report reportFromDB = Counter4Utils.fromJSON(Json.encode(savedReport.getReport()));
    assertThat(reportFromXML).usingRecursiveComparison().isEqualTo(reportFromDB);
  }

  @Test
  public void testReportR5Ok() throws Exception {
    String savedReportId =
        given()
            .header(HttpHeaders.CONTENT_TYPE, ContentType.BINARY)
            .body(FILE_REPORT5_OK)
            .post("/counter-reports/upload/provider/" + PROVIDER_ID)
            .then()
            .statusCode(200)
            .body(containsString("Saved report"))
            .extract()
            .asString()
            .replace("Saved report with ids: ", "");

    CounterReport savedReport =
        given().get("/counter-reports/" + savedReportId).then().extract().as(CounterReport.class);
    assertThat(savedReport.getProviderId()).isEqualTo(PROVIDER_ID);
    assertThat(savedReport.getRelease()).isEqualTo("5");
    assertThat(savedReport.getYearMonth()).isEqualTo("2019-01");
    assertThat(savedReport.getDownloadTime()).isNotNull();
    assertThat(savedReport.getReportName()).isEqualTo("TR_J1");

    org.folio.rest.jaxrs.model.Report report =
        Json.decodeValue(
            Files.toString(FILE_REPORT5_OK, StandardCharsets.UTF_8),
            org.folio.rest.jaxrs.model.Report.class);
    assertThat(savedReport.getReport()).usingRecursiveComparison().isEqualTo(report);
  }

  @Test
  public void testReportInvalidContent() {
    given()
        .header(HttpHeaders.CONTENT_TYPE, ContentType.BINARY)
        .body(FILE_NO_REPORT)
        .post("/counter-reports/upload/provider/" + PROVIDER_ID)
        .then()
        .statusCode(500)
        .body(containsString("Wrong format"));
  }

  @Test
  public void testReportMultipleMonthsC4() {
    String createdIds =
        given()
            .header(HttpHeaders.CONTENT_TYPE, ContentType.BINARY)
            .body(FILE_REPORT_MULTI_COP4)
            .post("/counter-reports/upload/provider/" + PROVIDER_ID)
            .then()
            .statusCode(200)
            .body(containsString("Saved report with ids"))
            .extract()
            .asString()
            .replace("Saved report with ids: ", "");

    String query =
        String.format("/counter-reports?query=(reportName=JR1 AND providerId=%s)", PROVIDER_ID);
    CounterReports reports = given().get(query).then().extract().as(CounterReports.class);
    assertThat(reports.getCounterReports().stream().map(CounterReport::getYearMonth))
        .containsExactlyInAnyOrder("2018-03", "2018-04");
    assertThat(reports.getCounterReports().stream().map(CounterReport::getId))
        .containsExactlyInAnyOrder(createdIds.split(","));

    given()
        .header(HttpHeaders.CONTENT_TYPE, ContentType.BINARY)
        .body(FILE_REPORT_MULTI_COP4)
        .post("/counter-reports/upload/provider/" + PROVIDER_ID)
        .then()
        .statusCode(500)
        .body(containsString("Report already existing"))
        .body(containsString("2018-03"))
        .body(containsString("2018-04"));
  }

  private void removeAttributes(Report report) {
    report.getCustomer().get(0).getReportItems().stream()
        .flatMap(ri -> ri.getItemPerformance().stream())
        .map(Metric::getInstance)
        .forEach(
            list ->
                list.removeIf(
                    pc ->
                        pc.getMetricType().equals(MetricType.FT_HTML)
                            || pc.getMetricType().equals(MetricType.FT_PDF)));
  }

  @Test
  public void testReportMultipleMonthsC4FromCsv() throws IOException, ReportSplitException {
    String csvString = Counter4Utils.toCSV(JAXB.unmarshal(FILE_REPORT_MULTI_COP4, Report.class));
    assertThat(csvString).isNotNull();

    String createdIds =
        given()
            .header(HttpHeaders.CONTENT_TYPE, ContentType.BINARY)
            .body(IOUtils.toInputStream(csvString, StandardCharsets.UTF_8))
            .post("/counter-reports/upload/provider/" + PROVIDER_ID)
            .then()
            .statusCode(200)
            .body(containsString("Saved report with ids"))
            .extract()
            .asString()
            .replace("Saved report with ids: ", "");

    String query =
        String.format(
            "/counter-reports?query=(reportName=JR1 AND providerId=%s) sortby yearMonth",
            PROVIDER_ID);
    CounterReports reports = given().get(query).then().extract().as(CounterReports.class);
    assertThat(reports.getCounterReports().stream().map(CounterReport::getYearMonth))
        .containsExactly("2018-03", "2018-04");
    assertThat(reports.getCounterReports().stream().map(CounterReport::getId))
        .containsExactly(createdIds.split(","));

    // check content here
    Report report =
        Counter4Utils.fromString(
            Files.asCharSource(FILE_REPORT_MULTI_COP4, StandardCharsets.UTF_8).read());
    assertThat(report).isNotNull();
    List<Report> expectedReports = new ArrayList<>(Counter4Utils.split(report));
    expectedReports.forEach(this::removeAttributes);

    List<Report> actualReports =
        reports.getCounterReports().stream()
            .map(CounterReport::getReport)
            .map(Json::encode)
            .map(Counter4Utils::fromJSON)
            .collect(Collectors.toList());

    assertThat(actualReports.get(0))
        .usingRecursiveComparison()
        .ignoringFields("created", "id", "name", "title", "vendor")
        .ignoringCollectionOrder()
        .isEqualTo(expectedReports.get(0));

    assertThat(actualReports.get(1))
        .usingRecursiveComparison()
        .ignoringFields("created", "id", "name", "title", "vendor")
        .ignoringCollectionOrder()
        .isEqualTo(expectedReports.get(1));

    given()
        .header(HttpHeaders.CONTENT_TYPE, ContentType.BINARY)
        .body(FILE_REPORT_MULTI_COP4)
        .post("/counter-reports/upload/provider/" + PROVIDER_ID)
        .then()
        .statusCode(500)
        .body(containsString("Report already existing"))
        .body(containsString("2018-03"))
        .body(containsString("2018-04"));
  }

  @Test
  public void testReportMultipleMonthsC5() {
    String createdIds =
        given()
            .header(HttpHeaders.CONTENT_TYPE, ContentType.BINARY)
            .body(FILE_REPORT_MULTI_COP5)
            .post("/counter-reports/upload/provider/" + PROVIDER_ID)
            .then()
            .statusCode(200)
            .body(containsString("Saved report with ids"))
            .extract()
            .asString()
            .replace("Saved report with ids: ", "");

    String query =
        String.format("/counter-reports?query=(reportName=TR AND providerId=%s)", PROVIDER_ID);
    CounterReports reports = given().get(query).then().extract().as(CounterReports.class);
    assertThat(reports.getCounterReports().stream().map(CounterReport::getYearMonth))
        .containsExactlyInAnyOrder("2019-09", "2019-10", "2019-11");
    assertThat(reports.getCounterReports().stream().map(CounterReport::getId))
        .containsExactlyInAnyOrder(createdIds.split(","));

    given()
        .header(HttpHeaders.CONTENT_TYPE, ContentType.BINARY)
        .body(FILE_REPORT_MULTI_COP5)
        .post("/counter-reports/upload/provider/" + PROVIDER_ID)
        .then()
        .statusCode(500)
        .body(containsString("Report already existing"))
        .body(containsString("2019-09"))
        .body(containsString("2019-10"))
        .body(containsString("2019-11"));
  }

  @Test
  public void testReportMultipleMonthsC5FromCsv() throws IOException, Counter5UtilsException {
    String jsonString =
        Resources.toString(FILE_REPORT_MULTI_COP5.toURI().toURL(), StandardCharsets.UTF_8);
    assertThat(jsonString).isNotNull();

    Object report = Counter5Utils.fromJSON(jsonString);
    String csvString = Counter5Utils.toCSV(report);
    assertThat(csvString).isNotNull();

    String createdIds =
        given()
            .header(HttpHeaders.CONTENT_TYPE, ContentType.BINARY)
            .body(IOUtils.toInputStream(csvString, StandardCharsets.UTF_8))
            .post("/counter-reports/upload/provider/" + PROVIDER_ID)
            .then()
            .statusCode(200)
            .body(containsString("Saved report with ids"))
            .extract()
            .asString()
            .replace("Saved report with ids: ", "");

    String query =
        String.format(
            "/counter-reports?query=(reportName=TR AND providerId=%s) sortby yearMonth",
            PROVIDER_ID);
    CounterReports reports = given().get(query).then().extract().as(CounterReports.class);
    assertThat(reports.getCounterReports().stream().map(CounterReport::getYearMonth))
        .containsExactly("2019-09", "2019-10", "2019-11");
    assertThat(reports.getCounterReports().stream().map(CounterReport::getId))
        .containsExactly(createdIds.split(","));

    // check content here
    assertThat(report).isNotNull();
    List<Object> expectedReports = new ArrayList<>(Counter5Utils.split(report));
    // expectedReports.forEach(this::removeAttributes);

    List<Object> actualReports =
        reports.getCounterReports().stream()
            .map(CounterReport::getReport)
            .map(Json::encode)
            .map(this::cop5ReportFromJson)
            .collect(Collectors.toList());

    compareCOP5Reports(actualReports, expectedReports, 0);
    compareCOP5Reports(actualReports, expectedReports, 1);
    compareCOP5Reports(actualReports, expectedReports, 2);

    given()
        .header(HttpHeaders.CONTENT_TYPE, ContentType.BINARY)
        .body(FILE_REPORT_MULTI_COP5)
        .post("/counter-reports/upload/provider/" + PROVIDER_ID)
        .then()
        .statusCode(500)
        .body(containsString("Report already existing"))
        .body(containsString("2019-09"))
        .body(containsString("2019-10"))
        .body(containsString("2019-11"));
  }

  private void compareCOP5Reports(
      List<Object> actualReports, List<Object> expectedReports, int index) {
    Object first = expectedReports.get(index);
    if (first instanceof COUNTERDatabaseReport) {
      COUNTERDatabaseReport actual = (COUNTERDatabaseReport) actualReports.get(index);
      COUNTERDatabaseReport expected = (COUNTERDatabaseReport) expectedReports.get(index);
      assertThat(actual.getReportHeader())
          .usingRecursiveComparison()
          .ignoringCollectionOrder()
          .isEqualTo(expected.getReportHeader());
      assertThat(actual.getReportItems().size()).isEqualTo(expected.getReportItems().size());

      // Compare each platform usage
      for (int i = 0; i < actual.getReportItems().size(); i++) {
        COUNTERDatabaseUsage actualUsage = actual.getReportItems().get(i);
        COUNTERDatabaseUsage expectedUsage =
            expected.getReportItems().stream()
                .filter(item -> item.getDatabase().equals(actualUsage.getDatabase()))
                .findFirst()
                .orElse(null);
        assertThat(actualUsage)
            .usingRecursiveComparison()
            .ignoringCollectionOrder()
            .isEqualTo(expectedUsage);
      }
    } else if (first instanceof COUNTERTitleReport) {
      COUNTERTitleReport actual = (COUNTERTitleReport) actualReports.get(index);
      COUNTERTitleReport expected = (COUNTERTitleReport) expectedReports.get(index);
      assertThat(actual.getReportHeader())
          .usingRecursiveComparison()
          .ignoringFields("customerID")
          .ignoringCollectionOrder()
          .isEqualTo(expected.getReportHeader());
      assertThat(actual.getReportItems().size()).isEqualTo(expected.getReportItems().size());

      // Compare each platform usage
      for (int i = 0; i < actual.getReportItems().size(); i++) {

        COUNTERTitleUsage actualUsage = actual.getReportItems().get(i);
        List<COUNTERItemIdentifiers> itemIDsWoNull =
            actual.getReportItems().get(i).getItemID().stream()
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        actualUsage.setItemID(itemIDsWoNull);

        COUNTERTitleUsage expectedUsage =
            expected.getReportItems().stream()
                .filter(item -> item.getTitle().equals(actualUsage.getTitle()))
                .findFirst()
                .orElse(null);

        assertThat(actualUsage)
            .usingRecursiveComparison()
            .ignoringCollectionOrder()
            .isEqualTo(expectedUsage);
      }
    } else {
      // casting to other types not implemented
      fail(
          String.format(
              "Comparing reports of type %s not implemented", first.getClass().toString()));
    }
  }

  private Object cop5ReportFromJson(String json) {
    try {
      return Counter5Utils.fromJSON(json);
    } catch (Counter5UtilsException e) {
      throw new CounterReportUploadException(e);
    }
  }

  @Test
  public void testProviderNotFound() {
    given()
        .header(HttpHeaders.CONTENT_TYPE, ContentType.BINARY)
        .body(FILE_REPORT_OK)
        .post("/counter-reports/upload/provider/" + PROVIDER_ID2)
        .then()
        .statusCode(500)
        .body(containsString(PROVIDER_ID2), containsString("not found"));
  }

  public static class CounterReportUploadException extends RuntimeException {

    public CounterReportUploadException(Throwable cause) {
      super(cause);
    }
  }
}
