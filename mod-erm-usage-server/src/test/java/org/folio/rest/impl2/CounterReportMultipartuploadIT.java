package org.folio.rest.impl2;

import static io.restassured.RestAssured.get;
import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.folio.rest.impl.CounterReportAPI.FORM_ATTR_EDITED;
import static org.folio.rest.impl.CounterReportAPI.FORM_ATTR_REASON;
import static org.folio.rest.util.UploadHelper.MSG_UNSUPPORTED_REPORT;
import static org.folio.rest.util.UploadHelper.MSG_WRONG_FORMAT;
import static org.hamcrest.Matchers.containsString;

import com.google.common.io.Files;
import com.google.common.io.Resources;
import com.google.common.net.HttpHeaders;
import io.restassured.RestAssured;
import io.restassured.builder.MultiPartSpecBuilder;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.parsing.Parser;
import io.restassured.specification.RequestSpecification;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import jakarta.xml.bind.JAXB;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.postgres.testing.PostgresTesterContainer;
import org.folio.rest.RestVerticle;
import org.folio.rest.impl.TenantAPI;
import org.folio.rest.jaxrs.model.CounterReport;
import org.folio.rest.jaxrs.model.CounterReports;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.jaxrs.model.UsageDataProvider;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.ModuleName;
import org.folio.rest.tools.utils.NetworkUtils;
import org.folio.rest.util.Constants;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
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
import org.openapitools.client.model.COUNTERTitleReport;
import org.openapitools.client.model.COUNTERTitleUsage;

@RunWith(VertxUnitRunner.class)
public class CounterReportMultipartuploadIT {

  private static final String TENANT = "diku";
  private static final String PROVIDER_ID = "4b659cb9-e4bb-493d-ae30-5f5690c54802";
  private static final String PROVIDER_ID2 = "4b659cb9-e4bb-493d-ae30-5f5690c54803";
  private static final File FILE_REPORT_OK =
      new File(Resources.getResource("fileupload/reportJSTOR.xml").getFile());

  private static final File FILE_REPORT_JR1GOA =
      new File(Resources.getResource("fileupload/reportJR1GOA.xml").getFile());
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
      new File(Resources.getResource("fileupload/brill_tr.json").getFile());
  private static final File FILE_REPORT5_NOT_OK =
      new File(Resources.getResource("fileupload/hwire_trj1.json").getFile());
  private static final String PATH = "/counter-reports/multipartupload/provider/";
  private static final String EDIT_REASON = "Edit Reason";

  private static final Map<String, String> FORM_PARAMS =
      Map.of(FORM_ATTR_EDITED, "true", FORM_ATTR_REASON, EDIT_REASON);
  private static Vertx vertx;

  @BeforeClass
  public static void setUp(TestContext context) {
    vertx = Vertx.vertx();

    PostgresClient.setPostgresTester(new PostgresTesterContainer());
    PostgresClient.getInstance(vertx);

    int port = NetworkUtils.nextFreePort();

    RestAssured.reset();
    RestAssured.baseURI = "http://localhost";
    RestAssured.basePath = "";
    RestAssured.port = port;
    RestAssured.defaultParser = Parser.JSON;
    // RestAssured.filters(new ResponseLoggingFilter(), new RequestLoggingFilter());
    RestAssured.requestSpecification =
        new RequestSpecBuilder().addHeader(XOkapiHeaders.TENANT, TENANT).build();

    DeploymentOptions options =
        new DeploymentOptions().setConfig(new JsonObject().put("http.port", port));

    Async async = context.async();
    vertx.deployVerticle(
        RestVerticle.class.getName(),
        options,
        res -> {
          try {
            new TenantAPI()
                .postTenantSync(
                    new TenantAttributes().withModuleTo(ModuleName.getModuleVersion()),
                    Map.of(XOkapiHeaders.TENANT, TENANT),
                    res2 -> {
                      context.verify(v -> assertThat(res2.result().getStatus()).isEqualTo(204));
                      async.complete();
                    },
                    vertx.getOrCreateContext());
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
              PostgresClient.stopPostgresTester();
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

  public static String createTitleReportWithSize(int sizeInMB) {
    COUNTERTitleReport counterTitleReport;
    try {
      counterTitleReport =
          Json.decodeValue(
              Buffer.buffer(Files.toByteArray(FILE_REPORT5_OK)), COUNTERTitleReport.class);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    int length = Json.encode(counterTitleReport).getBytes().length;
    int itemSize = Json.encode(counterTitleReport.getReportItems().get(0)).getBytes().length;

    while (length < sizeInMB * 1024 * 1024) {
      counterTitleReport.getReportItems().add(counterTitleReport.getReportItems().get(0));
      length += itemSize + 1;
    }
    return Json.encode(counterTitleReport);
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
              } else {
                async.complete();
              }
            });
    async.await();

    testThatDBIsEmpty();
  }

  @Test
  public void testLargeFile() {
    given()
        .multiPart(new MultiPartSpecBuilder(createTitleReportWithSize(50).getBytes()).build())
        .post(PATH + PROVIDER_ID)
        .then()
        .statusCode(200)
        .body(containsString("Saved report"));
    testThatDBSizeIsSize(1);
  }

  @Test
  public void testTooLargeFile(TestContext context) {
    Async async = context.async();
    given()
        .multiPart(new MultiPartSpecBuilder(createTitleReportWithSize(205).getBytes()).build())
        .post(PATH + PROVIDER_ID)
        .then()
        .statusCode(400)
        .body(containsString("File size exceeds the limit"));

    vertx.setTimer(
        2000,
        id -> {
          context.verify(v -> testThatDBIsEmpty());
          async.complete();
        });
  }

  @Test
  public void testMultipleFiles(TestContext context) {
    Async async = context.async();
    given()
        .header(HttpHeaders.CONTENT_TYPE, "multipart/form-data")
        .formParams(FORM_PARAMS)
        .multiPart(FILE_REPORT5_OK)
        .multiPart(FILE_REPORT_OK)
        .post("/counter-reports/multipartupload/provider/" + PROVIDER_ID)
        .then()
        .statusCode(400)
        .body(containsString("Multiple files are not supported"));

    vertx.setTimer(
        2000,
        id -> {
          context.verify(v -> testThatDBIsEmpty());
          async.complete();
        });
  }

  @Test
  public void testR4OkJR1GOA() {
    String savedReportId =
        given()
            .multiPart(FILE_REPORT_JR1GOA)
            .post(PATH + PROVIDER_ID)
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
    assertThat(savedReport.getYearMonth()).isEqualTo("2020-07");
    assertThat(savedReport.getDownloadTime()).isNotNull();
    assertThat(savedReport.getReportName()).isEqualTo("JR1 GOA");

    Report reportFromXML = JAXB.unmarshal(FILE_REPORT_JR1GOA, Report.class);
    Report reportFromDB = Counter4Utils.fromJSON(Json.encode(savedReport.getReport()));
    assertThat(reportFromXML).usingRecursiveComparison().isEqualTo(reportFromDB);
  }

  @Test
  public void testR4Ok() {
    String savedReportId =
        given()
            .multiPart(FILE_REPORT_OK)
            .post(PATH + PROVIDER_ID)
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
  public void testR4OkWithEditReason() {
    String savedReportId =
        given()
            .formParams(FORM_PARAMS)
            .multiPart(FILE_REPORT_OK)
            .post(PATH + PROVIDER_ID)
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
    assertThat(savedReport.getReportEditedManually()).isTrue();
    assertThat(savedReport.getEditReason()).isEqualTo(EDIT_REASON);

    Report reportFromXML = JAXB.unmarshal(FILE_REPORT_OK, Report.class);
    Report reportFromDB = Counter4Utils.fromJSON(Json.encode(savedReport.getReport()));
    assertThat(reportFromXML).usingRecursiveComparison().isEqualTo(reportFromDB);
  }

  @Test
  public void testR4UnsupportedReport() {
    given()
        .multiPart(FILE_REPORT_UNSUPPORTED)
        .post(PATH + PROVIDER_ID)
        .then()
        .statusCode(400)
        .body(containsString("Unsupported report"));
  }

  @Test
  public void testR4OkOverwriteTrue() {
    String savedReportId =
        given()
            .multiPart(FILE_REPORT_OK)
            .queryParam("overwrite", true)
            .post(PATH + PROVIDER_ID)
            .then()
            .statusCode(200)
            .extract()
            .asString()
            .replace("Saved report with id ", "");

    String overwriteReportId =
        given()
            .multiPart(FILE_REPORT_OK)
            .queryParam("overwrite", true)
            .post(PATH + PROVIDER_ID)
            .then()
            .statusCode(200)
            .extract()
            .asString()
            .replace("Saved report with id ", "");
    assertThat(savedReportId).isEqualTo(overwriteReportId);
    testThatDBSizeIsSize(1);
  }

  @Test
  public void testR4OkOverwriteFalse() {
    given()
        .multiPart(FILE_REPORT_OK)
        .queryParam("overwrite", false)
        .post(PATH + PROVIDER_ID)
        .then()
        .statusCode(200);

    given()
        .multiPart(FILE_REPORT_OK)
        .queryParam("overwrite", false)
        .post(PATH + PROVIDER_ID)
        .then()
        .statusCode(500)
        .body(containsString("Report already existing"))
        .body(containsString("2018-03"));
  }

  @Test
  public void testR4NssOk() {
    String savedReportId =
        given()
            .multiPart(FILE_REPORT_NSS_OK)
            .post(PATH + PROVIDER_ID)
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
  public void testR5Ok() throws Exception {
    String savedReportId =
        given()
            .multiPart(FILE_REPORT5_OK)
            .post(PATH + PROVIDER_ID)
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
    assertThat(savedReport.getYearMonth()).isEqualTo("2022-01");
    assertThat(savedReport.getDownloadTime()).isNotNull();
    assertThat(savedReport.getReportName()).isEqualTo("TR");

    org.folio.rest.jaxrs.model.Report report =
        Json.decodeValue(
            Files.asCharSource(FILE_REPORT5_OK, StandardCharsets.UTF_8).read(),
            org.folio.rest.jaxrs.model.Report.class);
    assertThat(savedReport.getReport()).usingRecursiveComparison().isEqualTo(report);
  }

  @Test
  public void testNoTenant() {
    RequestSpecification requestSpecification = RestAssured.requestSpecification;
    try {
      RestAssured.requestSpecification = null;
      given()
          .multiPart(FILE_REPORT5_OK)
          .post(PATH + PROVIDER_ID)
          .then()
          .statusCode(400)
          .body(containsString("Tenant must be set"));
    } finally {
      RestAssured.requestSpecification = requestSpecification;
    }
  }

  @Test
  public void testR5NotOk() {
    given()
        .multiPart(FILE_REPORT5_NOT_OK)
        .post(PATH + PROVIDER_ID)
        .then()
        .statusCode(400)
        .body(containsString(MSG_UNSUPPORTED_REPORT));
  }

  @Test
  public void testInvalidContent() {
    given()
        .multiPart(FILE_NO_REPORT)
        .post(PATH + PROVIDER_ID)
        .then()
        .statusCode(400)
        .body(containsString(MSG_WRONG_FORMAT));
  }

  @Test
  public void testMultipleMonthsC4() {
    String createdIds =
        given()
            .multiPart(FILE_REPORT_MULTI_COP4)
            .post(PATH + PROVIDER_ID)
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
        .multiPart(FILE_REPORT_MULTI_COP4)
        .post(PATH + PROVIDER_ID)
        .then()
        .statusCode(500)
        .body(containsString("Report already existing"))
        .body(containsString("2018-03"))
        .body(containsString("2018-04"));
  }

  @Test
  public void testMultipleMonthsC4FromCsv() throws IOException, ReportSplitException {
    String csvString = Counter4Utils.toCSV(JAXB.unmarshal(FILE_REPORT_MULTI_COP4, Report.class));
    assertThat(csvString).isNotNull();

    String createdIds =
        given()
            .multiPart(new MultiPartSpecBuilder(csvString.getBytes()).build())
            .post(PATH + PROVIDER_ID)
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
            .toList();

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
        .multiPart(FILE_REPORT_MULTI_COP4)
        .post(PATH + PROVIDER_ID)
        .then()
        .statusCode(500)
        .body(containsString("Report already existing"))
        .body(containsString("2018-03"))
        .body(containsString("2018-04"));
  }

  @Test
  public void testMultipleMonthsC5() {
    String createdIds =
        given()
            .multiPart(FILE_REPORT_MULTI_COP5)
            .post(PATH + PROVIDER_ID)
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
        .multiPart(FILE_REPORT_MULTI_COP5)
        .post(PATH + PROVIDER_ID)
        .then()
        .statusCode(500)
        .body(containsString("Report already existing"))
        .body(containsString("2019-09"))
        .body(containsString("2019-10"))
        .body(containsString("2019-11"));
  }

  @Test
  public void testMultipleMonthsC5FromCsv() throws IOException, Counter5UtilsException {
    String jsonString =
        Resources.toString(FILE_REPORT_MULTI_COP5.toURI().toURL(), StandardCharsets.UTF_8);
    assertThat(jsonString).isNotNull();

    Object report = Counter5Utils.fromJSON(jsonString);
    String csvString = Counter5Utils.toCSV(report);
    assertThat(csvString).isNotNull();

    String createdIds =
        given()
            .multiPart(new MultiPartSpecBuilder(csvString.getBytes()).build())
            .post(PATH + PROVIDER_ID)
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
    ArrayList<Object> expectedReports = new ArrayList<Object>(Counter5Utils.split(report));
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
        .multiPart(FILE_REPORT_MULTI_COP5)
        .post(PATH + PROVIDER_ID)
        .then()
        .statusCode(500)
        .body(containsString("Report already existing"))
        .body(containsString("2019-09"))
        .body(containsString("2019-10"))
        .body(containsString("2019-11"));
  }

  @Test
  public void testMultipleMonthsC5FromCsvWithEditReason()
      throws IOException, Counter5UtilsException {
    String jsonString =
        Resources.toString(FILE_REPORT_MULTI_COP5.toURI().toURL(), StandardCharsets.UTF_8);
    assertThat(jsonString).isNotNull();

    Object report = Counter5Utils.fromJSON(jsonString);
    String csvString = Counter5Utils.toCSV(report);
    assertThat(csvString).isNotNull();

    String createdIds =
        given()
            .formParams(FORM_PARAMS)
            .multiPart(new MultiPartSpecBuilder(csvString.getBytes()).build())
            .post(PATH + PROVIDER_ID)
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
    reports
        .getCounterReports()
        .forEach(
            counterReport -> {
              assertThat(counterReport.getReportEditedManually()).isTrue();
              assertThat(counterReport.getEditReason()).isEqualTo(EDIT_REASON);
            });
  }

  @Test
  public void testProviderNotFound() {
    given()
        .multiPart(FILE_REPORT_OK)
        .post(PATH + PROVIDER_ID2)
        .then()
        .statusCode(500)
        .body(containsString(PROVIDER_ID2), containsString("not found"));
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

  private void compareCOP5Reports(
      List<Object> actualReports, ArrayList<Object> expectedReports, int index) {
    Object first = expectedReports.get(index);
    if (first instanceof COUNTERDatabaseReport) {
      COUNTERDatabaseReport actual = (COUNTERDatabaseReport) actualReports.get(index);
      COUNTERDatabaseReport expected = (COUNTERDatabaseReport) expectedReports.get(index);
      assertThat(actual.getReportHeader())
          .usingRecursiveComparison()
          .ignoringFields("customerID")
          .ignoringFields("created")
          .ignoringCollectionOrder()
          .isEqualTo(expected.getReportHeader());
      assertThat(actual.getReportItems()).hasSameSizeAs(expected.getReportItems());

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
          .ignoringFields("created")
          .ignoringCollectionOrder()
          .isEqualTo(expected.getReportHeader());
      assertThat(actual.getReportItems()).hasSameSizeAs(expected.getReportItems());

      // Compare each platform usage
      for (int i = 0; i < actual.getReportItems().size(); i++) {
        COUNTERTitleUsage actualUsage = actual.getReportItems().get(i);
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
      fail(String.format("Comparing reports of type %s not implemented", first.getClass()));
    }
  }

  private Object cop5ReportFromJson(String json) {
    try {
      return Counter5Utils.fromJSON(json);
    } catch (Counter5UtilsException e) {
      throw new CounterReportUploadException(e);
    }
  }

  public static class CounterReportUploadException extends RuntimeException {

    public CounterReportUploadException(Throwable cause) {
      super(cause);
    }
  }
}
