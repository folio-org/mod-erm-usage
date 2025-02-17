package org.folio.rest.impl;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.folio.rest.impl.CounterReportExportIT.ExportFormat.CSV;
import static org.folio.rest.impl.CounterReportExportIT.ExportFormat.XLSX;
import static org.folio.rest.util.ReportExportHelper.NO_CSV_MAPPER_AVAILABLE;
import static org.folio.rest.util.ReportExportHelper.NO_REPORT_DATA;
import static org.folio.rest.util.ReportExportHelper.UNSUPPORTED_COUNTER_VERSION_MSG;
import static org.folio.rest.util.ReportExportHelper.UNSUPPORTED_FORMAT_MSG;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

import com.google.common.net.HttpHeaders;
import com.google.common.net.MediaType;
import io.restassured.RestAssured;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.parsing.Parser;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.postgres.testing.PostgresTesterContainer;
import org.folio.rest.RestVerticle;
import org.folio.rest.client.TenantClient;
import org.folio.rest.jaxrs.model.CounterReport;
import org.folio.rest.jaxrs.model.Parameter;
import org.folio.rest.jaxrs.model.Report;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.NetworkUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.olf.erm.usage.counter.common.ExcelUtil;

@ExtendWith(VertxExtension.class)
class CounterReportExportIT {

  static final String BASE_URI = "http://localhost";
  static final String BASE_PATH = "/counter-reports";
  static final String PATH_EXPORT_REPORT_ID = "/export/{id}";
  static final String PATH_EXPORT_PROVIDER_ID =
      "/export/provider/{id}/report/{name}/version/{version}/from/{begin}/to/{end}";
  private static final String TENANT = "diku";
  private static final String NON_EXISTENT_REPORT_ID = "13aa6f47-509b-4fe2-affb-c5a58cce69b4";
  private static final Vertx vertx = Vertx.vertx();
  private static final WebClient webClient = WebClient.create(vertx);
  private static final int port = NetworkUtils.nextFreePort();
  private static final Report INVALID_REPORT =
      Json.decodeValue("{ \"abc\": \"123\" }", Report.class);

  @BeforeAll
  static void beforeAll(VertxTestContext testContext) {
    RestAssured.reset();
    RestAssured.baseURI = BASE_URI;
    RestAssured.basePath = BASE_PATH;
    RestAssured.port = port;
    RestAssured.defaultParser = Parser.JSON;
    // RestAssured.filters(new ResponseLoggingFilter(), new RequestLoggingFilter());
    RestAssured.requestSpecification =
        new RequestSpecBuilder()
            .addHeader(XOkapiHeaders.TENANT, TENANT)
            .addHeader(HttpHeaders.CONTENT_TYPE, MediaType.JSON_UTF_8.toString())
            .build();

    PostgresClient.setPostgresTester(new PostgresTesterContainer());
    PostgresClient.getInstance(vertx).startPostgresTester();

    TenantClient tenantClient = new TenantClient(BASE_URI + ":" + port, TENANT, null, webClient);
    TenantAttributes ta = new TenantAttributes();
    ta.setParameters(List.of(new Parameter().withKey("loadSample").withValue("true")));

    DeploymentOptions options =
        new DeploymentOptions().setConfig(new JsonObject().put("http.port", port));
    vertx
        .deployVerticle(RestVerticle.class.getName(), options)
        .compose(v -> tenantClient.postTenant(ta))
        .onComplete(testContext.succeedingThenComplete());
  }

  @AfterAll
  static void afterAll() {
    PostgresClient.stopPostgresTester();
    webClient.close();
    vertx.close();
  }

  public static Stream<Arguments> provideTestDataAndExportFormatCombinations() {
    return Arrays.stream(TestData.values())
        .flatMap(
            testData ->
                Arrays.stream(ExportFormat.values())
                    .map(exportFormat -> Arguments.of(testData, exportFormat)));
  }

  private static void postCounterReport(CounterReport counterReport) {
    given().body(counterReport).post().then().statusCode(201);
  }

  private static CounterReport createCounterReport(
      String reportId, String release, String reportName, String providerId, Report report) {
    return new CounterReport()
        .withId(reportId)
        .withRelease(release)
        .withReportName(reportName)
        .withDownloadTime(Date.from(Instant.now()))
        .withProviderId(providerId)
        .withYearMonth("2020-01")
        .withReport(report);
  }

  private static Response getExportByProviderId(TestData testData, ExportFormat exportFormat) {
    return getExportByProviderId(
        testData.providerId,
        testData.reportName,
        testData.release,
        testData.beginDate,
        testData.endDate,
        exportFormat.format);
  }

  private static Response getExportByProviderId(TestData testData, String exportFormat) {
    return getExportByProviderId(
        testData.providerId,
        testData.reportName,
        testData.release,
        testData.beginDate,
        testData.endDate,
        exportFormat);
  }

  private static Response getExportByProviderId(
      String providerId,
      String reportName,
      String version,
      String beginDate,
      String endDate,
      String exportFormat) {
    RequestSpecification requestSpecification =
        given()
            .pathParam("id", providerId)
            .pathParam("name", reportName)
            .pathParam("version", version)
            .pathParam("begin", beginDate)
            .pathParam("end", endDate);

    if (exportFormat != null) {
      requestSpecification.queryParam("format", exportFormat);
    }

    return requestSpecification.get(PATH_EXPORT_PROVIDER_ID);
  }

  private static Response getExportByReportId(String id, ExportFormat exportFormat) {
    return getExportByReportId(id, exportFormat.format);
  }

  private static Response getExportByReportId(String id, String exportFormat) {
    RequestSpecification requestSpecification = given().pathParam("id", id);

    if (exportFormat != null) {
      requestSpecification.queryParam("format", exportFormat);
    }

    return requestSpecification.get(PATH_EXPORT_REPORT_ID);
  }

  @ParameterizedTest
  @EnumSource(TestData.class)
  void testThatExportByReportIdReturnsCsv(TestData testData) {
    String csv =
        getExportByReportId(testData.reportId, CSV)
            .then()
            .statusCode(200)
            .contentType(CSV.contentType)
            .extract()
            .asString();
    assertThat(csv).contains(testData.expectedByReportIdCsvLine);
  }

  @ParameterizedTest
  @EnumSource(TestData.class)
  void testThatExportByReportIdReturnsCsvByDefault(TestData testData) {
    String csv =
        getExportByReportId(testData.reportId, (String) null)
            .then()
            .statusCode(200)
            .contentType(CSV.contentType)
            .extract()
            .asString();
    assertThat(csv).contains(testData.expectedByReportIdCsvLine);
  }

  @ParameterizedTest
  @EnumSource(TestData.class)
  void testThatExportByReportIdReturnsXlsx(TestData testData) throws IOException {
    InputStream inputStream =
        getExportByReportId(testData.reportId, XLSX)
            .then()
            .statusCode(200)
            .contentType(XLSX.contentType)
            .extract()
            .asInputStream();
    String csv = ExcelUtil.toCSV(inputStream);
    assertThat(csv).contains(testData.expectedByReportIdCsvLine);
  }

  @ParameterizedTest
  @EnumSource(TestData.class)
  void testExportByReportIdWithUnsupportedExportFormat(TestData testData) {
    String requestedFormat = "tsv";
    getExportByReportId(testData.reportId, requestedFormat)
        .then()
        .statusCode(400)
        .body(equalTo(UNSUPPORTED_FORMAT_MSG.formatted(requestedFormat)));
  }

  @ParameterizedTest
  @MethodSource("provideTestDataAndExportFormatCombinations")
  void testExportByReportIdWithMissingReportData(TestData testData, ExportFormat format) {
    getExportByReportId(testData.failedReportId, format)
        .then()
        .statusCode(422)
        .body(equalTo(NO_REPORT_DATA));
  }

  @ParameterizedTest
  @EnumSource(ExportFormat.class)
  void testExportByReportIdWithUnsupportedReleaseVersion(ExportFormat exportFormat) {
    String reportId = UUID.randomUUID().toString();
    String providerId = UUID.randomUUID().toString();
    CounterReport counterReport =
        createCounterReport(reportId, "3", "TR", providerId, new Report());

    postCounterReport(counterReport);
    getExportByReportId(reportId, exportFormat)
        .then()
        .statusCode(500)
        .body(equalTo(NO_CSV_MAPPER_AVAILABLE));
  }

  @ParameterizedTest
  @MethodSource("provideTestDataAndExportFormatCombinations")
  void testExportByReportIdWithInvalidReportData(TestData testData, ExportFormat exportFormat) {
    String reportId = UUID.randomUUID().toString();
    String providerId = UUID.randomUUID().toString();
    CounterReport counterReport =
        createCounterReport(
            reportId, testData.release, testData.reportName, providerId, INVALID_REPORT);

    postCounterReport(counterReport);
    getExportByReportId(reportId, exportFormat).then().statusCode(500);
  }

  @ParameterizedTest
  @EnumSource(ExportFormat.class)
  void testExportByReportIdWithNonExistentReportId(ExportFormat exportFormat) {
    getExportByReportId(NON_EXISTENT_REPORT_ID, exportFormat).then().statusCode(404);
  }

  @ParameterizedTest
  @EnumSource(TestData.class)
  void testThatExportByProviderIdReturnsCsv(TestData testData) {
    getExportByProviderId(testData, CSV)
        .then()
        .statusCode(200)
        .contentType(CSV.contentType)
        .body(containsString(testData.expectedByProviderIdCsvLine));
  }

  @ParameterizedTest
  @EnumSource(TestData.class)
  void testThatExportByProviderIdReturnsCsvByDefault(TestData testData) {
    getExportByProviderId(testData, (String) null)
        .then()
        .statusCode(200)
        .contentType(CSV.contentType)
        .body(containsString(testData.expectedByProviderIdCsvLine));
  }

  @ParameterizedTest
  @EnumSource(TestData.class)
  void testThatExportByProviderIdReturnsXlsx(TestData testData) throws IOException {
    InputStream inputStream =
        getExportByProviderId(testData, XLSX)
            .then()
            .statusCode(200)
            .contentType(XLSX.contentType)
            .extract()
            .asInputStream();

    String csv = ExcelUtil.toCSV(inputStream);
    assertThat(csv).contains(testData.expectedByProviderIdCsvLine);
  }

  @ParameterizedTest
  @EnumSource(TestData.class)
  void testExportByProviderIdWithUnsupportedExportFormat(TestData testData) {
    String requestedFormat = "tsv";
    getExportByProviderId(testData, requestedFormat)
        .then()
        .statusCode(400)
        .body(equalTo(UNSUPPORTED_FORMAT_MSG.formatted(requestedFormat)));
  }

  @ParameterizedTest
  @MethodSource("provideTestDataAndExportFormatCombinations")
  void testExportByProviderIdWithUnsupportedVersion(TestData testData, ExportFormat exportFormat) {
    String version = "3";
    getExportByProviderId(
            testData.providerId,
            testData.reportName,
            version,
            testData.beginDate,
            testData.endDate,
            exportFormat.format)
        .then()
        .statusCode(400)
        .body(equalTo(UNSUPPORTED_COUNTER_VERSION_MSG.formatted(version)));
  }

  @ParameterizedTest
  @EnumSource(ExportFormat.class)
  void testExportByProviderIdWithNonExistentProviderId(ExportFormat exportFormat) {
    getExportByProviderId("1234", "TR", "5", "2022-01", "2022-12", exportFormat.format)
        .then()
        .statusCode(500)
        .body(equalTo("Merged report is null"));
  }

  enum TestData {
    REPORT_4_JR1(
        "4",
        "JR1",
        "1ad95437-4f1b-4f3b-9654-622ff28d271d",
        "c65805a8-dd3d-4740-82b9-54744fe4f3e5",
        "International Journal of Internet of Things and Cyber-Assurance,Inderscience Publishers,"
            + "Inderscience Publishers,10.1504/ijitca,ijitca,2059-7967,2059-7975,1,,1,1",
        "d54f9d37-7759-44b6-a621-f950e6332d32",
        "2019-01",
        "2019-12",
        "International Journal of Agile Systems and Management,Inderscience Publishers,"
            + "Inderscience Publishers,10.1504/ijasm,ijasm,1741-9174,1741-9182,1,,1,,,,,,1,,,,,,"),
    REPORT_5_TR(
        "5",
        "TR",
        "f4e81505-cd81-4dda-946e-fcdda8b6059c",
        "c71fa962-587d-4443-910e-4ff2aa6525aa",
        "Book 1715,Publisher 109,,PPDelta,,"
            + "ppdelta:1715,978-0-300-94426-6,,,,Book,Book,2012,Controlled,Regular,No_License,1,"
            + "1",
        "63c056c2-9e56-4148-8002-2a8649827d71",
        "2022-01",
        "2022-12",
        "Book 1715,Publisher 109,,PPDelta,,ppdelta:1715,978-0-300-94426-6,,,,Book,,2012,"
            + "Controlled,Regular,Unique_Title_Investigations,5,0,0,5"),
    REPORT_51_TR(
        "5.1",
        "TR",
        "ae030429-6d98-42bc-9d01-5104b995fa6b",
        "7200c0bc-6d16-4442-a7f9-b589190af7de",
        "Title 1,Sample Publisher,ISNI:4321432143214321,Platform 1,10.9999/xxxxt01,P1:T01,"
            + "979-8-88888-888-8,,,https://doi.org/10.9999/xxxxt01,Book,2022,Controlled,Regular,"
            + "No_License,50,50",
        "63c056c2-9e56-4148-8002-2a8649827d71",
        "2022-01",
        "2022-12",
        "Title 3,Sample Publisher,ISNI:4321432143214321,Platform 1,10.9999/xxxxt03,P1:T03,,,"
            + "1234-4321,https://doi.org/10.9999/xxxxt03,Journal,2022,Controlled,Regular,"
            + "No_License,638,48,80,37,42,37,76,47,80,42,30,74,45"),
    ;

    public final String release;
    public final String reportName;
    public final String reportId;
    public final String failedReportId;
    public final String expectedByReportIdCsvLine;
    public final String providerId;
    public final String beginDate;
    public final String endDate;
    public final String expectedByProviderIdCsvLine;

    TestData(
        String release,
        String reportName,
        String reportId,
        String failedReportId,
        String expectedByReportIdCsvLine,
        String providerId,
        String beginDate,
        String endDate,
        String expectedByProviderIdCsvLine) {
      this.release = release;
      this.reportName = reportName;
      this.reportId = reportId;
      this.failedReportId = failedReportId;
      this.expectedByReportIdCsvLine = expectedByReportIdCsvLine;
      this.providerId = providerId;
      this.beginDate = beginDate;
      this.endDate = endDate;
      this.expectedByProviderIdCsvLine = expectedByProviderIdCsvLine;
    }
  }

  enum ExportFormat {
    CSV("csv", MediaType.CSV_UTF_8.withoutParameters().toString()),
    XLSX("xlsx", MediaType.OOXML_SHEET.toString());

    public final String format;
    public final String contentType;

    ExportFormat(String format, String contentType) {
      this.format = format;
      this.contentType = contentType;
    }
  }
}
