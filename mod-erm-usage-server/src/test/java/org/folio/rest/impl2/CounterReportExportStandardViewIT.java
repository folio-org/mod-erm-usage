package org.folio.rest.impl2;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.folio.rest.util.Constants.TABLE_NAME_COUNTER_REPORTS;
import static org.folio.rest.util.Constants.TABLE_NAME_UDP;
import static org.folio.rest.util.ReportExportHelper.CREATED_BY_SUFFIX;

import com.google.common.io.Resources;
import io.restassured.RestAssured;
import io.restassured.parsing.Parser;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.assertj.core.util.Lists;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.rest.RestVerticle;
import org.folio.rest.jaxrs.model.CounterReport;
import org.folio.rest.jaxrs.model.HarvestingConfig;
import org.folio.rest.jaxrs.model.HarvestingConfig.HarvestVia;
import org.folio.rest.jaxrs.model.HarvestingConfig.HarvestingStatus;
import org.folio.rest.jaxrs.model.Report;
import org.folio.rest.jaxrs.model.SushiConfig;
import org.folio.rest.jaxrs.model.SushiCredentials;
import org.folio.rest.jaxrs.model.UsageDataProvider;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.NetworkUtils;
import org.folio.rest.util.PostgresContainerRule;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.olf.erm.usage.counter50.Counter5Utils;
import org.olf.erm.usage.counter50.Counter5Utils.Counter5UtilsException;
import org.openapitools.client.model.COUNTERTitleReport;

@RunWith(VertxUnitRunner.class)
public class CounterReportExportStandardViewIT {

  private static final String TENANT = "testtenant";
  private static final Vertx vertx = Vertx.vertx();
  private static final UsageDataProvider sampleUDP = createSampleUDP();
  private static final int HEADER_SIZE = 12;
  @ClassRule public static PostgresContainerRule pgRule = new PostgresContainerRule(vertx, TENANT);

  @BeforeClass
  public static void beforeClass(TestContext context) {
    int port = NetworkUtils.nextFreePort();
    RestAssured.reset();
    RestAssured.baseURI = "http://localhost";
    RestAssured.port = port;
    RestAssured.defaultParser = Parser.JSON;

    DeploymentOptions options =
        new DeploymentOptions().setConfig(new JsonObject().put("http.port", port));

    Async async = context.async();
    vertx
        .deployVerticle(RestVerticle.class.getName(), options)
        .compose(s -> loadData())
        .onSuccess(rs -> async.complete())
        .onFailure(context::fail);
  }

  public static Future<RowSet<Row>> loadData() {
    List<CounterReport> sampleReports =
        List.of(
            createSampleCounterReport(
                sampleUDP.getId(),
                Resources.getResource("standardviews/karger_tr_2021-01.json").getFile(),
                "TR",
                "2021-01"),
            createSampleCounterReport(
                sampleUDP.getId(),
                Resources.getResource("standardviews/karger_tr_2021-02.json").getFile(),
                "TR",
                "2021-02"),
            createSampleCounterReport(
                sampleUDP.getId(),
                Resources.getResource("standardviews/karger_tr_2021-03.json").getFile(),
                "TR",
                "2021-03"));
    PostgresClient pgClient = PostgresClient.getInstance(vertx, TENANT);
    return pgClient
        .save(TABLE_NAME_UDP, sampleUDP)
        .compose(s -> pgClient.saveBatch(TABLE_NAME_COUNTER_REPORTS, sampleReports));
  }

  @Test
  public void testExportTRJ1() throws IOException, Counter5UtilsException {
    String result =
        given()
            .headers(Map.of(XOkapiHeaders.TENANT, TENANT))
            .pathParam("id", sampleUDP.getId())
            .pathParam("name", "TR_J1")
            .pathParam("aversion", "5")
            .pathParam("begin", "2021-01")
            .pathParam("end", "2021-03")
            .queryParam("format", "csv")
            .get(
                "/counter-reports/export/provider/{id}/report/{name}/version/{aversion}/from/{begin}/to/{end}")
            .then()
            .statusCode(200)
            .extract()
            .asString();

    List<String> resultLines = IOUtils.readLines(new StringReader(result));
    COUNTERTitleReport expectedReport =
        (COUNTERTitleReport)
            Counter5Utils.fromJSON(
                Resources.toString(
                    Resources.getResource("standardviews/tr_j1.json"), StandardCharsets.UTF_8));
    expectedReport
        .getReportHeader()
        .setCreatedBy(expectedReport.getReportHeader().getCreatedBy() + " " + CREATED_BY_SUFFIX);
    String expectedCsv = Counter5Utils.toCSV(expectedReport);
    assertThat(expectedCsv).isNotNull();
    List<String> expectedLines = IOUtils.readLines(new StringReader(expectedCsv));

    assertThatCsvLinesAreMatching(resultLines, expectedLines);
  }

  @Test
  public void testExportTRB1() throws IOException, Counter5UtilsException {
    String result =
        given()
            .headers(Map.of(XOkapiHeaders.TENANT, TENANT))
            .pathParam("id", sampleUDP.getId())
            .pathParam("name", "TR_B1")
            .pathParam("aversion", "5")
            .pathParam("begin", "2021-01")
            .pathParam("end", "2021-03")
            .queryParam("format", "csv")
            .get(
                "/counter-reports/export/provider/{id}/report/{name}/version/{aversion}/from/{begin}/to/{end}")
            .then()
            .statusCode(200)
            .extract()
            .asString();

    List<String> resultLines = IOUtils.readLines(new StringReader(result));

    COUNTERTitleReport expectedReport =
        (COUNTERTitleReport)
            Counter5Utils.fromJSON(
                Resources.toString(
                    Resources.getResource("standardviews/tr_b1.json"), StandardCharsets.UTF_8));
    expectedReport
        .getReportHeader()
        .setCreatedBy(expectedReport.getReportHeader().getCreatedBy() + " " + CREATED_BY_SUFFIX);
    String expectedCsv = Counter5Utils.toCSV(expectedReport);
    assertThat(expectedCsv).isNotNull();
    List<String> expectedLines = IOUtils.readLines(new StringReader(expectedCsv));

    assertThatCsvLinesAreMatching(resultLines, expectedLines);
  }

  private static UsageDataProvider createSampleUDP() {
    return new UsageDataProvider()
        .withId(UUID.randomUUID().toString())
        .withLabel("TestProvider")
        .withHarvestingConfig(
            new HarvestingConfig()
                .withReportRelease(5)
                .withHarvestingStart("2021-01")
                .withHarvestingStatus(HarvestingStatus.INACTIVE)
                .withHarvestVia(HarvestVia.SUSHI)
                .withSushiConfig(
                    new SushiConfig()
                        .withServiceUrl("http://localhost:1234")
                        .withServiceType("cs50")))
        .withSushiCredentials(new SushiCredentials().withCustomerId("1234"));
  }

  private static CounterReport createSampleCounterReport(
      String providerId, String reportFile, String reportName, String yearMonth) {
    String reportStr;
    try {
      reportStr = Files.readString(Paths.get(reportFile));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return new CounterReport()
        .withId(UUID.randomUUID().toString())
        .withRelease("5")
        .withProviderId(providerId)
        .withReportName(reportName)
        .withYearMonth(yearMonth)
        .withReport(Json.decodeValue(reportStr, Report.class));
  }

  private void assertThatCsvLinesAreMatching(List<String> actual, List<String> expected) {
    assertThat(actual).hasSameSizeAs(expected).hasSizeGreaterThan(HEADER_SIZE);

    Map<String, List<String>> hactual = createHeaderMap(actual);
    Map<String, List<String>> hexpected = createHeaderMap(expected);
    assertThat(hactual)
        .usingRecursiveComparison()
        .ignoringCollectionOrder()
        .ignoringFields("Created")
        .isEqualTo(hexpected);

    List<String> cactual = actual.subList(HEADER_SIZE, actual.size());
    List<String> cexpected = expected.subList(HEADER_SIZE, expected.size());
    assertThat(cactual).containsExactlyInAnyOrderElementsOf(cexpected);
  }

  private Map<String, List<String>> createHeaderMap(List<String> lines) {
    return lines.subList(0, HEADER_SIZE).stream()
        .map(l -> l.split(","))
        .collect(
            Collectors.toMap(
                sa -> sa[0],
                sa ->
                    (sa.length > 1)
                        ? Arrays.stream(sa[1].split(";"))
                            .map(String::trim)
                            .collect(Collectors.toList())
                        : Lists.emptyList()));
  }
}
