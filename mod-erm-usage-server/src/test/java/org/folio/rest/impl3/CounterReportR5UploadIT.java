package org.folio.rest.impl3;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.folio.rest.TestResources.PROVIDER;
import static org.folio.rest.TestResources.R51_SAMPLE_DRD2_OK;
import static org.folio.rest.TestResources.R51_SAMPLE_DR_INVALID_ATTRIBUTES;
import static org.folio.rest.TestResources.R51_SAMPLE_DR_INVALID_DATA;
import static org.folio.rest.TestResources.R51_SAMPLE_DR_OK;
import static org.folio.rest.TestResources.R51_SAMPLE_TR_TSV;
import static org.folio.rest.TestResources.R51_SAMPLE_TR_XLSX;
import static org.folio.rest.util.Constants.TABLE_NAME_COUNTER_REPORTS;
import static org.folio.rest.util.Constants.TABLE_NAME_UDP;
import static org.hamcrest.Matchers.containsString;

import io.restassured.response.Response;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.junit5.VertxTestContext;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.folio.rest.Setup;
import org.folio.rest.SetupTenant;
import org.folio.rest.TestResources;
import org.folio.rest.TestUtils;
import org.folio.rest.jaxrs.model.CounterReport;
import org.folio.rest.jaxrs.model.CounterReports;
import org.folio.rest.jaxrs.model.UsageDataProvider;
import org.folio.rest.persist.PostgresClient;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

@Setup
@SetupTenant
class CounterReportR5UploadIT {
  static final String MSG_SAVED_REPORT_WITH_IDS = "Saved report with ids: ";
  static final String QUERY_TEMPLATE = "?query=(providerId=%s)&limit=1000";
  static final String[] EXPECTED_MONTHS =
      new String[] {
        "2022-01", "2022-02", "2022-03", "2022-04", "2022-05", "2022-06", "2022-07", "2022-08",
        "2022-09", "2022-10", "2022-11", "2022-12"
      };
  static final String MSG_EXPECTED_ATTRIBUTES =
      "Expected 'reportAttributes' to be: {\"Attributes_To_Show\":[\"Access_Method\"]";
  static final String MSG_UNRECOGNIZED_FIELD = "Unrecognized field \"foo\"";
  static final String MSG_UNSUPPORTED_REPORT = "Unsupported report";
  static final String MSG_REPORT_ALREADY_EXISTING = "Report already existing";
  private static final String BASE_PATH = "/counter-reports";
  private static final String TENANT = TestUtils.getTenant();
  private static final Vertx vertx = TestUtils.getVertx();
  private static final String UPLOAD_PATH = "/multipartupload/provider/%s";
  private static final String PROVIDER_ID = "4b659cb9-e4bb-493d-ae30-5f5690c54802";

  @BeforeAll
  static void beforeAll(VertxTestContext testContext) {
    TestUtils.setupRestAssured(BASE_PATH, false);
    postProvider().onComplete(testContext.succeedingThenComplete());
  }

  @BeforeEach
  void beforeEach(VertxTestContext testContext) {
    TestUtils.clearTable(TENANT, TABLE_NAME_COUNTER_REPORTS)
        .onComplete(testContext.succeedingThenComplete());
  }

  private static Future<String> postProvider() {
    return PostgresClient.getInstance(vertx, TENANT)
        .save(
            TABLE_NAME_UDP,
            PROVIDER_ID,
            Json.decodeValue(PROVIDER.getAsString(), UsageDataProvider.class).withId(null));
  }

  private static Response postFile(TestResources testResource) {
    return given().multiPart(testResource.getAsFile()).post(UPLOAD_PATH.formatted(PROVIDER_ID));
  }

  private static CounterReports getCounterReports() {
    return given()
        .get(QUERY_TEMPLATE.formatted(PROVIDER_ID))
        .then()
        .statusCode(200)
        .extract()
        .as(CounterReports.class);
  }

  private static List<String> getIds(CounterReports reports) {
    return reports.getCounterReports().stream().map(CounterReport::getId).toList();
  }

  private static List<String> getYearMonths(CounterReports reports) {
    return reports.getCounterReports().stream().map(CounterReport::getYearMonth).toList();
  }

  @ParameterizedTest(name = "{0}")
  @ArgumentsSource(ValidReportsArgumentsProvider.class)
  void testUploadOfValidReportsInDifferentFormats(String testName, TestResources resource) {
    List<String> createdIds =
        Arrays.asList(
            postFile(resource)
                .then()
                .statusCode(200)
                .body(containsString(MSG_SAVED_REPORT_WITH_IDS))
                .extract()
                .asString()
                .replace(MSG_SAVED_REPORT_WITH_IDS, "")
                .split(","));
    CounterReports reports = getCounterReports();

    assertThat(getYearMonths(reports)).containsExactlyInAnyOrder(EXPECTED_MONTHS);
    assertThat(getIds(reports)).containsExactlyInAnyOrderElementsOf(createdIds);

    String postResult = postFile(resource).then().statusCode(500).extract().asString();
    assertThat(postResult).contains(MSG_REPORT_ALREADY_EXISTING).contains(EXPECTED_MONTHS);
  }

  @ParameterizedTest(name = "{0}")
  @ArgumentsSource(InvalidReportsArgumentsProvider.class)
  void testUploadOfInvalidReports(
      String testName, TestResources resource, String expectedErrorMessage) {
    postFile(resource).then().statusCode(400).body(containsString(expectedErrorMessage));
  }

  static class InvalidReportsArgumentsProvider implements ArgumentsProvider {
    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
      return Stream.of(
          Arguments.of("Unsupported Report - JSON", R51_SAMPLE_DRD2_OK, MSG_UNSUPPORTED_REPORT),
          Arguments.of("Invalid Data - JSON", R51_SAMPLE_DR_INVALID_DATA, MSG_UNRECOGNIZED_FIELD),
          Arguments.of(
              "Invalid Attributes - JSON",
              R51_SAMPLE_DR_INVALID_ATTRIBUTES,
              MSG_EXPECTED_ATTRIBUTES),
          Arguments.of(
              "Invalid Attributes - XLSX",
              R51_SAMPLE_DR_INVALID_ATTRIBUTES,
              MSG_EXPECTED_ATTRIBUTES));
    }
  }

  static class ValidReportsArgumentsProvider implements ArgumentsProvider {
    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) throws Exception {
      return Stream.of(
          Arguments.of("DR - JSON", R51_SAMPLE_DR_OK),
          Arguments.of("TR - TSV", R51_SAMPLE_TR_TSV),
          Arguments.of("TR - XLSX", R51_SAMPLE_TR_XLSX));
    }
  }
}
