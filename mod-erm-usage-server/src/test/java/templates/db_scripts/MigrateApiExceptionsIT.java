package templates.db_scripts;

import static org.assertj.core.api.Assertions.assertThat;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.Tuple;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.StreamSupport;
import org.folio.rest.PostgresExtension;
import org.folio.rest.TestUtils;
import org.folio.rest.impl.TenantAPI;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.persist.PostgresClient;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests {@code migration/6.0.0/migrate_api_exceptions.sql} and the index change in {@code
 * custom_indexes.sql} on an upgrade from 5.2.0.
 */
@ExtendWith(PostgresExtension.class)
class MigrateApiExceptionsIT {

  private static final String TENANT = "apiexceptions";
  private static final String SCHEMA = PostgresClient.convertToPsqlStandard(TENANT);
  private static final String PROVIDER_ID = "bf6c9ddc-ff82-40c4-be64-dd2414bdcd72";
  private static final String OTHER_PROVIDER_ID = "6a8f4c2e-1b3d-4e5f-9a7b-8c9d0e1f2a3b";
  private static final String TRIGGER = "update_usage_data_providers_on_insert_or_update_or_delete";
  private static final String ERROR_CODE_PATTERN = "(?:Number=|\"Code\": ?)([0-9]{1,4})";

  /** Stand-in for the index of 5.2.x, which this upgrade drops. */
  private static final String LEGACY_INDEX =
      """
      CREATE INDEX counter_reports_custom_errorcodes_idx ON %1$s.counter_reports
        USING btree(SUBSTRING(jsonb->>'failedReason','%2$s'))
        WHERE jsonb ->> 'failedReason' IS NOT NULL;
      """;

  private static final String C4_SINGLE_ID = "1a1c1d1e-0000-4000-8000-000000000001";
  private static final String C4_MULTIPLE_ID = "1a1c1d1e-0000-4000-8000-000000000002";
  private static final String C5_OBJECT_ID = "1a1c1d1e-0000-4000-8000-000000000003";
  private static final String C5_ARRAY_ID = "1a1c1d1e-0000-4000-8000-000000000004";
  private static final String HTTP_ERROR_ID = "1a1c1d1e-0000-4000-8000-000000000005";
  private static final String CUT_OFF_ID = "1a1c1d1e-0000-4000-8000-000000000006";
  private static final String HTML_ID = "1a1c1d1e-0000-4000-8000-000000000007";
  private static final String TIMEOUT_ID = "1a1c1d1e-0000-4000-8000-000000000008";
  private static final String EXISTING_ID = "1a1c1d1e-0000-4000-8000-000000000009";
  private static final String SUCCESSFUL_ID = "1a1c1d1e-0000-4000-8000-000000000010";
  private static final String OTHER_PROVIDER_REPORT_ID = "1a1c1d1e-0000-4000-8000-000000000011";

  private static final Map<String, String> FAILED_REASONS = new LinkedHashMap<>();

  static {
    FAILED_REASONS.put(
        C4_SINGLE_ID,
        "Report not valid: Exception{Number=3031, Severity=ERROR, Message=Service has not yet"
            + " processed the usage for one or more of the requested months, Data=Months not"
            + " processed: 2018-07}");
    FAILED_REASONS.put(
        C4_MULTIPLE_ID,
        "Report not valid: Exception{Number=3030, Severity=ERROR, Message=No Usage Available for"
            + " Requested Dates}, Exception{Number=3040, Severity=WARNING, Message=Partial Data"
            + " Returned, HelpUrl=https://example.com/help, Data=Some data}");
    FAILED_REASONS.put(
        C5_OBJECT_ID,
        """
        {"Code":3032,"Message":"Usage no longer available for requested dates",\
        "Help_URL":"http://example.com","Data":"No usage data prior to Jan 2022 is available"}\
        """);
    FAILED_REASONS.put(
        C5_ARRAY_ID,
        """
        Report not valid: [{"Code":1011,"Severity":"Warning","Message":"Report Queued for \
        Processing","Data":{"Retry_After":"60"}},{"Severity":"Error","Message":"No code"},\
        {"Code":"3060","Message":"  "},{"Code":3062,"Help_URL":"not a url"}]\
        """);
    FAILED_REASONS.put(
        HTTP_ERROR_ID,
        """
        HTTP 401: Unauthorized - {"Code": 2010, "Severity": "Error", "Message": "Requestor is \
        Not Authorized to Access Usage for Institution", \
        "Help_URL": "https://example.com/counter/2010"}\
        """);
    FAILED_REASONS.put(
        CUT_OFF_ID,
        """
        HTTP 400: Bad Request - [{"Code":3030,"Message":"No Usage Available for Requested \
        Dates"},{"Code":3050,"Message":"Parameter Not Recognized in this Context"},\
        {"Code":3060,"Mess...\
        """);
    FAILED_REASONS.put(
        HTML_ID, "HTTP 500: Internal Server Error - <html><style>body {color: red}</style></html>");
    FAILED_REASONS.put(TIMEOUT_ID, "Error getting report: Connection timed out");
    FAILED_REASONS.put(EXISTING_ID, "{\"Code\":3030}");
    FAILED_REASONS.put(OTHER_PROVIDER_REPORT_ID, "{\"Code\":3000}");
  }

  private static final JsonArray EXISTING_API_EXCEPTIONS =
      new JsonArray().add(new JsonObject().put("code", 3030).put("message", "Kept as is"));

  private static PostgresClient pgClient;
  private static List<JsonObject> reportsAfterUpgrade;

  @BeforeAll
  static void beforeAll() throws Exception {
    pgClient = PostgresClient.getInstance(TestUtils.getVertx());
    JsonObject udp = loadSample("udproviders.sample");
    udp.getJsonObject("harvestingConfig").getJsonObject("sushiConfig").put("serviceType", "cs50");

    runSqlFile(new TenantAPI().sqlFile(TENANT, false, null, null, null));
    runSqlFile(LEGACY_INDEX.formatted(SCHEMA, ERROR_CODE_PATTERN));
    insert("usage_data_providers", PROVIDER_ID, udp);
    insert("usage_data_providers", OTHER_PROVIDER_ID, udp.copy().put("id", OTHER_PROVIDER_ID));
    FAILED_REASONS.forEach(
        (id, failedReason) -> {
          JsonObject report = report(id, failedReason);
          if (id.equals(EXISTING_ID)) {
            report.put("apiExceptions", EXISTING_API_EXCEPTIONS);
          }
          if (id.equals(OTHER_PROVIDER_REPORT_ID)) {
            report.put("providerId", OTHER_PROVIDER_ID);
          }
          insert("counter_reports", id, report);
        });
    insert("counter_reports", SUCCESSFUL_ID, report(SUCCESSFUL_ID, null));

    upgrade();
    reportsAfterUpgrade = getReports();
  }

  private static void upgrade() throws Exception {
    TenantAttributes upgrade =
        new TenantAttributes()
            .withModuleFrom("mod-erm-usage-5.2.0")
            .withModuleTo("mod-erm-usage-6.0.0");
    runSqlFile(new TenantAPI().sqlFile(TENANT, true, upgrade, "mod-erm-usage-5.2.0", null));
  }

  private static void runSqlFile(String... sql) {
    pgClient.runSqlFile(String.join("\n", sql)).await();
  }

  private static JsonObject loadSample(String name) throws IOException {
    return new JsonObject(Files.readString(Path.of("../ramls/examples", name)));
  }

  private static JsonObject report(String id, String failedReason) {
    JsonObject report =
        new JsonObject()
            .put("id", id)
            .put("downloadTime", "2026-01-01T00:00:00.000+00:00")
            .put("release", "5")
            .put("reportName", "TR")
            .put("yearMonth", "2026-01")
            .put("providerId", PROVIDER_ID);
    if (failedReason != null) {
      report.put("failedReason", failedReason).put("failedAttempts", 1);
    }
    return report;
  }

  private static void insert(String table, String id, JsonObject jsonb) {
    pgClient
        .execute(
            "INSERT INTO %s.%s (id, jsonb) VALUES ($1, $2)".formatted(SCHEMA, table),
            Tuple.of(UUID.fromString(id), jsonb))
        .await();
  }

  private static Row selectSingle(String sql) {
    return pgClient.selectSingle(sql.formatted(SCHEMA)).await();
  }

  private static List<JsonObject> getReports() {
    return StreamSupport.stream(
            pgClient
                .select("SELECT jsonb FROM %s.counter_reports ORDER BY id".formatted(SCHEMA))
                .await()
                .spliterator(),
            false)
        .map(row -> row.getJsonObject(0))
        .toList();
  }

  private static JsonObject getReport(String id) {
    return reportsAfterUpgrade.stream()
        .filter(report -> report.getString("id").equals(id))
        .findFirst()
        .orElseThrow();
  }

  private static JsonObject getUdp(String id) {
    return selectSingle("SELECT jsonb FROM %s.usage_data_providers WHERE id = '" + id + "'")
        .getJsonObject(0);
  }

  private static JsonObject apiException(int code) {
    return new JsonObject().put("code", code);
  }

  @Test
  void testCounter4Exception() {
    assertThat(getReport(C4_SINGLE_ID).getJsonArray("apiExceptions"))
        .isEqualTo(
            new JsonArray()
                .add(
                    apiException(3031)
                        .put(
                            "message",
                            "Service has not yet processed the usage for one or more of the"
                                + " requested months")
                        .put("data", "Months not processed: 2018-07")));
  }

  @Test
  void testSeveralCounter4Exceptions() {
    assertThat(getReport(C4_MULTIPLE_ID).getJsonArray("apiExceptions"))
        .isEqualTo(
            new JsonArray()
                .add(apiException(3030).put("message", "No Usage Available for Requested Dates"))
                .add(
                    apiException(3040)
                        .put("message", "Partial Data Returned")
                        .put("helpUrl", "https://example.com/help")
                        .put("data", "Some data")));
  }

  @Test
  void testCounter5Object() {
    assertThat(getReport(C5_OBJECT_ID).getJsonArray("apiExceptions"))
        .isEqualTo(
            new JsonArray()
                .add(
                    apiException(3032)
                        .put("message", "Usage no longer available for requested dates")
                        .put("helpUrl", "http://example.com")
                        .put("data", "No usage data prior to Jan 2022 is available")));
  }

  @Test
  void testCounter5ArrayKeepsOnlyExceptionsWithCodeAndValidValues() {
    assertThat(getReport(C5_ARRAY_ID).getJsonArray("apiExceptions"))
        .isEqualTo(
            new JsonArray()
                .add(
                    apiException(1011)
                        .put("message", "Report Queued for Processing")
                        .put("data", "{\"Retry_After\": \"60\"}"))
                .add(apiException(3060))
                .add(apiException(3062)));
  }

  @Test
  void testHttpErrorBody() {
    assertThat(getReport(HTTP_ERROR_ID).getJsonArray("apiExceptions"))
        .isEqualTo(
            new JsonArray()
                .add(
                    apiException(2010)
                        .put(
                            "message",
                            "Requestor is Not Authorized to Access Usage for Institution")
                        .put("helpUrl", "https://example.com/counter/2010")));
  }

  @Test
  void testCutOffBody() {
    assertThat(getReport(CUT_OFF_ID).getJsonArray("apiExceptions"))
        .isEqualTo(
            new JsonArray()
                .add(apiException(3030).put("message", "No Usage Available for Requested Dates"))
                .add(apiException(3050).put("message", "Parameter Not Recognized in this Context"))
                .add(apiException(3060)));
  }

  @Test
  void testReportsWithoutExceptionsStayWithoutApiExceptions() {
    assertThat(List.of(HTML_ID, TIMEOUT_ID, SUCCESSFUL_ID))
        .allSatisfy(id -> assertThat(getReport(id).containsKey("apiExceptions")).isFalse());
  }

  @Test
  void testExistingApiExceptionsAreUnchanged() {
    assertThat(getReport(EXISTING_ID).getJsonArray("apiExceptions"))
        .isEqualTo(EXISTING_API_EXCEPTIONS);
  }

  @Test
  void testFailedReasonsAreUnchanged() {
    FAILED_REASONS.forEach(
        (id, failedReason) ->
            assertThat(getReport(id).getString("failedReason")).isEqualTo(failedReason));
  }

  @Test
  void testEveryCodeOfTheFormerPatternIsMigrated() {
    String missingCodes =
        """
        SELECT count(*) FROM %s.counter_reports cr,
          regexp_matches(cr.jsonb->>'failedReason', '%s', 'g') AS t(m)
        WHERE NOT COALESCE(cr.jsonb->'apiExceptions', '[]'::jsonb)
          @> jsonb_build_array(jsonb_build_object('code', m[1]::int))
        """;
    assertThat(
            pgClient
                .selectSingle(missingCodes.formatted(SCHEMA, ERROR_CODE_PATTERN))
                .await()
                .getLong(0))
        .isZero();
  }

  @Test
  void testNoEmptyApiExceptions() {
    assertThat(
            selectSingle(
                    "SELECT count(*) FROM %s.counter_reports WHERE jsonb->'apiExceptions' = '[]'")
                .getLong(0))
        .isZero();
  }

  @Test
  void testMigrationIsIdempotent() throws Exception {
    upgrade();
    assertThat(getReports()).isEqualTo(reportsAfterUpgrade);
  }

  @Test
  void testReportErrorCodesAreRecomputed() {
    assertThat(getUdp(PROVIDER_ID).getJsonArray("reportErrorCodes"))
        .containsExactly(
            "1011", "2010", "3030", "3031", "3032", "3040", "3050", "3060", "3062", "other");
    assertThat(getUdp(OTHER_PROVIDER_ID).getJsonArray("reportErrorCodes")).containsExactly("3000");
  }

  @Test
  void testTriggerIsEnabledAgain() {
    String triggerEnabled =
        """
        SELECT t.tgenabled::text FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid
          JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = '%s' AND t.tgname = '%s'
        """;
    assertThat(
            pgClient.selectSingle(triggerEnabled.formatted(SCHEMA, TRIGGER)).await().getString(0))
        .isEqualTo("O");
  }

  @Test
  void testHelperFunctionsAreDropped() {
    String helperFunctions =
        """
        SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = '%s' AND p.proname LIKE 'api\\_exceptions\\_%%'
        """;
    assertThat(selectSingle(helperFunctions).getLong(0)).isZero();
  }

  @Test
  void testErrorCodesIndexIsReplaced() {
    String indexCount =
        "SELECT count(*) FROM pg_indexes WHERE schemaname = '%s' AND indexname = '%s'";
    assertThat(
            pgClient
                .selectSingle(indexCount.formatted(SCHEMA, "counter_reports_custom_errorcodes_idx"))
                .await()
                .getLong(0))
        .isZero();
    assertThat(
            pgClient
                .selectSingle(indexCount.formatted(SCHEMA, "counter_reports_custom_failed_idx"))
                .await()
                .getLong(0))
        .isOne();
  }
}
