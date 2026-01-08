package performance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.folio.rest.util.Constants.TABLE_NAME_COUNTER_REPORTS;

import com.google.common.base.Stopwatch;
import com.google.common.io.Resources;
import com.google.common.net.HttpHeaders;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.Year;
import java.time.YearMonth;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import javax.ws.rs.core.MediaType;
import org.apache.commons.lang3.StringUtils;
import org.folio.rest.RestVerticle;
import org.folio.rest.jaxrs.model.CounterReport;
import org.folio.rest.jaxrs.model.Report;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.NetworkUtils;
import org.folio.rest.util.PostgresContainerRule;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(PerformanceTest.class)
@RunWith(VertxUnitRunner.class)
public class CounterReportAPIPerformanceIT {
  private static final int PORT = NetworkUtils.nextFreePort();
  private static final String TENANT = "diku";
  private static final List<String> PROVIDER_IDS =
      IntStream.rangeClosed(1, 20).mapToObj(i -> UUID.randomUUID().toString()).toList();
  private static final Logger log = LoggerFactory.getLogger(CounterReportAPIPerformanceIT.class);

  private static Vertx vertx = Vertx.vertx();

  @ClassRule
  public static PostgresContainerRule postgresRule = new PostgresContainerRule(vertx, TENANT);

  private static WebClient wc = WebClient.create(vertx);
  private static Report[] reports = new Report[12];
  @Rule public Timeout timeout = Timeout.seconds(300);

  @BeforeClass
  public static void beforeClass(TestContext context) {
    try {
      reports[0] =
          Json.decodeValue(
              Resources.toString(
                  Resources.getResource("performance/JR1-2019-01.json"), StandardCharsets.UTF_8),
              Report.class);
      reports[1] =
          Json.decodeValue(
              Resources.toString(
                  Resources.getResource("performance/JR1-2019-02.json"), StandardCharsets.UTF_8),
              Report.class);
      reports[2] =
          Json.decodeValue(
              Resources.toString(
                  Resources.getResource("performance/JR1-2019-03.json"), StandardCharsets.UTF_8),
              Report.class);
      IntStream.rangeClosed(3, 11).forEach(i -> reports[i] = null);
    } catch (Exception e) {
      context.fail(e);
    }

    DeploymentOptions options =
        new DeploymentOptions().setConfig(new JsonObject().put("http.port", PORT));

    vertx
        .deployVerticle(RestVerticle.class.getName(), options)
        .compose(resp -> saveBatch2())
        .compose(r -> executeSQL("REINDEX TABLE " + TENANT + "_mod_erm_usage.counter_reports;"))
        .compose(r -> executeSQL("VACUUM ANALYZE " + TENANT + "_mod_erm_usage.counter_reports;"))
        .compose(resultSet -> getCounterReports())
        .onComplete(
            context.asyncAssertSuccess(
                resp -> {
                  assertThat(resp.bodyAsJsonObject().getInteger("totalRecords"))
                      .isEqualTo(PROVIDER_IDS.size() * 12 * 10);
                }));
  }

  private static Future<RowSet<Row>> executeSQL(String sql) {
    return PostgresClient.getInstance(vertx).execute(sql);
  }

  private static Future<HttpResponse<Buffer>> getCounterReports() {
    return wc.get("/counter-reports")
        .putHeader("X-Okapi-Tenant", TENANT)
        .putHeader(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON)
        .addQueryParam("tiny", "true")
        .port(PORT)
        .send();
  }

  private static CounterReport createSampleCounterReport(
      String providerId, Report report, YearMonth yearMonth) {
    CounterReport cr = new CounterReport();
    cr.setProviderId(providerId);
    cr.setRelease("4");
    cr.setReportName("JR1");
    cr.setYearMonth(yearMonth.toString());
    cr.setReport(report);
    cr.setDownloadTime(Date.from(Instant.now()));
    if (report == null) {
      cr.setFailedReason(
          "Report not valid: Exception{Number=3000, Severity=ERROR, Message=Report Not Supported}");
      cr.setFailedAttempts(1);
    }
    return cr;
  }

  private static List<CounterReport> createSampleReports() {
    YearMonth start = YearMonth.of(2019, 1);
    return PROVIDER_IDS.stream()
        .flatMap(
            id ->
                IntStream.rangeClosed(1, 10)
                    .mapToObj(
                        y ->
                            IntStream.rangeClosed(1, 12)
                                .mapToObj(
                                    m ->
                                        createSampleCounterReport(
                                            id,
                                            reports[m - 1],
                                            start.plusYears(y - 1).plusMonths(m - 1)))
                                .toList()))
        .flatMap(Collection::stream)
        .toList();
  }

  private static Future<List<String>> saveBatch2() {
    PostgresClient pgClient = PostgresClient.getInstance(vertx, TENANT);
    List<CounterReport> sampleReports = createSampleReports();
    log.info("saving {} sample reports to postgres", sampleReports.size());

    int batchSize = 50;
    List<List<CounterReport>> batches = new java.util.ArrayList<>();
    for (int i = 0; i < sampleReports.size(); i += batchSize) {
      batches.add(sampleReports.subList(i, Math.min(i + batchSize, sampleReports.size())));
    }

    Future<List<String>> result = Future.succeededFuture(new java.util.ArrayList<>());
    for (List<CounterReport> batch : batches) {
      result =
          result.compose(
              accumulatedResults -> {
                List<Future<String>> batchFutures =
                    batch.stream()
                        .map(cr -> pgClient.save(TABLE_NAME_COUNTER_REPORTS, cr))
                        .toList();
                return Future.all(batchFutures)
                    .map(
                        cf -> {
                          List<String> batchResults =
                              batchFutures.stream().map(Future::result).toList();
                          accumulatedResults.addAll(batchResults);
                          return accumulatedResults;
                        });
              });
    }
    return result;
  }

  private Future<Entry<HttpResponse<Buffer>, Double>> getCsvReport(String providerId, Year year) {
    String url =
        String.format(
            "/counter-reports/export/provider/%s/report/JR1/version/4/from/%s/to/%s",
            providerId, year.atMonth(1).toString(), year.atMonth(12).toString());
    Stopwatch stopwatch = Stopwatch.createStarted();
    return wc.get(url)
        .setQueryParam("format", "csv")
        .putHeader("X-Okapi-Tenant", TENANT)
        .putHeader(HttpHeaders.ACCEPT, "text/csv")
        .port(PORT)
        .send()
        .map(
            resp -> {
              stopwatch.stop();
              return new SimpleImmutableEntry<>(
                  resp, stopwatch.elapsed(TimeUnit.MILLISECONDS) / 1000.0);
            });
  }

  private Future<Entry<HttpResponse<Buffer>, Double>> getErrorCodes() {
    String url = "/counter-reports/errors/codes";
    Stopwatch stopwatch = Stopwatch.createStarted();
    return wc.get(url)
        .putHeader("X-Okapi-Tenant", TENANT)
        .putHeader(HttpHeaders.ACCEPT, "application/json")
        .port(PORT)
        .send()
        .map(
            resp -> {
              stopwatch.stop();
              return new SimpleImmutableEntry<>(
                  resp, stopwatch.elapsed(TimeUnit.MILLISECONDS) / 1000.0);
            });
  }

  @Test
  public void testGetCsvReportMultipleMonthsPerformance(TestContext context) {
    List<Future<Entry<HttpResponse<Buffer>, Double>>> futures =
        List.of(
            getCsvReport(PROVIDER_IDS.get(0), Year.of(2019)),
            getCsvReport(PROVIDER_IDS.get(PROVIDER_IDS.size() - 1), Year.of(2019)),
            getCsvReport(PROVIDER_IDS.get(PROVIDER_IDS.size() - 1), Year.of(2028)));

    Future.all(futures)
        .map(cf -> futures.stream().map(Future::result).toList())
        .onComplete(
            context.asyncAssertSuccess(
                list -> {
                  list.forEach(
                      e ->
                          System.out.println(
                              String.format(
                                  "Received csv response in %ss: %s",
                                  e.getValue(),
                                  StringUtils.abbreviate(e.getKey().bodyAsString(), 30))));
                }));
  }

  @Test
  public void testGetErrorCodesPerformance(TestContext context) {
    getErrorCodes()
        .onComplete(
            context.asyncAssertSuccess(
                e -> {
                  System.out.println(
                      String.format(
                          "Received error codes response in %ss: %s",
                          e.getValue(), e.getKey().bodyAsJsonObject().encode()));
                }));
  }
}
