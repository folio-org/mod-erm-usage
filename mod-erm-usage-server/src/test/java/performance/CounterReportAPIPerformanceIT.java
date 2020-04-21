package performance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.folio.rest.util.Constants.TABLE_NAME_COUNTER_REPORTS;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Maps;
import com.google.common.io.Resources;
import com.google.common.net.HttpHeaders;
import io.reactivex.Single;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.UpdateResult;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.ext.web.client.HttpResponse;
import io.vertx.reactivex.ext.web.client.WebClient;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.Year;
import java.time.YearMonth;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.ws.rs.core.MediaType;
import org.codehaus.plexus.util.StringUtils;
import org.folio.rest.RestVerticle;
import org.folio.rest.jaxrs.model.CounterReport;
import org.folio.rest.jaxrs.model.Report;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.NetworkUtils;
import org.folio.rest.util.EmbeddedPostgresRule;
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
      IntStream.rangeClosed(1, 20)
          .mapToObj(i -> UUID.randomUUID().toString())
          .collect(Collectors.toList());
  private static final Logger log = LoggerFactory.getLogger(CounterReportAPIPerformanceIT.class);
  @ClassRule public static EmbeddedPostgresRule postgresRule = new EmbeddedPostgresRule(TENANT);
  private static Vertx vertx = Vertx.vertx();
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

    Async async = context.async();
    DeploymentOptions options =
        new DeploymentOptions().setConfig(new JsonObject().put("http.port", PORT));
    vertx
        .rxDeployVerticle(RestVerticle.class.getName(), options)
        .flatMap(resp -> saveBatch2())
        .flatMap(r -> executeSQL("REINDEX TABLE " + TENANT + "_mod_erm_usage.counter_reports;"))
        .flatMap(r -> executeSQL("VACUUM ANALYZE " + TENANT + "_mod_erm_usage.counter_reports;"))
        .flatMap(resultSet -> getCounterReports())
        .subscribe(
            resp -> {
              context.verify(
                  v ->
                      assertThat(resp.bodyAsJsonObject().getInteger("totalRecords"))
                          .isEqualTo(PROVIDER_IDS.size() * 12 * 10));
              async.complete();
            },
            context::fail);
  }

  private static Single<UpdateResult> executeSQL(String sql) {
    return vertx
        .<UpdateResult>rxExecuteBlocking(
            promise ->
                PostgresClient.getInstance(vertx.getDelegate())
                    .execute(
                        sql,
                        ar -> {
                          if (ar.succeeded()) {
                            promise.complete(ar.result());
                          } else {
                            promise.fail(ar.cause());
                          }
                        }))
        .toSingle();
  }

  private static Single<HttpResponse<Buffer>> getCounterReports() {
    return wc.get("/counter-reports")
        .putHeader("X-Okapi-Tenant", TENANT)
        .putHeader(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON)
        .addQueryParam("tiny", "true")
        .port(PORT)
        .rxSend();
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
                                .collect(Collectors.toList())))
        .flatMap(Collection::stream)
        .collect(Collectors.toList());
  }

  /** saveBatch method is actually slower than multiple single saves */
  /*private static Single<ResultSet> saveBatch() {
    PostgresClient pgClient = PostgresClient.getInstance(vertx.getDelegate(), TENANT);
    List<CounterReport> sampleReports = createSampleReports();
    log.info("batch saving {} sample reports", sampleReports.size());
    return vertx
        .<ResultSet>rxExecuteBlocking(
            promise ->
                pgClient.saveBatch(
                    TABLE_NAME_COUNTER_REPORTS,
                    sampleReports,
                    ar -> {
                      if (ar.succeeded()) {
                        promise.complete(ar.result());
                      } else {
                        promise.fail(ar.cause());
                      }
                    }))
        .toSingle();
  }*/

  private static Single<List<String>> saveBatch2() {
    PostgresClient pgClient = PostgresClient.getInstance(vertx.getDelegate(), TENANT);
    List<CounterReport> sampleReports = createSampleReports();
    log.info("saving {} sample reports to postgres", sampleReports.size());

    List<Single<String>> singles =
        sampleReports.stream()
            .map(
                cr ->
                    vertx
                        .<String>rxExecuteBlocking(
                            promise ->
                                pgClient.save(
                                    TABLE_NAME_COUNTER_REPORTS,
                                    cr,
                                    ar -> {
                                      if (ar.succeeded()) {
                                        promise.complete(ar.result());
                                      } else {
                                        promise.fail(ar.cause());
                                      }
                                    }))
                        .toSingle())
            .collect(Collectors.toList());

    return Single.merge(singles).toList();
  }

  private Single<Entry<HttpResponse<Buffer>, Double>> getCsvReport(String providerId, Year year) {
    String url =
        String.format(
            "/counter-reports/csv/provider/%s/report/JR1/version/4/from/%s/to/%s",
            providerId, year.atMonth(1).toString(), year.atMonth(12).toString());
    Stopwatch stopwatch = Stopwatch.createUnstarted();
    return wc.get(url)
        .putHeader("X-Okapi-Tenant", TENANT)
        .putHeader(HttpHeaders.ACCEPT, "text/csv")
        .port(PORT)
        .rxSend()
        .doOnSubscribe(d -> stopwatch.start())
        .doAfterTerminate(stopwatch::stop)
        .map(resp -> Maps.immutableEntry(resp, stopwatch.elapsed(TimeUnit.MILLISECONDS) / 1000.0));
  }

  private Single<Entry<HttpResponse<Buffer>, Double>> getErrorCodes() {
    String url = "/counter-reports/errors/codes";
    Stopwatch stopwatch = Stopwatch.createUnstarted();
    return wc.get(url)
        .putHeader("X-Okapi-Tenant", TENANT)
        .putHeader(HttpHeaders.ACCEPT, "application/json")
        .port(PORT)
        .rxSend()
        .doOnSubscribe(d -> stopwatch.start())
        .doAfterTerminate(stopwatch::stop)
        .map(resp -> Maps.immutableEntry(resp, stopwatch.elapsed(TimeUnit.MILLISECONDS) / 1000.0));
  }

  @Test
  public void testGetCsvReportMultipleMonthsPerformance(TestContext context) {
    Async async = context.async();

    List<Single<Entry<HttpResponse<Buffer>, Double>>> singles =
        Arrays.asList(
            getCsvReport(PROVIDER_IDS.get(0), Year.of(2019)),
            getCsvReport(PROVIDER_IDS.get(PROVIDER_IDS.size() - 1), Year.of(2019)),
            getCsvReport(PROVIDER_IDS.get(PROVIDER_IDS.size() - 1), Year.of(2028)));

    Single.merge(singles)
        .toList()
        .subscribe(
            list -> {
              list.forEach(
                  e ->
                      System.out.println(
                          String.format(
                              "Received csv response in %ss: %s",
                              e.getValue(),
                              StringUtils.abbreviate(e.getKey().bodyAsString(), 30))));
              async.complete();
            },
            context::fail);
  }

  @Test
  public void testGetErrorCodesPerformance(TestContext context) {
    Async async = context.async();
    getErrorCodes()
        .subscribe(
            e -> {
              System.out.println(
                  String.format(
                      "Received error codes response in %ss: %s",
                      e.getValue(), e.getKey().bodyAsJsonObject().encode()));
              async.complete();
            },
            context::fail);
  }
}
