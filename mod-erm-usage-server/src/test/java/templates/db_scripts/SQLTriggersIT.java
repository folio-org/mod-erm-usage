package templates.db_scripts;

import static io.vertx.core.Future.succeededFuture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.folio.rest.jaxrs.model.AccountConfig.ConfigType.MANUAL;
import static org.folio.rest.jaxrs.model.HarvestingConfig.HarvestingStatus.INACTIVE;
import static org.folio.rest.jaxrs.model.UsageDataProvider.HasFailedReport.NO;
import static org.folio.rest.jaxrs.model.UsageDataProvider.HasFailedReport.YES;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import org.folio.rest.jaxrs.model.AccountConfig;
import org.folio.rest.jaxrs.model.Aggregator;
import org.folio.rest.jaxrs.model.AggregatorSetting;
import org.folio.rest.jaxrs.model.CounterReport;
import org.folio.rest.jaxrs.model.HarvestingConfig;
import org.folio.rest.jaxrs.model.UsageDataProvider;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.util.PostgresContainerRule;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Tests that the SQL triggers defined in the following files are working as expected.
 *
 * <ul>
 *   <li>{@code templates/db_scripts/aggregatorsettings_triggers.sql}
 *   <li>{@code templates/db_scripts/counterreports_triggers.sql}
 *   <li>{@code templates/db_scripts/usagedataproviders_triggers.sql}
 * </ul>
 */
@RunWith(VertxUnitRunner.class)
public class SQLTriggersIT {

  public static final String AGGREGATOR_TBL = "aggregator_settings";
  public static final String UDP_TABLE = "usage_data_providers";
  public static final String REPORTS_TBL = "counter_reports";
  private static final String AGGREGATOR_ID = "5ea343c7-5aac-4648-bb37-c4f72a6c2836";
  private static final String PROVIDER_ID = "af802f18-116d-4553-a5da-84db410c1eac";
  private static final String TENANT = "tenant";
  private static final Vertx vertx = Vertx.vertx();
  private static final AggregatorSetting AGGREGATOR =
      new AggregatorSetting()
          .withId(AGGREGATOR_ID)
          .withLabel("Aggregator")
          .withServiceType("serviceType")
          .withServiceUrl("http://localhost")
          .withAccountConfig(new AccountConfig().withConfigType(MANUAL));

  private static final UsageDataProvider PROVIDER =
      new UsageDataProvider()
          .withId(PROVIDER_ID)
          .withLabel("Test Provider")
          .withHarvestingConfig(
              new HarvestingConfig()
                  .withAggregator(new Aggregator().withId(AGGREGATOR_ID))
                  .withHarvestingStatus(INACTIVE));

  private final List<CounterReport> sampleReports =
      List.of(
          createReport("c068cdb3-6477-4307-922b-55c680db5262", "2020-01", "4", "JR1", null),
          createReport("36ed3988-da14-48ae-a5df-9b83ad7f0ee7", "2020-02", "5", "TR", "Exception"),
          createReport(
                  "e3fb2da9-bf3a-4b49-b5a9-409457ac3637",
                  "2020-03",
                  "5.1",
                  "IR",
                  "\"Code\": " + "3003")
              .withFailedAttempts(1));

  private final List<CounterReport> updatedReports =
      List.of(
          createReport("c068cdb3-6477-4307-922b-55c680db5262", "2019-12", "4", "JR1", null),
          createReport("36ed3988-da14-48ae-a5df-9b83ad7f0ee7", "2020-02", "5", "TR", null),
          createReport("e3fb2da9-bf3a-4b49-b5a9-409457ac3637", "2020-03", "5", "TR", null));

  private static PostgresClient pgClient;

  @SuppressWarnings("unchecked")
  private <T> T deepClone(T o) {
    return Json.decodeValue(Json.encode(o), (Class<T>) o.getClass());
  }

  private Future<UsageDataProvider> getTestProvider() {
    return pgClient.getById(UDP_TABLE, PROVIDER_ID, UsageDataProvider.class);
  }

  private CounterReport createReport(
      String id, String yearMonth, String release, String reportName, String failedReason) {
    return new CounterReport()
        .withId(id)
        .withDownloadTime(Date.from(Instant.now()))
        .withRelease(release)
        .withReportName(reportName)
        .withFailedReason(failedReason)
        .withYearMonth(yearMonth)
        .withProviderId(PROVIDER_ID);
  }

  private String getIdFromEntity(Object entity) {
    JsonObject jsonEntity = JsonObject.mapFrom(entity);
    return jsonEntity.getString("id");
  }

  private Future<Long> getRowCount(String table) {
    return pgClient
        .execute("SELECT COUNT(*) FROM " + table)
        .map(rs -> rs.iterator().next().getLong(0));
  }

  private CompositeFuture insertEntity(String table, Object entity) {
    return insertEntities(table, List.of(entity));
  }

  private CompositeFuture insertEntities(String table, List<?> entities) {
    return Future.all(
        entities.stream()
            .unordered()
            .parallel()
            .map(entity -> pgClient.save(table, getIdFromEntity(entity), entity))
            .toList());
  }

  private CompositeFuture updateEntity(String table, Object entity) {
    return updateEntities(table, List.of(entity));
  }

  private CompositeFuture updateEntities(String table, List<?> entities) {
    return Future.all(
        entities.stream()
            .unordered()
            .parallel()
            .map(entity -> pgClient.update(table, entity, getIdFromEntity(entity)))
            .toList());
  }

  private CompositeFuture deleteEntity(String table, Object entity) {
    return deleteEntities(table, List.of(entity));
  }

  private CompositeFuture deleteEntities(String table, List<?> entities) {
    return Future.all(
        entities.stream()
            .unordered()
            .parallel()
            .map(entity -> pgClient.delete(table, getIdFromEntity(entity)))
            .toList());
  }

  @ClassRule
  public static PostgresContainerRule postgresContainerRule =
      new PostgresContainerRule(vertx, TENANT);

  @BeforeClass
  public static void beforeClass() {
    pgClient = PostgresClient.getInstance(vertx, TENANT);
  }

  @Before
  public void setUp(TestContext context) {
    Future.all(
            pgClient.delete(UDP_TABLE, new Criterion()),
            pgClient.delete(AGGREGATOR_TBL, new Criterion()))
        .onComplete(context.asyncAssertSuccess());
  }

  @Test
  public void testStatisticsUpdateOnCounterReportInsert(TestContext context) {
    succeededFuture()
        .compose(v -> insertEntity(UDP_TABLE, PROVIDER))
        .compose(v -> insertEntities(REPORTS_TBL, sampleReports))
        .compose(v -> getTestProvider())
        .onComplete(
            context.asyncAssertSuccess(
                udp -> {
                  assertThat(udp.getEarliestReport()).isEqualTo("2020-01");
                  assertThat(udp.getLatestReport()).isEqualTo("2020-02");
                  assertThat(udp.getHasFailedReport()).isEqualTo(YES);
                  assertThat(udp.getReportErrorCodes()).containsExactly("3003", "other");
                  assertThat(udp.getReportTypes()).containsExactly("IR", "JR1", "TR");
                  assertThat(udp.getReportReleases()).containsExactly("4", "5", "5.1");
                }));
  }

  @Test
  public void testStatisticsUpdateOnCounterReportUpdate(TestContext context) {
    succeededFuture()
        .compose(v -> insertEntity(UDP_TABLE, PROVIDER))
        .compose(v -> insertEntities(REPORTS_TBL, sampleReports))
        .compose(v -> updateEntities(REPORTS_TBL, updatedReports))
        .compose(cf -> getTestProvider())
        .onComplete(
            context.asyncAssertSuccess(
                udp -> {
                  assertThat(udp.getEarliestReport()).isEqualTo("2019-12");
                  assertThat(udp.getLatestReport()).isEqualTo("2020-03");
                  assertThat(udp.getHasFailedReport()).isEqualTo(NO);
                  assertThat(udp.getReportErrorCodes()).isEmpty();
                  assertThat(udp.getReportTypes()).containsExactly("JR1", "TR");
                  assertThat(udp.getReportReleases()).containsExactly("4", "5");
                }));
  }

  @Test
  public void testStatisticsUpdateOnCounterReportDelete(TestContext context) {
    succeededFuture()
        .compose(v -> insertEntity(UDP_TABLE, PROVIDER))
        .compose(v -> insertEntities(REPORTS_TBL, sampleReports))
        .compose(v -> deleteEntities(REPORTS_TBL, sampleReports))
        .compose(cf -> getTestProvider())
        .onComplete(
            context.asyncAssertSuccess(
                udp -> {
                  assertThat(udp.getEarliestReport()).isNull();
                  assertThat(udp.getLatestReport()).isNull();
                  assertThat(udp.getHasFailedReport()).isEqualTo(NO);
                  assertThat(udp.getReportErrorCodes()).isEmpty();
                  assertThat(udp.getReportTypes()).isEmpty();
                  assertThat(udp.getReportReleases()).isEmpty();
                }));
  }

  @Test
  public void testAggregatorNameUpdatesOnAggregatorUpdate(TestContext context) {
    AggregatorSetting updatedAggregator = deepClone(AGGREGATOR).withLabel("new label");
    succeededFuture()
        .compose(v -> insertEntity(AGGREGATOR_TBL, AGGREGATOR))
        .compose(v -> insertEntity(UDP_TABLE, PROVIDER))
        .compose(v -> updateEntity(AGGREGATOR_TBL, updatedAggregator))
        .compose(v -> getTestProvider())
        .onComplete(
            context.asyncAssertSuccess(
                udp ->
                    assertThat(udp.getHarvestingConfig().getAggregator().getName())
                        .isEqualTo(updatedAggregator.getLabel())));
  }

  @Test
  public void testAggregatorNameUpdatesOnUDPInsert(TestContext context) {
    succeededFuture()
        .compose(v -> insertEntity(AGGREGATOR_TBL, AGGREGATOR))
        .compose(v -> insertEntity(UDP_TABLE, PROVIDER))
        .compose(v -> getTestProvider())
        .onComplete(
            context.asyncAssertSuccess(
                udp ->
                    assertThat(udp.getHarvestingConfig().getAggregator().getName())
                        .isEqualTo(AGGREGATOR.getLabel())));
  }

  @Test
  public void testAggregatorNameUpdatesOnUDPUpdate(TestContext context) {
    AggregatorSetting aggregator2 =
        deepClone(AGGREGATOR)
            .withId("862b89ec-0783-484c-9eb0-705804721b3e")
            .withLabel("Another Aggregator");
    UsageDataProvider updatedProvider = deepClone(PROVIDER);
    updatedProvider.getHarvestingConfig().getAggregator().setId(aggregator2.getId());

    succeededFuture()
        .compose(v -> insertEntities(AGGREGATOR_TBL, List.of(AGGREGATOR, aggregator2)))
        .compose(v -> insertEntity(UDP_TABLE, PROVIDER))
        .compose(v -> updateEntity(UDP_TABLE, updatedProvider))
        .compose(v -> getTestProvider())
        .onComplete(
            context.asyncAssertSuccess(
                udp ->
                    assertThat(udp.getHarvestingConfig().getAggregator().getName())
                        .isEqualTo(aggregator2.getLabel())));
  }

  @Test
  public void testReportsAreDeletedOnUDPDelete(TestContext context) {
    succeededFuture()
        .compose(v -> insertEntity(UDP_TABLE, PROVIDER))
        .compose(v -> insertEntities(REPORTS_TBL, sampleReports))
        .compose(v -> getRowCount(REPORTS_TBL))
        .compose(
            count -> {
              assertThat(count).isEqualTo(sampleReports.size());
              return succeededFuture();
            })
        .compose(v -> deleteEntity(UDP_TABLE, PROVIDER))
        .compose(v -> getRowCount(REPORTS_TBL))
        .onComplete(context.asyncAssertSuccess(count -> assertThat(count).isZero()));
  }
}
