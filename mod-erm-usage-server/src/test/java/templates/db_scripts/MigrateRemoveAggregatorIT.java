package templates.db_scripts;

import static org.assertj.core.api.Assertions.assertThat;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.folio.rest.impl.TenantAPI;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.util.PostgresContainerRule;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

/** Tests {@code migration/6.0.0/migrate_remove_aggregator.sql} on an upgrade from 5.2.0. */
@RunWith(VertxUnitRunner.class)
public class MigrateRemoveAggregatorIT {

  private static final String TENANT = "migration";
  private static final String SCHEMA = PostgresClient.convertToPsqlStandard(TENANT);
  private static final Vertx vertx = Vertx.vertx();

  @ClassRule
  public static PostgresContainerRule postgresContainerRule =
      new PostgresContainerRule(vertx, TENANT);

  private static PostgresClient pgClient;
  private static JsonObject sushiUdp;
  private static JsonObject aggregatorUdp;

  @BeforeClass
  public static void beforeClass(TestContext context) throws Exception {
    pgClient = PostgresClient.getInstance(vertx);
    sushiUdp = loadSample("udproviders.sample");
    aggregatorUdp = loadSample("udproviders2.sample");

    // UDPs as stored by mod-erm-usage 5.2.x
    JsonObject legacySushiUdp = sushiUdp.copy();
    legacySushiUdp.getJsonObject("harvestingConfig").put("harvestVia", "sushi");
    JsonObject legacyAggregatorUdp = aggregatorUdp.copy();
    legacyAggregatorUdp
        .getJsonObject("harvestingConfig")
        .put("harvestVia", "aggregator")
        .put("aggregator", new JsonObject().put("id", "5ea343c7-5aac-4648-bb37-c4f72a6c2836"));

    TenantAttributes upgrade =
        new TenantAttributes()
            .withModuleFrom("mod-erm-usage-5.2.0")
            .withModuleTo("mod-erm-usage-6.0.0");
    String[] upgradeSql =
        new TenantAPI().sqlFile(TENANT, true, upgrade, "mod-erm-usage-5.2.0", null);

    pgClient
        .execute(
            "CREATE TABLE %s.aggregator_settings (id UUID PRIMARY KEY, jsonb JSONB NOT NULL)"
                .formatted(SCHEMA))
        .compose(v -> insertUdp(legacySushiUdp))
        .compose(v -> insertUdp(legacyAggregatorUdp))
        .compose(v -> pgClient.runSqlFile(String.join("\n", upgradeSql)))
        .onComplete(context.asyncAssertSuccess());
  }

  private static JsonObject loadSample(String name) throws IOException {
    return new JsonObject(Files.readString(Path.of("../ramls/examples", name)));
  }

  private static Future<Void> insertUdp(JsonObject udp) {
    return pgClient
        .execute(
            "INSERT INTO %s.usage_data_providers (id, jsonb) VALUES ('%s', '%s')"
                .formatted(SCHEMA, udp.getString("id"), udp.encode().replace("'", "''")))
        .mapEmpty();
  }

  private Future<JsonObject> getHarvestingConfig(JsonObject udp) {
    return pgClient
        .selectSingle(
            "SELECT jsonb->'harvestingConfig' FROM %s.usage_data_providers WHERE id = '%s'"
                .formatted(SCHEMA, udp.getString("id")))
        .map(row -> row.getJsonObject(0));
  }

  @Test
  public void testAggregatorUdpIsDeactivated(TestContext context) {
    JsonObject expected =
        aggregatorUdp.getJsonObject("harvestingConfig").copy().put("harvestingStatus", "inactive");
    getHarvestingConfig(aggregatorUdp)
        .onComplete(context.asyncAssertSuccess(hc -> assertThat(hc).isEqualTo(expected)));
  }

  @Test
  public void testSushiUdpStaysActive(TestContext context) {
    getHarvestingConfig(sushiUdp)
        .onComplete(
            context.asyncAssertSuccess(
                hc -> assertThat(hc).isEqualTo(sushiUdp.getJsonObject("harvestingConfig"))));
  }

  @Test
  public void testAggregatorSettingsTableIsDropped(TestContext context) {
    pgClient
        .selectSingle("SELECT to_regclass('%s.aggregator_settings')".formatted(SCHEMA))
        .onComplete(context.asyncAssertSuccess(row -> assertThat(row.getValue(0)).isNull()));
  }
}
