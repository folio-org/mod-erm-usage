package templates.db_scripts;

import static org.assertj.core.api.Assertions.assertThat;

import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Tuple;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.stream.Stream;
import org.folio.rest.PostgresExtension;
import org.folio.rest.TestUtils;
import org.folio.rest.impl.TenantAPI;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.persist.PostgresClient;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests {@code migration/6.0.0/migrate_remove_aggregator.sql} and {@code
 * migration/6.0.0/migrate_cs41_harvesting_status.sql} on an upgrade from 5.2.0.
 */
@ExtendWith(PostgresExtension.class)
class MigrateRemoveAggregatorAndCS41HarvestingStatusIT {

  private static final String TENANT = "migration";
  private static final String SCHEMA = PostgresClient.convertToPsqlStandard(TENANT);

  /** Stand-ins for the aggregator database objects of 5.2.x, the drops only need their names. */
  private static final String LEGACY_OBJECTS =
      """
      CREATE TABLE %1$s.aggregator_settings (id UUID PRIMARY KEY, jsonb JSONB NOT NULL);
      CREATE INDEX usage_data_providers_custom_aggregatorid_idx ON %1$s.usage_data_providers ((jsonb->'harvestingConfig'->'aggregator'->>'id'));
      CREATE FUNCTION %1$s.resolve_aggregator_label() RETURNS trigger AS $$ BEGIN RETURN NEW; END $$ LANGUAGE plpgsql;
      CREATE FUNCTION %1$s.update_aggregator_label_references() RETURNS trigger AS $$ BEGIN RETURN NEW; END $$ LANGUAGE plpgsql;
      CREATE FUNCTION %1$s.aggregator_settings_set_md() RETURNS trigger AS $$ BEGIN RETURN NEW; END $$ LANGUAGE plpgsql;
      CREATE FUNCTION %1$s.set_aggregator_settings_md_json() RETURNS trigger AS $$ BEGIN RETURN NEW; END $$ LANGUAGE plpgsql;
      CREATE TRIGGER resolve_aggregator_label_before_insert BEFORE INSERT ON %1$s.usage_data_providers FOR EACH ROW EXECUTE PROCEDURE %1$s.resolve_aggregator_label();
      CREATE TRIGGER resolve_aggregator_label_before_update BEFORE UPDATE ON %1$s.usage_data_providers FOR EACH ROW EXECUTE PROCEDURE %1$s.resolve_aggregator_label();
      CREATE TRIGGER update_aggregator_label_references_after_update AFTER UPDATE ON %1$s.aggregator_settings FOR EACH ROW EXECUTE PROCEDURE %1$s.update_aggregator_label_references();
      """;

  /** Number of tables, indexes, functions and triggers with "aggregator" in their name. */
  private static final String COUNT_AGGREGATOR_OBJECTS =
      """
      SELECT (SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
              WHERE n.nspname = '%1$s' AND c.relname LIKE '%%aggregator%%')
           + (SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
              WHERE n.nspname = '%1$s' AND p.proname LIKE '%%aggregator%%')
           + (SELECT count(*) FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid
              JOIN pg_namespace n ON n.oid = c.relnamespace
              WHERE n.nspname = '%1$s' AND t.tgname LIKE '%%aggregator%%')
      """;

  private static final String AGGREGATOR_ID = "5ea343c7-5aac-4648-bb37-c4f72a6c2836";
  private static final String NO_DESCRIPTION_ID = "9c5e7f4b-1a6d-4c8e-8f0b-4d5e6f7a8b9c";
  private static final String INACTIVE_VENDOR_CODE_ID = "0d6f8a5c-2b7e-4d9f-9a1c-5e6f7a8b9c0d";
  private static final String CS41_ACTIVE_ID = "1e7a9b6d-3c8f-4e0a-8b2d-6f7a8b9c0d1e";
  private static final String CS41_INACTIVE_ID = "2f8b0c7e-4d9a-4f1b-9c3e-7a8b9c0d1e2f";

  private static PostgresClient pgClient;
  private static JsonObject sushiUdp;
  private static JsonObject aggregatorUdp;
  private static JsonObject inactiveAggregatorUdp;
  private static JsonObject cs41ActiveUdp;
  private static JsonObject cs41InactiveUdp;

  @BeforeAll
  static void beforeAll() throws Exception {
    pgClient = PostgresClient.getInstance(TestUtils.getVertx());
    sushiUdp = loadSample("udproviders.sample");
    cs41ActiveUdp = sushiUdp.copy().put("id", CS41_ACTIVE_ID);
    cs41InactiveUdp = sushiUdp.copy().put("id", CS41_INACTIVE_ID);
    cs41InactiveUdp.getJsonObject("harvestingConfig").put("harvestingStatus", "inactive");
    // the sample uses cs41, which would be deactivated by migrate_cs41_harvesting_status.sql
    sushiUdp
        .getJsonObject("harvestingConfig")
        .getJsonObject("sushiConfig")
        .put("serviceType", "cs50");
    aggregatorUdp = loadSample("udproviders2.sample");
    inactiveAggregatorUdp = aggregatorUdp.copy().put("id", "8b4d6e3a-0f5c-4b7d-9e9a-3c4d5e6f7a8b");
    inactiveAggregatorUdp.getJsonObject("harvestingConfig").put("harvestingStatus", "inactive");

    // UDPs as stored by mod-erm-usage 5.2.x
    JsonObject legacySushiUdp = sushiUdp.copy();
    legacySushiUdp
        .getJsonObject("harvestingConfig")
        .put("harvestVia", "sushi")
        .put("aggregator", new JsonObject().put("id", AGGREGATOR_ID));
    JsonObject legacyAggregatorUdp = legacyAggregatorUdp(aggregatorUdp, "ACMDL");
    JsonObject legacyInactiveAggregatorUdp = legacyAggregatorUdp(inactiveAggregatorUdp, null);
    JsonObject legacyNoDescriptionUdp =
        legacyAggregatorUdp(aggregatorUdp.copy().put("id", NO_DESCRIPTION_ID), "ACMDL");
    legacyNoDescriptionUdp.remove("description");
    JsonObject legacyInactiveVendorCodeUdp =
        legacyAggregatorUdp(
            inactiveAggregatorUdp.copy().put("id", INACTIVE_VENDOR_CODE_ID), "ACMDL");

    runSqlFile(new TenantAPI().sqlFile(TENANT, false, null, null, null));
    runSqlFile(LEGACY_OBJECTS.formatted(SCHEMA));
    Stream.of(
            legacySushiUdp,
            legacyAggregatorUdp,
            legacyInactiveAggregatorUdp,
            legacyNoDescriptionUdp,
            legacyInactiveVendorCodeUdp,
            cs41ActiveUdp,
            cs41InactiveUdp)
        .forEach(MigrateRemoveAggregatorAndCS41HarvestingStatusIT::insertUdp);

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

  private static JsonObject legacyAggregatorUdp(JsonObject udp, String vendorCode) {
    JsonObject legacyUdp = udp.copy();
    legacyUdp
        .getJsonObject("harvestingConfig")
        .put("harvestVia", "aggregator")
        .put("aggregator", new JsonObject().put("id", AGGREGATOR_ID).put("vendorCode", vendorCode));
    return legacyUdp;
  }

  private static void insertUdp(JsonObject udp) {
    pgClient
        .execute(
            "INSERT INTO %s.usage_data_providers (id, jsonb) VALUES ($1, $2)".formatted(SCHEMA),
            Tuple.of(UUID.fromString(udp.getString("id")), udp))
        .await();
  }

  private static JsonObject getUdp(String id) {
    return pgClient
        .selectSingle(
            "SELECT jsonb FROM %s.usage_data_providers WHERE id = '%s'".formatted(SCHEMA, id))
        .await()
        .getJsonObject(0);
  }

  @Test
  void testAggregatorUdpIsDeactivated() {
    JsonObject udp = getUdp(aggregatorUdp.getString("id"));
    assertThat(udp.getJsonObject("harvestingConfig"))
        .isEqualTo(
            aggregatorUdp
                .getJsonObject("harvestingConfig")
                .copy()
                .put("harvestingStatus", "inactive"));
    assertThat(udp.getString("description"))
        .isEqualTo(
            """
            See meeting notes 2023-10-05

            --- Umbrellaleaf upgrade ---
            Harvesting deactivated: harvesting via aggregator is no longer supported.
            Aggregator vendor code: ACMDL\
            """);
  }

  @Test
  void testInactiveAggregatorUdpWithoutVendorCodeKeepsDescription() {
    JsonObject udp = getUdp(inactiveAggregatorUdp.getString("id"));
    assertThat(udp.getJsonObject("harvestingConfig"))
        .isEqualTo(inactiveAggregatorUdp.getJsonObject("harvestingConfig"));
    assertThat(udp.getString("description"))
        .isEqualTo(inactiveAggregatorUdp.getString("description"));
  }

  @Test
  void testDescriptionIsSetWithoutBlankLineIfEmpty() {
    assertThat(getUdp(NO_DESCRIPTION_ID).getString("description"))
        .isEqualTo(
            """
            --- Umbrellaleaf upgrade ---
            Harvesting deactivated: harvesting via aggregator is no longer supported.
            Aggregator vendor code: ACMDL\
            """);
  }

  @Test
  void testDescriptionOfInactiveUdpHasOnlyVendorCode() {
    assertThat(getUdp(INACTIVE_VENDOR_CODE_ID).getString("description"))
        .isEqualTo(
            """
            See meeting notes 2023-10-05

            --- Umbrellaleaf upgrade ---
            Aggregator vendor code: ACMDL\
            """);
  }

  @Test
  void testSushiUdpStaysActive() {
    assertThat(getUdp(sushiUdp.getString("id")).getJsonObject("harvestingConfig"))
        .isEqualTo(sushiUdp.getJsonObject("harvestingConfig"));
  }

  @Test
  void testAggregatorObjectsAreDropped() {
    assertThat(pgClient.selectSingle(COUNT_AGGREGATOR_OBJECTS.formatted(SCHEMA)).await().getLong(0))
        .isZero();
  }

  @Test
  void testActiveCs41UdpIsDeactivated() {
    JsonObject udp = getUdp(CS41_ACTIVE_ID);
    assertThat(udp.getJsonObject("harvestingConfig"))
        .isEqualTo(
            cs41ActiveUdp
                .getJsonObject("harvestingConfig")
                .copy()
                .put("harvestingStatus", "inactive"));
    assertThat(udp.getString("description")).isEqualTo(cs41ActiveUdp.getString("description"));
  }

  @Test
  void testInactiveCs41UdpIsUnchanged() {
    assertThat(getUdp(CS41_INACTIVE_ID)).isEqualTo(cs41InactiveUdp);
  }
}
