package templates.db_scripts;

import static org.assertj.core.api.Assertions.assertThat;
import static org.folio.rest.util.Constants.TABLE_NAME_UDP;

import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.folio.cql2pgjson.CQL2PgJSON;
import org.folio.rest.PostgresExtension;
import org.folio.rest.TestUtils;
import org.folio.rest.impl.TenantAPI;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.cql.CQLWrapper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Checks that the search and the filters of the ui-erm-usage UDP list and the label search of
 * ui-plugin-find-erm-usage-data-provider can use an index.
 */
@ExtendWith(PostgresExtension.class)
class UsageDataProvidersIndexesIT {

  private static final String TENANT = "udpindexes";
  private static final String SCHEMA = PostgresClient.convertToPsqlStandard(TENANT);

  private static PostgresClient pgClient;

  @BeforeAll
  static void beforeAll() throws Exception {
    pgClient = PostgresClient.getInstance(TestUtils.getVertx());
    pgClient
        .runSqlFile(String.join("\n", new TenantAPI().sqlFile(TENANT, false, null, null, null)))
        .await();
  }

  // PostgreSQL truncates index names to 63 characters, see the service type index
  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      textBlock =
          """
          keywords all "springer*"                        | usage_data_providers_keywords_idx_ft
          label="springer*"                               | usage_data_providers_label_idx_ft
          status="active"                                 | usage_data_providers_status_idx_ft
          harvestingConfig.harvestingStatus="active"      | usage_data_providers_harvestingconfig_harvestingstatus_idx_ft
          hasFailedReport="yes"                           | usage_data_providers_hasfailedreport_idx_ft
          tags.tagList="urgent"                           | usage_data_providers_tags_taglist_idx_ft
          reportErrorCodes="3030"                         | usage_data_providers_reporterrorcodes_idx_ft
          reportTypes="TR_J1"                             | usage_data_providers_reporttypes_idx_ft
          reportReleases="5.1"                            | usage_data_providers_reportreleases_idx_ft
          harvestingConfig.sushiConfig.serviceType="cs50" | usage_data_providers_harvestingconfig_sushiconfig_servicetype_i
          """)
  void testThatQueryUsesIndex(String cql, String indexName) throws Exception {
    String where = new CQLWrapper(new CQL2PgJSON(TABLE_NAME_UDP + ".jsonb"), cql).getWhereClause();
    RowSet<Row> rows =
        pgClient
            .withTrans(
                conn ->
                    conn.execute("SET LOCAL search_path TO " + SCHEMA)
                        // the table is empty, the planner would always prefer a sequential scan
                        .compose(v -> conn.execute("SET LOCAL enable_seqscan TO off"))
                        .compose(
                            v ->
                                conn.execute(
                                    "EXPLAIN SELECT id FROM " + TABLE_NAME_UDP + " " + where)))
            .await();
    String plan =
        StreamSupport.stream(rows.spliterator(), false)
            .map(row -> row.getString(0))
            .collect(Collectors.joining("\n"));
    assertThat(plan).contains(" on " + indexName + " ");
  }
}
