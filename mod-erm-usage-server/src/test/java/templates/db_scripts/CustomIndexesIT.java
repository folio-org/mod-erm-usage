package templates.db_scripts;

import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.util.stream.Stream;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.util.PostgresContainerRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class CustomIndexesIT {

  private static final String[] TENANTS = {"aTenant", "bTenant"};
  private static final String[] INDEXES = {
    "counter_reports_custom_getcsv_idx",
    "counter_reports_custom_errorcodes_idx",
    "counter_reports_custom_reporttypes_idx",
    "counter_reports_custom_reportreleases_idx",
    "usage_data_providers_custom_aggregatorid_idx"
  };

  private static Vertx vertx = Vertx.vertx();

  @ClassRule
  public static PostgresContainerRule postgresRule = new PostgresContainerRule(vertx, TENANTS);

  @Rule public Timeout timeout = Timeout.seconds(5);

  @Test
  public void testThatCustomIndexesArePresentForEachTenant(TestContext context) {
    Async async = context.async(INDEXES.length);
    PostgresClient postgresClient = PostgresClient.getInstance(vertx);
    Stream.of(INDEXES)
        .forEach(
            idxName ->
                postgresClient.select(
                    "SELECT COUNT(*) FROM pg_class WHERE relname = '" + idxName + "';",
                    ar -> {
                      if (ar.succeeded()) {
                        if (ar.result()
                            .iterator()
                            .next()
                            .getInteger("count")
                            .equals(TENANTS.length)) {
                          async.countDown();
                        } else {
                          context.fail(String.format("Index %s not found.", idxName));
                        }
                      } else {
                        context.fail(ar.cause());
                      }
                    }));
  }
}
