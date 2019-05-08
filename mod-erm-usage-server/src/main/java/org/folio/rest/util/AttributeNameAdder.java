package org.folio.rest.util;

import io.vertx.core.Context;
import io.vertx.core.Future;
import java.util.Map;
import java.util.Objects;
import org.folio.rest.jaxrs.model.UsageDataProvider;
import org.folio.rest.util.nameresolver.AggregatorNameResolver;

public class AttributeNameAdder {

  private AttributeNameAdder() {
    throw new IllegalStateException("Utility class.");
  }

  public static Future<UsageDataProvider> resolveAndAddAttributeNames(
      UsageDataProvider udp, Map<String, String> okapiHeaders, Context vertxContext) {
    String aggregatorId = getAggregatorId(udp);
    if (Objects.isNull(aggregatorId)) return Future.succeededFuture(udp);

    Future<String> aggregatorNameFuture =
        AggregatorNameResolver.resolveName(aggregatorId, okapiHeaders, vertxContext);
    return aggregatorNameFuture.map(
        name -> {
          if (Objects.nonNull(name)) udp.getHarvestingConfig().getAggregator().setName(name);
          return udp;
        });
  }

  private static String getAggregatorId(UsageDataProvider udp) {
    if (udp.getHarvestingConfig() != null && udp.getHarvestingConfig().getAggregator() != null) {
      return udp.getHarvestingConfig().getAggregator().getId();
    }
    return null;
  }
}
