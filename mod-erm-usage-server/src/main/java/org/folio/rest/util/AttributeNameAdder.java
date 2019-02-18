package org.folio.rest.util;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import java.util.Map;
import org.folio.rest.jaxrs.model.Aggregator;
import org.folio.rest.jaxrs.model.UsageDataProvider;
import org.folio.rest.jaxrs.model.Vendor;
import org.folio.rest.util.nameresolver.AggregatorNameResolver;
import org.folio.rest.util.nameresolver.VendorNameResolver;

public class AttributeNameAdder {

  private AttributeNameAdder() {
    throw new IllegalStateException("Utility class.");
  }

  public static Future<UsageDataProvider> resolveAndAddAttributeNames(
      UsageDataProvider udp, Map<String, String> okapiHeaders, Vertx vertx) {
    String okapiUrl = okapiHeaders.get("x-okapi-url");

    String vendorId = udp.getVendor().getId();
    Future<String> vendorNameFuture =
        VendorNameResolver.resolveName(vendorId, okapiUrl, okapiHeaders, vertx);

    String aggregatorId = getAggregatorId(udp);
    Future<String> aggregatorNameFuture =
        AggregatorNameResolver.resolveName(aggregatorId, okapiUrl, okapiHeaders, vertx);

    Future<UsageDataProvider> future = Future.future();
    CompositeFuture.all(vendorNameFuture, aggregatorNameFuture)
        .setHandler(
            ar -> {
              if (ar.succeeded()) {
                if (ar instanceof CompositeFuture) {
                  CompositeFuture cf = (CompositeFuture) ar;
                  String vendorName = cf.resultAt(0);
                  String aggName = cf.resultAt(1);

                  setVendorName(udp, vendorName);
                  setAggregatorName(udp, aggName);

                  future.complete(udp);
                } else {
                  future.fail("Error while adding names to udp. No composite future.");
                }
              } else {
                future.fail("Error while adding names to udp: " + ar.cause());
              }
            });
    return future;
  }

  private static String getAggregatorId(UsageDataProvider udp) {
    if (udp.getHarvestingConfig() != null && udp.getHarvestingConfig().getAggregator() != null) {
      return udp.getHarvestingConfig().getAggregator().getId();
    }
    return null;
  }

  private static void setVendorName(UsageDataProvider udp, String vendorName) {
    if (vendorName != null) {
      Vendor vendor = udp.getVendor();
      vendor.setName(vendorName);
      udp.setVendor(vendor);
    }
  }

  private static void setAggregatorName(UsageDataProvider udp, String aggName) {
    if (aggName != null) {
      Aggregator aggregator = udp.getHarvestingConfig().getAggregator();
      aggregator.setName(aggName);
      udp.getHarvestingConfig().setAggregator(aggregator);
    }
  }
}
