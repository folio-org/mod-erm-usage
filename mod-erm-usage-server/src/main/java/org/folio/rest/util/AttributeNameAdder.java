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

  public static Future<UsageDataProvider> resolveAndAddAttributeNames(UsageDataProvider udp,
    Map<String, String> okapiHeaders, Vertx vertx) {
    String okapiUrl = okapiHeaders.get("x-okapi-url");
    okapiUrl = "http://localhost:9130";

    String vendorId = udp.getVendor().getId();
    Future<String> vendorNameFuture = VendorNameResolver
      .resolveName(vendorId, okapiUrl, okapiHeaders,
        vertx);

    String aggregatorId = udp.getHarvestingConfig().getAggregator().getId();
    Future<String> aggregatorNameFuture = AggregatorNameResolver
      .resolveName(aggregatorId, okapiUrl, okapiHeaders, vertx);

    Future<UsageDataProvider> future = Future.future();

    CompositeFuture.all(vendorNameFuture, aggregatorNameFuture).setHandler(ar -> {
      if (ar.succeeded()) {
        if (ar instanceof CompositeFuture) {
          CompositeFuture cf = (CompositeFuture) ar;
          String vendorName = cf.resultAt(0);
          String aggName = cf.resultAt(1);

          Vendor vendor = udp.getVendor();
          vendor.setName(vendorName);
          udp.setVendor(vendor);

          Aggregator aggregator = udp.getHarvestingConfig().getAggregator();
          aggregator.setName(aggName);
          udp.getHarvestingConfig().setAggregator(aggregator);
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

}
