package org.folio.rest.util;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import java.util.Map;
import org.apache.commons.lang3.ObjectUtils;
import org.folio.okapi.common.XOkapiHeaders;
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
    String okapiUrl = okapiHeaders.get(XOkapiHeaders.URL);

    String vendorId = udp.getVendor().getId();
    Future<String> vendorNameFuture =
        VendorNameResolver.resolveName(vendorId, okapiUrl, okapiHeaders, vertx);

    String aggregatorId = getAggregatorId(udp);
    Future<String> aggregatorNameFuture =
        AggregatorNameResolver.resolveName(
            aggregatorId,
            ObjectUtils.firstNonNull(
                okapiHeaders.get(XOkapiHeaders.URL_TO),
                okapiUrl), // FIXME: workaround for loading sample-data
            okapiHeaders,
            vertx);

    return CompositeFuture.all(vendorNameFuture, aggregatorNameFuture)
        .map(
            cf -> {
              setVendorName(udp, cf.resultAt(0));
              setAggregatorName(udp, cf.resultAt(1));
              return udp;
            });
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
