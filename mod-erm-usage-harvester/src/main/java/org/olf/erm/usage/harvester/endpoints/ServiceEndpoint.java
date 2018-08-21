package org.olf.erm.usage.harvester.endpoints;

import org.apache.log4j.Logger;
import org.folio.rest.jaxrs.model.AggregatorSetting;
import org.folio.rest.jaxrs.model.UsageDataProvider;
import io.vertx.core.Future;
import io.vertx.core.Vertx;


public interface ServiceEndpoint {

  String buildURL(String report, String begin, String end);

  boolean isValidReport(String report);

  Future<String> fetchSingleReport(String report, String beginDate, String endDate);

  public static ServiceEndpoint create(Vertx vertx, UsageDataProvider provider,
      AggregatorSetting aggregator) {
    final Logger LOG = Logger.getLogger(ServiceEndpoint.class);

    String serviceType = "";
    String name = "";
    if (aggregator == null) {
      serviceType = provider.getServiceType();
      name = provider.getLabel();
    } else {
      serviceType = aggregator.getServiceType();
      name = aggregator.getLabel();
    }

    switch (serviceType) {
      case "NSS":
        return new NSS(vertx, provider, aggregator);
      default:
        LOG.error("No implementation found for serviceType '" + serviceType + "'. (" + name + ")");
        return null;
    }
  }
}
