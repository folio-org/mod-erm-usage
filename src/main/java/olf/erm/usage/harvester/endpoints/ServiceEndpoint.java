package olf.erm.usage.harvester.endpoints;

import org.apache.log4j.Logger;
import org.folio.rest.jaxrs.model.AggregatorSetting;
import org.folio.rest.jaxrs.model.UsageDataProvider;


public interface ServiceEndpoint {

  String buildURL(String report, String begin, String end);

  UsageDataProvider getProvider();

  AggregatorSetting getAggregator();

  public static ServiceEndpoint create(UsageDataProvider provider, AggregatorSetting aggregator) {
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
        return new NSS(provider, aggregator);
      default:
        LOG.error("No implementation found for serviceType '" + serviceType + "'. (" + name + ")");
        return null;
    }
  }
}
