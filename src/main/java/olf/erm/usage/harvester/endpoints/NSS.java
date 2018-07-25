package olf.erm.usage.harvester.endpoints;

import org.folio.rest.jaxrs.model.AggregatorSetting;
import org.folio.rest.jaxrs.model.UsageDataProvider;

public class NSS implements ServiceEndpoint {

  private UsageDataProvider provider;
  private AggregatorSetting aggregator;

  @Override
  public String buildURL(String report, String begin, String end) {
    String url = "%s?APIKey=%s&RequestorID="
        + "%s&CustomerID=%s&Report=%s&Release=%s&BeginDate=%s&EndDate=%s&Platform=%s&Format=xml";
    String serviceUrl =
        (aggregator == null) ? provider.getServiceUrl() : aggregator.getServiceUrl();
    String apiKey = (aggregator == null) ? provider.getApiKey() : aggregator.getApiKey();
    return String.format(url, serviceUrl, apiKey, provider.getRequestorId(),
        provider.getCustomerId(), report, provider.getReportRelease(), begin, end,
        provider.getAggregator().getVendorCode());
  }

  public NSS(UsageDataProvider provider, AggregatorSetting aggregator) {
    this.provider = provider;
    this.aggregator = aggregator;
  }
}
