package olf.erm.usage.harvester.endpoints;

import org.folio.rest.jaxrs.model.AggregatorSetting;
import org.folio.rest.jaxrs.model.UsageDataProvider;
import io.vertx.core.Vertx;

public class NSS extends AbstractServiceEndpoint {

  public NSS(Vertx vertx, UsageDataProvider provider, AggregatorSetting aggregator) {
    super(vertx, provider, aggregator);
  }

  @Override
  public String buildURL(String report, String begin, String end) {
    String url = "%s?APIKey=%s&RequestorID="
        + "%s&CustomerID=%s&Report=%s&Release=%s&BeginDate=%s&EndDate=%s&Platform=%s&Format=xml";
    String serviceUrl =
        (aggregator == null) ? provider.getServiceUrl() : aggregator.getServiceUrl();
    String apiKey = (aggregator == null) ? provider.getApiKey() : aggregator.getApiKey();
    String vendorCode = (aggregator == null) ? "" : provider.getAggregator().getVendorCode();
    return String.format(url, serviceUrl, apiKey, provider.getRequestorId(),
        provider.getCustomerId(), report, provider.getReportRelease(), begin, end, vendorCode);
  }

  @Override
  public boolean isValidReport(String report) {
    return !(report == null || report.contains("<s:Exception>"));
  }

}
