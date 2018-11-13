package org.olf.erm.usage.harvester.endpoints;

import org.folio.rest.jaxrs.model.AggregatorSetting;
import org.folio.rest.jaxrs.model.UsageDataProvider;
import org.olf.erm.usage.harvester.endpoints.ServiceEndpoint;
import io.vertx.core.Future;

public class CS41Impl implements ServiceEndpoint {

  private UsageDataProvider provider;
  private AggregatorSetting aggregator;

  public CS41Impl(UsageDataProvider provider, AggregatorSetting aggregator) {
    this.provider = provider;
    this.aggregator = aggregator;
  }

  @Override
  public boolean isValidReport(String report) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public Future<String> fetchSingleReport(String report, String beginDate, String endDate) {
    // TODO Auto-generated method stub
    return null;
  }

}
