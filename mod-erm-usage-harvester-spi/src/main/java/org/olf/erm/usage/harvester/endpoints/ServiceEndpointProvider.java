package org.olf.erm.usage.harvester.endpoints;

import org.folio.rest.jaxrs.model.AggregatorSetting;
import org.folio.rest.jaxrs.model.UsageDataProvider;

public interface ServiceEndpointProvider {

  String getServiceType();

  ServiceEndpoint create(UsageDataProvider provider, AggregatorSetting aggregator);
}
