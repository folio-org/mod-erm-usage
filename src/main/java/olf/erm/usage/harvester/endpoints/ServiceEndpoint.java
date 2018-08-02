package olf.erm.usage.harvester.endpoints;

import org.apache.log4j.Logger;
import org.folio.rest.jaxrs.model.AggregatorSetting;
import org.folio.rest.jaxrs.model.UsageDataProvider;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.client.WebClient;


public interface ServiceEndpoint {

  String buildURL(String report, String begin, String end);

  boolean isValidReport(String report);

  default Future<String> fetchSingleReport(String report, String beginDate, String endDate) {
    final String url = buildURL(report, beginDate, endDate);

    Future<String> future = Future.future();
    WebClient client = WebClient.create(Vertx.vertx());
    client.requestAbs(HttpMethod.GET, url).send(ar -> {
      if (ar.succeeded()) {
        client.close();
        if (ar.result().statusCode() == 200) {
          String result = ar.result().bodyAsString();
          if (isValidReport(result))
            future.complete(result);
          else
            future.fail("Report not valid");
        } else {
          future.fail(url + " - " + ar.result().statusCode() + " : " + ar.result().statusMessage());
        }
      } else {
        future.fail(ar.cause());
      }
    });
    return future;
  }

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
