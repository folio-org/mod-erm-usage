package org.olf.erm.usage.harvester.endpoints;

import org.folio.rest.jaxrs.model.AggregatorSetting;
import org.folio.rest.jaxrs.model.UsageDataProvider;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.client.WebClient;

public abstract class AbstractServiceEndpoint implements ServiceEndpoint {

  protected Vertx vertx;
  protected UsageDataProvider provider;
  protected AggregatorSetting aggregator;

  @Override
  public Future<String> fetchSingleReport(String report, String beginDate, String endDate) {
    final String url = buildURL(report, beginDate, endDate);

    Future<String> future = Future.future();
    WebClient client = WebClient.create(vertx);
    client.requestAbs(HttpMethod.GET, url).send(ar -> {
      if (ar.succeeded()) {
        client.close();
        if (ar.result().statusCode() == 200) {
          String result = ar.result().bodyAsString();
          if (isValidReport(result))
            future.complete(result);
          else
            future.complete(null);
        } else {
          future.fail(url + " - " + ar.result().statusCode() + " : " + ar.result().statusMessage());
        }
      } else {
        future.fail(ar.cause());
      }
    });
    return future;
  }

  public AbstractServiceEndpoint(Vertx vertx, UsageDataProvider provider,
      AggregatorSetting aggregator) {
    this.vertx = vertx;
    this.provider = provider;
    this.aggregator = aggregator;
  }

}
