package org.folio.rest.util.nameresolver;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.WebClient;
import java.util.Map;
import org.apache.log4j.Logger;

public class AggregatorNameResolver {

  private static final Logger LOG = Logger.getLogger(VendorNameResolver.class);

  private static final String AGGREGATOR_ENDPOINT = "/aggregator-settings/";

  private AggregatorNameResolver() {
    throw new IllegalStateException("Utility class");
  }

  public static Future<String> resolveName(
      String aggregatorId, String okapiUrl, Map<String, String> okapiHeaders, Vertx vertx) {
    Future<String> future = Future.future();

    if (aggregatorId == null) {
      future.complete();
      return future;
    }

    String endpoint = AGGREGATOR_ENDPOINT + aggregatorId;
    WebClient webClient = WebClient.create(vertx);
    String url = okapiUrl + endpoint;
    HttpRequest<Buffer> request = webClient.getAbs(url);

    okapiHeaders.forEach(request::putHeader);
    request.putHeader("accept", "application/json");

    request.send(
        ar -> {
          if (ar.succeeded()) {
            if (ar.result().statusCode() == 200) {
              JsonObject vendorJson = ar.result().bodyAsJsonObject();
              String aggregatorName = vendorJson.getString("label");
              LOG.info("Found aggregator name " + aggregatorName + " for id " + aggregatorId);
              future.complete(aggregatorName);
            } else {
              future.fail(
                  "Got status code != 200 when fetching aggregator. Got code: "
                      + ar.result().statusCode()
                      + ". Maybe aggregator id is not correct?");
            }
          } else {
            future.fail(ar.cause());
          }
        });
    return future;
  }
}
