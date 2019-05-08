package org.folio.rest.util.nameresolver;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.WebClient;
import java.util.Map;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.log4j.Logger;
import org.folio.okapi.common.XOkapiHeaders;

public class AggregatorNameResolver {

  private static final Logger LOG = Logger.getLogger(AggregatorNameResolver.class);

  private static final String AGGREGATOR_ENDPOINT = "/aggregator-settings/";

  private AggregatorNameResolver() {
    throw new IllegalStateException("Utility class");
  }

  public static Future<String> resolveName(
      String aggregatorId, Map<String, String> okapiHeaders, Context vertxContext) {
    Future<String> future = Future.future();

    if (aggregatorId == null) {
      return Future.succeededFuture();
    }

    String endpoint = AGGREGATOR_ENDPOINT + aggregatorId;
    WebClient webClient = WebClient.create(vertxContext.owner());
    String url =
        ObjectUtils.firstNonNull(
                okapiHeaders.get(XOkapiHeaders.URL_TO), okapiHeaders.get(XOkapiHeaders.URL))
            + endpoint;
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
                  String.format(
                      "%s %s: %s",
                      ar.result().statusCode(),
                      ar.result().statusMessage(),
                      ar.result().bodyAsString()));
            }
          } else {
            future.fail(ar.cause());
          }
        });
    return future;
  }
}
