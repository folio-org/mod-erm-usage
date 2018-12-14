package org.folio.rest.util.nameresolver;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.WebClient;
import java.util.Map;
import org.apache.log4j.Logger;

public class VendorNameResolver {

  private static final Logger LOG = Logger.getLogger(VendorNameResolver.class);

  private static final String VENDOR_ENDPOINT = "/vendor/";

  public static Future<String> resolveName(String vendorId, String okapiUrl,
    Map<String, String> okapiHeaders, Vertx vertx) {
    Future<String> future = Future.future();

    if (vendorId == null) {
      future.complete();
      return future;
    }

    String endpoint = VENDOR_ENDPOINT + vendorId;
    WebClient webClient = WebClient.create(vertx);
    String url = okapiUrl + endpoint;
    HttpRequest<Buffer> request = webClient.getAbs(url);

    okapiHeaders.forEach(request::putHeader);
    request.putHeader("accept", "application/json");

    request.send(ar -> {
      if (ar.succeeded()) {
        if (ar.result().statusCode() == 200) {
          JsonObject vendorJson = ar.result().bodyAsJsonObject();
          String vendorName = vendorJson.getString("name");
          LOG.info("Found vendor name " + vendorName + " for id " + vendorId);
          future.complete(vendorName);
        } else {
          future
            .fail(
              "Got status code != 200 when fetching vendor. Got code: " + ar.result().statusCode()
                + ". May vendor id is not correct?");
        }
      } else {
        future.fail(ar.cause());
      }
    });
    return future;
  }

}
