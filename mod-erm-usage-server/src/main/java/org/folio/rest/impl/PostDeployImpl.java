package org.folio.rest.impl;

import static io.vertx.core.Future.failedFuture;
import static io.vertx.core.Future.succeededFuture;
import static org.folio.rest.RestVerticle.OKAPI_HEADER_PREFIX;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.impl.VertxImpl;
import io.vertx.core.json.Json;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.impl.RouterImpl;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.ws.rs.core.Response;
import org.apache.commons.collections4.map.CaseInsensitiveMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.rest.jaxrs.model.ReportUploadError;
import org.folio.rest.jaxrs.resource.CounterReports.PostCounterReportsMultipartuploadProviderByIdResponse;
import org.folio.rest.resource.interfaces.PostDeployVerticle;
import org.folio.rest.util.ReportUploadErrorCode;
import org.folio.rest.util.ReportUploadErrorFactory;
import org.folio.rest.util.VertxUtil;

public class PostDeployImpl implements PostDeployVerticle {

  private static final Logger log = LogManager.getLogger();

  private static CaseInsensitiveMap<String, String> getOkapiHeadersFromRoutingContext(
      RoutingContext rctx) {
    return rctx.request().headers().entries().stream()
        .filter(e -> e.getKey().toLowerCase().startsWith(OKAPI_HEADER_PREFIX))
        .collect(
            Collectors.toMap(
                Map.Entry::getKey, Map.Entry::getValue, (v1, v2) -> v2, CaseInsensitiveMap::new));
  }

  @Override
  public void init(Vertx vertx, Context context, Handler<AsyncResult<Boolean>> resultHandler) {
    VertxImpl vertxImpl = (VertxImpl) vertx;
    int port = context.config().getInteger("http.port", 8081);
    List<HttpServer> httpServers =
        vertxImpl.sharedTcpServers().values().stream()
            .filter(s -> s.actualPort() == port)
            .map(VertxUtil::extractHttpServer)
            .toList();
    if (httpServers.size() != 1) {
      resultHandler.handle(failedFuture("Unable to find HTTP server"));
      return;
    }

    // Setup a custom route handler for multipart/form-data uploads.
    // Order 0 ensures this handler runs before RMB's generated handler, allowing us to intercept
    // the request and use Vert.x's native multipart handling. RMB's handler would otherwise try to
    // parse the request as JSON, which fails for multipart/form-data content. The handler parses
    // the multipart request and delegates to RMB's generated API method with the parsed
    // RoutingContext, effectively bypassing RMB's request parsing while reusing its business logic.
    RouterImpl router = (RouterImpl) httpServers.get(0).requestHandler();
    router
        .postWithRegex("/counter-reports/multipartupload/provider/([^/]+)/?")
        .order(0)
        .handler(
            rctx -> {
              log.info("invoking postCounterReportsMultipartuploadProviderById");
              String id = rctx.pathParams().values().stream().findFirst().orElse(null);
              Boolean overwrite =
                  rctx.queryParam("overwrite").stream()
                      .findFirst()
                      .map(s -> s.equals("true"))
                      .orElse(false);
              CaseInsensitiveMap<String, String> okapiHeaders =
                  getOkapiHeadersFromRoutingContext(rctx);

              if (okapiHeaders.get(XOkapiHeaders.TENANT) == null) {
                endResponseWithReportUploadError(
                    rctx,
                    ReportUploadErrorCode.OTHER,
                    "Request is missing the %s header.".formatted(XOkapiHeaders.TENANT));
                return;
              }

              VertxUtil.<Response>toFuture(
                      handler ->
                          new CounterReportAPI()
                              .postCounterReportsMultipartuploadProviderById(
                                  id,
                                  overwrite,
                                  null,
                                  rctx,
                                  okapiHeaders,
                                  handler,
                                  rctx.vertx().getOrCreateContext()))
                  .onSuccess(resp -> endResponse(rctx, resp))
                  .onFailure(
                      t ->
                          endResponseWithReportUploadError(
                              rctx, ReportUploadErrorCode.OTHER, t.toString()));
            });

    resultHandler.handle(succeededFuture(true));
  }

  @SuppressWarnings(
      "java:S6880") // can't replace if with a switch statement because aspectj-maven-plugin:1.14
  // does not support java21 features
  private void endResponse(RoutingContext rctx, Response response) {
    rctx.response().setStatusCode(response.getStatus());
    response.getStringHeaders().forEach((k, v) -> rctx.response().putHeader(k, v));
    Object responseEntity = response.getEntity();
    if (responseEntity instanceof String entity) {
      rctx.response().end(entity);
    } else if (responseEntity instanceof ReportUploadError entity) {
      rctx.response().end(Json.encode(entity));
    } else {
      rctx.response().end();
    }
  }

  private void endResponseWithReportUploadError(
      RoutingContext rctx, ReportUploadErrorCode errorCode, String errorDetails) {
    ReportUploadError error = ReportUploadErrorFactory.create(errorCode, errorDetails);
    PostCounterReportsMultipartuploadProviderByIdResponse response =
        PostCounterReportsMultipartuploadProviderByIdResponse.respond400WithApplicationJson(error);

    endResponse(rctx, response);
  }
}
