package org.folio.rest.impl;

import static io.vertx.core.Future.succeededFuture;
import static org.folio.rest.util.Constants.TABLE_NAME_UDP;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.Response;
import org.folio.cql2pgjson.CQL2PgJSON;
import org.folio.cql2pgjson.exception.FieldException;
import org.folio.rest.annotations.Validate;
import org.folio.rest.jaxrs.model.UsageDataProvider;
import org.folio.rest.jaxrs.model.UsageDataProviders;
import org.folio.rest.jaxrs.model.UsageDataProvidersGetOrder;
import org.folio.rest.persist.PgUtil;
import org.folio.rest.persist.cql.CQLWrapper;
import org.folio.rest.tools.utils.ValidationHelper;

public class UsageDataProvidersAPI implements org.folio.rest.jaxrs.resource.UsageDataProviders {

  private final Logger logger = LoggerFactory.getLogger(UsageDataProvidersAPI.class);

  private CQLWrapper getCQL(String query, int limit, int offset) throws FieldException {
    return new CQLWrapper(new CQL2PgJSON(TABLE_NAME_UDP + ".jsonb"), query, limit, offset);
  }

  @Override
  @Validate
  public void getUsageDataProviders(
      String query,
      String orderBy,
      UsageDataProvidersGetOrder order,
      int offset,
      int limit,
      String lang,
      Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {
    logger.debug("Getting usage data providers");
    logger.debug("Headers present are: " + okapiHeaders.keySet().toString());

    CQLWrapper cql;
    try {
      cql = getCQL(query, limit, offset);
    } catch (Exception e) {
      ValidationHelper.handleError(e, asyncResultHandler);
      return;
    }

    PgUtil.postgresClient(vertxContext, okapiHeaders)
        .get(
            TABLE_NAME_UDP,
            UsageDataProvider.class,
            cql,
            true,
            reply -> {
              if (reply.succeeded()) {
                UsageDataProviders udProvidersDataCollection = new UsageDataProviders();
                List<UsageDataProvider> dataProviders = reply.result().getResults();
                udProvidersDataCollection.setUsageDataProviders(dataProviders);
                udProvidersDataCollection.setTotalRecords(
                    reply.result().getResultInfo().getTotalRecords());
                asyncResultHandler.handle(
                    succeededFuture(
                        GetUsageDataProvidersResponse.respond200WithApplicationJson(
                            udProvidersDataCollection)));
              } else {
                ValidationHelper.handleError(reply.cause(), asyncResultHandler);
              }
            });
  }

  @Override
  @Validate
  public void postUsageDataProviders(
      String lang,
      UsageDataProvider entity,
      Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {

    PgUtil.post(
        TABLE_NAME_UDP,
        entity,
        okapiHeaders,
        vertxContext,
        PostUsageDataProvidersResponse.class,
        asyncResultHandler);
  }

  @Override
  @Validate
  public void getUsageDataProvidersById(
      String id,
      String lang,
      Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {

    PgUtil.getById(
        TABLE_NAME_UDP,
        UsageDataProvider.class,
        id,
        okapiHeaders,
        vertxContext,
        GetUsageDataProvidersByIdResponse.class,
        asyncResultHandler);
  }

  @Override
  @Validate
  public void deleteUsageDataProvidersById(
      String id,
      String lang,
      Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {

    PgUtil.deleteById(
        TABLE_NAME_UDP,
        id,
        okapiHeaders,
        vertxContext,
        DeleteUsageDataProvidersByIdResponse.class,
        asyncResultHandler);
  }

  @Override
  @Validate
  public void putUsageDataProvidersById(
      String id,
      String lang,
      UsageDataProvider entity,
      Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {

    PgUtil.put(
        TABLE_NAME_UDP,
        entity,
        id,
        okapiHeaders,
        vertxContext,
        PutUsageDataProvidersByIdResponse.class,
        asyncResultHandler);
  }
}
