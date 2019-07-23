package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.Response;
import org.folio.cql2pgjson.CQL2PgJSON;
import org.folio.cql2pgjson.exception.FieldException;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.rest.annotations.Validate;
import org.folio.rest.jaxrs.model.UsageDataProvider;
import org.folio.rest.jaxrs.model.UsageDataProviders;
import org.folio.rest.jaxrs.model.UsageDataProvidersGetOrder;
import org.folio.rest.persist.Criteria.Limit;
import org.folio.rest.persist.Criteria.Offset;
import org.folio.rest.persist.PgUtil;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.cql.CQLWrapper;
import org.folio.rest.tools.messages.MessageConsts;
import org.folio.rest.tools.messages.Messages;
import org.folio.rest.tools.utils.TenantTool;

public class UsageDataProvidersAPI implements org.folio.rest.jaxrs.resource.UsageDataProviders {

  private static final String TABLE_UDP = "usage_data_providers";

  private final Messages messages = Messages.getInstance();
  private final Logger logger = LoggerFactory.getLogger(UsageDataProvidersAPI.class);

  private CQLWrapper getCQL(String query, int limit, int offset) throws FieldException {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON(Arrays.asList(TABLE_UDP + ".jsonb"));
    return new CQLWrapper(cql2pgJson, query)
        .setLimit(new Limit(limit))
        .setOffset(new Offset(offset));
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
    try {
      CQLWrapper cql = getCQL(query, limit, offset);
      vertxContext.runOnContext(
          v -> {
            String tenantId = TenantTool.calculateTenantId(okapiHeaders.get(XOkapiHeaders.TENANT));
            logger.debug("Headers present are: " + okapiHeaders.keySet().toString());
            logger.debug("tenantId = " + tenantId);
            String[] fieldList = {"*"};
            try {
              PostgresClient.getInstance(vertxContext.owner(), tenantId)
                  .get(
                      TABLE_UDP,
                      UsageDataProvider.class,
                      fieldList,
                      cql,
                      true,
                      false,
                      reply -> {
                        try {
                          if (reply.succeeded()) {
                            UsageDataProviders udProvidersDataCollection = new UsageDataProviders();
                            List<UsageDataProvider> dataProviders = reply.result().getResults();
                            udProvidersDataCollection.setUsageDataProviders(dataProviders);
                            udProvidersDataCollection.setTotalRecords(
                                reply.result().getResultInfo().getTotalRecords());
                            asyncResultHandler.handle(
                                Future.succeededFuture(
                                    GetUsageDataProvidersResponse.respond200WithApplicationJson(
                                        udProvidersDataCollection)));
                          } else {
                            asyncResultHandler.handle(
                                Future.succeededFuture(
                                    GetUsageDataProvidersResponse.respond500WithTextPlain(
                                        reply.cause().getMessage())));
                          }
                        } catch (Exception e) {
                          logger.debug(e.getLocalizedMessage());

                          asyncResultHandler.handle(
                              Future.succeededFuture(
                                  GetUsageDataProvidersResponse.respond500WithTextPlain(
                                      reply.cause().getMessage())));
                        }
                      });
            } catch (IllegalStateException e) {
              logger.debug("IllegalStateException: " + e.getLocalizedMessage());
              asyncResultHandler.handle(
                  Future.succeededFuture(
                      GetUsageDataProvidersResponse.respond400WithTextPlain(
                          "CQL Illegal State Error for '" + "" + "': " + e.getLocalizedMessage())));
            } catch (Exception e) {
              Throwable cause = e;
              while (cause.getCause() != null) {
                cause = cause.getCause();
              }
              logger.debug(
                  "Got error " + cause.getClass().getSimpleName() + ": " + e.getLocalizedMessage());
              if (cause.getClass().getSimpleName().contains("CQLParseException")) {
                logger.debug("BAD CQL");
                asyncResultHandler.handle(
                    Future.succeededFuture(
                        GetUsageDataProvidersResponse.respond400WithTextPlain(
                            "CQL Parsing Error for '" + "" + "': " + cause.getLocalizedMessage())));
              } else {
                asyncResultHandler.handle(
                    io.vertx.core.Future.succeededFuture(
                        GetUsageDataProvidersResponse.respond500WithTextPlain(
                            messages.getMessage(lang, MessageConsts.InternalServerError))));
              }
            }
          });
    } catch (Exception e) {
      logger.error(e.getLocalizedMessage(), e);
      if (e.getCause() != null
          && e.getCause().getClass().getSimpleName().contains("CQLParseException")) {
        logger.debug("BAD CQL");
        asyncResultHandler.handle(
            Future.succeededFuture(
                GetUsageDataProvidersResponse.respond400WithTextPlain(
                    "CQL Parsing Error for '" + "" + "': " + e.getLocalizedMessage())));
      } else {
        asyncResultHandler.handle(
            io.vertx.core.Future.succeededFuture(
                GetUsageDataProvidersResponse.respond500WithTextPlain(
                    messages.getMessage(lang, MessageConsts.InternalServerError))));
      }
    }
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
        TABLE_UDP,
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
        TABLE_UDP,
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
        TABLE_UDP,
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
        TABLE_UDP,
        entity,
        id,
        okapiHeaders,
        vertxContext,
        PutUsageDataProvidersByIdResponse.class,
        asyncResultHandler);
  }
}
