package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.ws.rs.core.Response;
import org.folio.rest.annotations.Validate;
import org.folio.rest.jaxrs.model.UsageDataProvider;
import org.folio.rest.jaxrs.model.UsageDataProviders;
import org.folio.rest.jaxrs.model.UsageDataProvidersGetOrder;
import org.folio.rest.persist.Criteria.Criteria;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.Criteria.Limit;
import org.folio.rest.persist.Criteria.Offset;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.cql.CQLWrapper;
import org.folio.rest.tools.messages.MessageConsts;
import org.folio.rest.tools.messages.Messages;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.rest.tools.utils.ValidationHelper;
import org.folio.rest.util.AttributeNameAdder;
import org.folio.rest.util.Constants;
import org.z3950.zing.cql.cql2pgjson.CQL2PgJSON;
import org.z3950.zing.cql.cql2pgjson.FieldException;

public class UsageDataProvidersAPI implements org.folio.rest.jaxrs.resource.UsageDataProviders {

  private static final String ID_FIELD = "_id";
  private static final String LABEL_FIELD = "'label'";
  private static final String TABLE_NAME_SUSHI_SETTINGS = "usage_data_providers";

  private final Messages messages = Messages.getInstance();
  private final Logger logger = LoggerFactory.getLogger(UsageDataProvidersAPI.class);

  public UsageDataProvidersAPI(Vertx vertx, String tenantId) {
    PostgresClient.getInstance(vertx, tenantId).setIdField(ID_FIELD);
  }

  private CQLWrapper getCQL(String query, int limit, int offset) throws FieldException {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON(Arrays.asList(TABLE_NAME_SUSHI_SETTINGS + ".jsonb"));
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
            String tenantId =
                TenantTool.calculateTenantId(okapiHeaders.get(Constants.OKAPI_HEADER_TENANT));
            logger.debug("Headers present are: " + okapiHeaders.keySet().toString());
            logger.debug("tenantId = " + tenantId);
            String[] fieldList = {"*"};
            try {
              PostgresClient.getInstance(vertxContext.owner(), tenantId)
                  .get(
                      TABLE_NAME_SUSHI_SETTINGS,
                      UsageDataProvider.class,
                      fieldList,
                      cql,
                      true,
                      false,
                      reply -> {
                        try {
                          if (reply.succeeded()) {
                            UsageDataProviders udProvidersDataCollection = new UsageDataProviders();
                            List<UsageDataProvider> dataProviders =
                                (List<UsageDataProvider>) reply.result().getResults();
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
    try {
      vertxContext.runOnContext(
          v -> {
            String tenantId =
                TenantTool.calculateTenantId(okapiHeaders.get(Constants.OKAPI_HEADER_TENANT));
            try {
              String id = entity.getId();
              if (id == null) {
                id = UUID.randomUUID().toString();
                entity.setId(id);
              }
              Criteria labelCrit = new Criteria();
              labelCrit.addField(LABEL_FIELD);
              labelCrit.setOperation("=");
              labelCrit.setValue(entity.getLabel());
              Criterion crit = new Criterion(labelCrit);
              try {
                PostgresClient.getInstance(
                        vertxContext.owner(), TenantTool.calculateTenantId(tenantId))
                    .get(
                        TABLE_NAME_SUSHI_SETTINGS,
                        UsageDataProvider.class,
                        crit,
                        true,
                        getReply -> {
                          logger.debug(
                              "Attempting to get existing sushi settings of same id and/or label");
                          if (getReply.failed()) {
                            logger.debug(
                                "Attempt to get sushi settings failed: "
                                    + getReply.cause().getMessage());
                            asyncResultHandler.handle(
                                Future.succeededFuture(
                                    PostUsageDataProvidersResponse.respond500WithTextPlain(
                                        getReply.cause().getMessage())));
                          } else {
                            List<UsageDataProvider> dataProviders =
                                (List<UsageDataProvider>) getReply.result().getResults();
                            if (dataProviders.size() > 0) {
                              logger.debug("Usage data provider with this label/id already exists");
                              asyncResultHandler.handle(
                                  Future.succeededFuture(
                                      PostUsageDataProvidersResponse.respond422WithApplicationJson(
                                          ValidationHelper.createValidationErrorMessage(
                                              "'label'",
                                              entity.getLabel(),
                                              "Usage data provider with this label/id already exists"))));
                            } else {

                              Future<UsageDataProvider> udpWithNames =
                                  AttributeNameAdder.resolveAndAddAttributeNames(
                                      entity, okapiHeaders, vertxContext.owner());

                              udpWithNames.setHandler(
                                  ar -> {
                                    if (ar.succeeded()) {
                                      UsageDataProvider udp = ar.result();
                                      PostgresClient postgresClient =
                                          PostgresClient.getInstance(
                                              vertxContext.owner(), tenantId);
                                      postgresClient.save(
                                          TABLE_NAME_SUSHI_SETTINGS,
                                          udp.getId(),
                                          udp,
                                          reply -> {
                                            try {
                                              if (reply.succeeded()) {
                                                logger.debug("save successful");
                                                asyncResultHandler.handle(
                                                    Future.succeededFuture(
                                                        PostUsageDataProvidersResponse
                                                            .respond201WithApplicationJson(
                                                                udp,
                                                                PostUsageDataProvidersResponse
                                                                    .headersFor201()
                                                                    .withLocation(
                                                                        "/usage-data-providers/"
                                                                            + udp.getId()))));
                                              } else {
                                                asyncResultHandler.handle(
                                                    Future.succeededFuture(
                                                        PostUsageDataProvidersResponse
                                                            .respond500WithTextPlain(
                                                                reply.cause().toString())));
                                              }
                                            } catch (Exception e) {
                                              asyncResultHandler.handle(
                                                  io.vertx.core.Future.succeededFuture(
                                                      PostUsageDataProvidersResponse
                                                          .respond500WithTextPlain(
                                                              e.getMessage())));
                                            }
                                          });
                                    } else {
                                      asyncResultHandler.handle(
                                          Future.succeededFuture(
                                              PostUsageDataProvidersResponse
                                                  .respond500WithTextPlain(ar.cause())));
                                    }
                                  });
                            }
                          }
                        });
              } catch (Exception e) {
                logger.error(e.getLocalizedMessage(), e);
                asyncResultHandler.handle(
                    Future.succeededFuture(
                        PostUsageDataProvidersResponse.respond500WithTextPlain(
                            messages.getMessage(lang, MessageConsts.InternalServerError))));
              }
            } catch (Exception e) {
              asyncResultHandler.handle(
                  Future.succeededFuture(
                      PostUsageDataProvidersResponse.respond500WithTextPlain(
                          messages.getMessage(lang, MessageConsts.InternalServerError))));
            }
          });
    } catch (Exception e) {
      asyncResultHandler.handle(
          Future.succeededFuture(
              PostUsageDataProvidersResponse.respond500WithTextPlain(
                  messages.getMessage(lang, MessageConsts.InternalServerError))));
    }
  }

  @Override
  @Validate
  public void getUsageDataProvidersById(
      String id,
      String lang,
      Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {
    try {
      vertxContext.runOnContext(
          v -> {
            String tenantId =
                TenantTool.calculateTenantId(okapiHeaders.get(Constants.OKAPI_HEADER_TENANT));
            try {
              Criteria idCrit =
                  new Criteria()
                      .addField(ID_FIELD)
                      .setJSONB(false)
                      .setOperation("=")
                      .setValue("'" + id + "'");
              Criterion criterion = new Criterion(idCrit);
              logger.debug("Using criterion: " + criterion.toString());
              PostgresClient.getInstance(vertxContext.owner(), tenantId)
                  .get(
                      TABLE_NAME_SUSHI_SETTINGS,
                      UsageDataProvider.class,
                      criterion,
                      true,
                      false,
                      getReply -> {
                        if (getReply.failed()) {
                          asyncResultHandler.handle(
                              Future.succeededFuture(
                                  GetUsageDataProvidersByIdResponse.respond500WithTextPlain(
                                      messages.getMessage(
                                          lang, MessageConsts.InternalServerError))));
                        } else {
                          List<UsageDataProvider> dataProviders =
                              (List<UsageDataProvider>) getReply.result().getResults();
                          if (dataProviders.size() < 1) {
                            asyncResultHandler.handle(
                                Future.succeededFuture(
                                    GetUsageDataProvidersByIdResponse.respond404WithTextPlain(
                                        "Usage data provider "
                                            + messages.getMessage(
                                                lang, MessageConsts.ObjectDoesNotExist))));
                          } else if (dataProviders.size() > 1) {
                            logger.debug("Multiple usage data providers found with the same id");
                            asyncResultHandler.handle(
                                Future.succeededFuture(
                                    GetUsageDataProvidersByIdResponse.respond500WithTextPlain(
                                        messages.getMessage(
                                            lang, MessageConsts.InternalServerError))));
                          } else {
                            asyncResultHandler.handle(
                                Future.succeededFuture(
                                    GetUsageDataProvidersByIdResponse.respond200WithApplicationJson(
                                        dataProviders.get(0))));
                          }
                        }
                      });
            } catch (Exception e) {
              logger.debug("Error occurred: " + e.getMessage());
              asyncResultHandler.handle(
                  Future.succeededFuture(
                      GetUsageDataProvidersByIdResponse.respond500WithTextPlain(
                          messages.getMessage(lang, MessageConsts.InternalServerError))));
            }
          });
    } catch (Exception e) {
      asyncResultHandler.handle(
          Future.succeededFuture(
              GetUsageDataProvidersByIdResponse.respond500WithTextPlain(
                  messages.getMessage(lang, MessageConsts.InternalServerError))));
    }
  }

  @Override
  @Validate
  public void deleteUsageDataProvidersById(
      String id,
      String lang,
      Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {
    try {
      vertxContext.runOnContext(
          v -> {
            String tenantId =
                TenantTool.calculateTenantId(okapiHeaders.get(Constants.OKAPI_HEADER_TENANT));
            Criteria idCrit =
                new Criteria()
                    .addField(ID_FIELD)
                    .setJSONB(false)
                    .setOperation("=")
                    .setValue("'" + id + "'");
            try {
              PostgresClient.getInstance(vertxContext.owner(), tenantId)
                  .delete(
                      TABLE_NAME_SUSHI_SETTINGS,
                      new Criterion(idCrit),
                      deleteReply -> {
                        if (deleteReply.failed()) {
                          logger.debug("Delete failed: " + deleteReply.cause().getMessage());
                          asyncResultHandler.handle(
                              Future.succeededFuture(
                                  DeleteUsageDataProvidersByIdResponse.respond404WithTextPlain(
                                      "Not found")));
                        } else {
                          asyncResultHandler.handle(
                              Future.succeededFuture(
                                  DeleteUsageDataProvidersByIdResponse.respond204()));
                        }
                      });
            } catch (Exception e) {
              logger.debug("Delete failed: " + e.getMessage());
              asyncResultHandler.handle(
                  Future.succeededFuture(
                      DeleteUsageDataProvidersByIdResponse.respond500WithTextPlain(
                          messages.getMessage(lang, MessageConsts.InternalServerError))));
            }
          });
    } catch (Exception e) {
      asyncResultHandler.handle(
          Future.succeededFuture(
              DeleteUsageDataProvidersByIdResponse.respond500WithTextPlain(
                  messages.getMessage(lang, MessageConsts.InternalServerError))));
    }
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
    try {
      vertxContext.runOnContext(
          v -> {
            if (!id.equals(entity.getId())) {
              asyncResultHandler.handle(
                  Future.succeededFuture(
                      PutUsageDataProvidersByIdResponse.respond400WithTextPlain(
                          "You cannot change the value of the id field")));
            } else {
              String tenantId =
                  TenantTool.calculateTenantId(okapiHeaders.get(Constants.OKAPI_HEADER_TENANT));
              Criteria nameCrit = new Criteria();
              nameCrit.addField(LABEL_FIELD);
              nameCrit.setOperation("=");
              nameCrit.setValue("'" + entity.getLabel() + "'");
              try {
                PostgresClient.getInstance(vertxContext.owner(), tenantId)
                    .get(
                        TABLE_NAME_SUSHI_SETTINGS,
                        UsageDataProvider.class,
                        new Criterion(nameCrit),
                        true,
                        false,
                        getReply -> {
                          if (getReply.failed()) {
                            logger.debug(
                                "Error querying existing sushi settings: "
                                    + getReply.cause().getLocalizedMessage());
                            asyncResultHandler.handle(
                                Future.succeededFuture(
                                    PutUsageDataProvidersByIdResponse.respond500WithTextPlain(
                                        messages.getMessage(
                                            lang, MessageConsts.InternalServerError))));
                          } else {
                            List<UsageDataProvider> dataProviders =
                                (List<UsageDataProvider>) getReply.result().getResults();
                            if (dataProviders.size() > 0
                                && (!dataProviders.get(0).getId().equals(entity.getId()))) {
                              asyncResultHandler.handle(
                                  Future.succeededFuture(
                                      PutUsageDataProvidersByIdResponse.respond400WithTextPlain(
                                          "Label " + entity.getLabel() + " is already in use")));
                            } else {
                              Criteria idCrit =
                                  new Criteria()
                                      .addField(ID_FIELD)
                                      .setJSONB(false)
                                      .setOperation("=")
                                      .setValue("'" + id + "'");
                              try {
                                Future<UsageDataProvider> udpWithNames =
                                    AttributeNameAdder.resolveAndAddAttributeNames(
                                        entity, okapiHeaders, vertxContext.owner());
                                udpWithNames.setHandler(
                                    ar -> {
                                      if (ar.succeeded()) {
                                        UsageDataProvider udp = ar.result();
                                        PostgresClient.getInstance(vertxContext.owner(), tenantId)
                                            .update(
                                                TABLE_NAME_SUSHI_SETTINGS,
                                                udp,
                                                new Criterion(idCrit),
                                                true,
                                                putReply -> {
                                                  try {
                                                    if (putReply.failed()) {
                                                      asyncResultHandler.handle(
                                                          Future.succeededFuture(
                                                              PutUsageDataProvidersByIdResponse
                                                                  .respond500WithTextPlain(
                                                                      putReply
                                                                          .cause()
                                                                          .getMessage())));
                                                    } else {
                                                      asyncResultHandler.handle(
                                                          Future.succeededFuture(
                                                              PutUsageDataProvidersByIdResponse
                                                                  .respond204()));
                                                    }
                                                  } catch (Exception e) {
                                                    asyncResultHandler.handle(
                                                        Future.succeededFuture(
                                                            PutUsageDataProvidersByIdResponse
                                                                .respond500WithTextPlain(
                                                                    messages.getMessage(
                                                                        lang,
                                                                        MessageConsts
                                                                            .InternalServerError))));
                                                  }
                                                });
                                      } else {
                                        asyncResultHandler.handle(
                                            Future.succeededFuture(
                                                PutUsageDataProvidersByIdResponse
                                                    .respond500WithTextPlain(ar.cause())));
                                      }
                                    });
                              } catch (Exception e) {
                                asyncResultHandler.handle(
                                    Future.succeededFuture(
                                        PutUsageDataProvidersByIdResponse.respond500WithTextPlain(
                                            messages.getMessage(
                                                lang, MessageConsts.InternalServerError))));
                              }
                            }
                          }
                        });
              } catch (Exception e) {
                logger.debug(e.getLocalizedMessage());
                asyncResultHandler.handle(
                    Future.succeededFuture(
                        PutUsageDataProvidersByIdResponse.respond500WithTextPlain(
                            messages.getMessage(lang, MessageConsts.InternalServerError))));
              }
            }
          });
    } catch (Exception e) {
      logger.debug(e.getLocalizedMessage());
      asyncResultHandler.handle(
          Future.succeededFuture(
              PutUsageDataProvidersByIdResponse.respond500WithTextPlain(
                  messages.getMessage(lang, MessageConsts.InternalServerError))));
    }
  }
}
