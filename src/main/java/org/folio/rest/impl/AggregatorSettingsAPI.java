package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.ws.rs.core.Response;
import org.folio.rest.annotations.Validate;
import org.folio.rest.jaxrs.model.AggregatorSetting;
import org.folio.rest.jaxrs.model.AggregatorSettingsDataCollection;
import org.folio.rest.jaxrs.resource.AggregatorSettingsResource;
import org.folio.rest.persist.Criteria.Criteria;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.Criteria.Limit;
import org.folio.rest.persist.Criteria.Offset;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.cql.CQLWrapper;
import org.folio.rest.tools.messages.MessageConsts;
import org.folio.rest.tools.messages.Messages;
import org.folio.rest.tools.utils.OutStream;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.rest.tools.utils.ValidationHelper;
import org.folio.rest.util.Constants;
import org.z3950.zing.cql.cql2pgjson.CQL2PgJSON;
import org.z3950.zing.cql.cql2pgjson.FieldException;

public class AggregatorSettingsAPI implements AggregatorSettingsResource {

  public static final String ID_FIELD = "_id";
  public static final String LABEL_FIELD = "'label'";
  public static final String SCHEMA_PATH = "/schemas/aggregatorSettingsData.json";
  private static final String TABLE_NAME_AGGREGATOR_SETTINGS = "aggregator_settings";

  private final Messages messages = Messages.getInstance();
  private final Logger logger = LoggerFactory.getLogger(AggregatorSettingsAPI.class);

  public AggregatorSettingsAPI(Vertx vertx, String tenantId) {
    PostgresClient.getInstance(vertx, tenantId).setIdField(ID_FIELD);
  }

  private CQLWrapper getCQL(String query, int limit, int offset) throws FieldException {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON(
        Arrays.asList(TABLE_NAME_AGGREGATOR_SETTINGS + ".jsonb"));
    return new CQLWrapper(cql2pgJson, query).setLimit(new Limit(limit))
        .setOffset(new Offset(offset));
  }

  @Override
  @Validate
  public void getAggregatorSettings(String query, String orderBy, Order order, int offset,
      int limit, String lang, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) throws Exception {
    logger.debug("Getting aggregator settings");
    try {
      CQLWrapper cql = getCQL(query, limit, offset);
      vertxContext.runOnContext(v -> {
        String tenantId = TenantTool
            .calculateTenantId(okapiHeaders.get(Constants.OKAPI_HEADER_TENANT));
        logger.debug("Headers present are: " + okapiHeaders.keySet().toString());
        logger.debug("tenantId = " + tenantId);
        String[] fieldList = {"*"};
        try {
          PostgresClient.getInstance(vertxContext.owner(), tenantId)
              .get(TABLE_NAME_AGGREGATOR_SETTINGS,
                  AggregatorSetting.class, fieldList, cql, true, false, reply -> {
                    try {
                      if (reply.succeeded()) {
                        AggregatorSettingsDataCollection aggregatorSettingsDataCollection = new AggregatorSettingsDataCollection();
                        List<AggregatorSetting> aggregatorSettings = (List<AggregatorSetting>) reply
                            .result().getResults();
                        aggregatorSettingsDataCollection.setAggregatorSettings(aggregatorSettings);
                        aggregatorSettingsDataCollection
                            .setTotalRecords(reply.result().getResultInfo().getTotalRecords());
                        asyncResultHandler.handle(Future.succeededFuture(
                            GetAggregatorSettingsResponse
                                .withJsonOK(aggregatorSettingsDataCollection)
                        ));
                      } else {
                        asyncResultHandler.handle(Future.succeededFuture(
                            GetAggregatorSettingsResponse.withPlainInternalServerError(
                                reply.cause().getMessage()
                            )
                        ));
                      }
                    } catch (Exception e) {
                      logger.debug(e.getLocalizedMessage());

                      asyncResultHandler.handle(Future.succeededFuture(
                          GetAggregatorSettingsResponse.withPlainInternalServerError(
                              reply.cause().getMessage()
                          )
                      ));
                    }
                  });
        } catch (IllegalStateException e) {
          logger.debug("IllegalStateException: " + e.getLocalizedMessage());
          asyncResultHandler
              .handle(Future.succeededFuture(GetAggregatorSettingsResponse.withPlainBadRequest(
                  "CQL Illegal State Error for '" + query + "': " + e.getLocalizedMessage())));
        } catch (Exception e) {
          Throwable cause = e;
          while (cause.getCause() != null) {
            cause = cause.getCause();
          }
          logger.debug(
              "Got error " + cause.getClass().getSimpleName() + ": " + e.getLocalizedMessage());
          if (cause.getClass().getSimpleName().contains("CQLParseException")) {
            logger.debug("BAD CQL");
            asyncResultHandler
                .handle(Future.succeededFuture(GetAggregatorSettingsResponse.withPlainBadRequest(
                    "CQL Parsing Error for '" + query + "': " + cause.getLocalizedMessage())));
          } else {
            asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
                GetAggregatorSettingsResponse.withPlainInternalServerError(
                    messages.getMessage(lang,
                        MessageConsts.InternalServerError))));
          }
        }
      });
    } catch (Exception e) {
      logger.error(e.getLocalizedMessage(), e);
      if (e.getCause() != null && e.getCause().getClass().getSimpleName()
          .contains("CQLParseException")) {
        logger.debug("BAD CQL");
        asyncResultHandler
            .handle(Future.succeededFuture(GetAggregatorSettingsResponse.withPlainBadRequest(
                "CQL Parsing Error for '" + query + "': " + e.getLocalizedMessage())));
      } else {
        asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
            GetAggregatorSettingsResponse.withPlainInternalServerError(
                messages.getMessage(lang,
                    MessageConsts.InternalServerError))));
      }
    }
  }

  @Override
  @Validate
  public void postAggregatorSettings(String lang, AggregatorSetting entity,
      Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) throws Exception {
    try {
      vertxContext.runOnContext(v -> {
        String tenantId = TenantTool
            .calculateTenantId(okapiHeaders.get(Constants.OKAPI_HEADER_TENANT));
        try {

          String id = entity.getId();
          if (id == null) {
            id = UUID.randomUUID().toString();
            entity.setId(id);
          }
          Criteria labelCrit = new Criteria(Constants.RAML_PATH + SCHEMA_PATH);
          labelCrit.addField(LABEL_FIELD);
          labelCrit.setOperation("=");
          labelCrit.setValue(entity.getLabel());
          Criterion crit = new Criterion(labelCrit);
          try {
            PostgresClient.getInstance(vertxContext.owner(),
                TenantTool.calculateTenantId(tenantId)).get(TABLE_NAME_AGGREGATOR_SETTINGS,
                AggregatorSetting.class, crit, true, getReply -> {
                  logger.debug(
                      "Attempting to get existing aggregator settings of same id and/or label");
                  if (getReply.failed()) {
                    logger.debug("Attempt to get aggregator settings failed: " +
                        getReply.cause().getMessage());
                    asyncResultHandler.handle(Future.succeededFuture(
                        PostAggregatorSettingsResponse.withPlainInternalServerError(
                            getReply.cause().getMessage())));
                  } else {
                    List<AggregatorSetting> aggregatorList = (List<AggregatorSetting>) getReply
                        .result()
                        .getResults();
                    if (aggregatorList.size() > 0) {
                      logger.debug("Aggregator setting with this id already exists");
                      asyncResultHandler.handle(Future.succeededFuture(
                          PostAggregatorSettingsResponse.withJsonUnprocessableEntity(
                              ValidationHelper.createValidationErrorMessage(
                                  "'label'", entity.getLabel(),
                                  "Aggregator setting with this id already exists"))));
                    } else {
                      PostgresClient postgresClient = PostgresClient
                          .getInstance(vertxContext.owner(), tenantId);
                      postgresClient
                          .save(TABLE_NAME_AGGREGATOR_SETTINGS, entity.getId(), entity, reply -> {
                            try {
                              if (reply.succeeded()) {
                                logger.debug("save successful");
                                final AggregatorSetting aggregatorSetting = entity;
                                aggregatorSetting.setId(entity.getId());
                                OutStream stream = new OutStream();
                                stream.setData(aggregatorSetting);
                                asyncResultHandler.handle(
                                    Future.succeededFuture(
                                        PostAggregatorSettingsResponse
                                            .withJsonCreated(
                                                reply.result(), stream)));
                              } else {
                                asyncResultHandler.handle(Future.succeededFuture(
                                    PostAggregatorSettingsResponse.withPlainInternalServerError(
                                        reply.cause().toString())));
                              }
                            } catch (Exception e) {
                              asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
                                  PostAggregatorSettingsResponse
                                      .withPlainInternalServerError(e.getMessage())));
                            }
                          });
                    }
                  }
                });
          } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            asyncResultHandler.handle(Future.succeededFuture(
                PostAggregatorSettingsResponse.withPlainInternalServerError(
                    messages.getMessage(lang, MessageConsts.InternalServerError))));
          }
        } catch (Exception e) {
          asyncResultHandler.handle(Future.succeededFuture(
              PostAggregatorSettingsResponse.withPlainInternalServerError(
                  messages.getMessage(lang, MessageConsts.InternalServerError))));
        }
      });
    } catch (Exception e) {
      asyncResultHandler.handle(Future.succeededFuture(
          PostAggregatorSettingsResponse.withPlainInternalServerError(
              messages.getMessage(lang, MessageConsts.InternalServerError))));
    }
  }

  @Override
  @Validate
  public void getAggregatorSettingsById(String id, String lang, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) throws Exception {
    try {
      vertxContext.runOnContext(v -> {
        String tenantId = TenantTool
            .calculateTenantId(okapiHeaders.get(Constants.OKAPI_HEADER_TENANT));
        try {
          Criteria idCrit = new Criteria(Constants.RAML_PATH + SCHEMA_PATH)
              .addField(ID_FIELD)
              .setJSONB(false)
              .setOperation("=")
              .setValue("'" + id + "'");
          Criterion criterion = new Criterion(idCrit);
          logger.debug("Using criterion: " + criterion.toString());
          PostgresClient.getInstance(vertxContext.owner(), tenantId)
              .get(TABLE_NAME_AGGREGATOR_SETTINGS, AggregatorSetting.class, criterion,
                  true, false, getReply -> {
                    if (getReply.failed()) {
                      asyncResultHandler.handle(Future.succeededFuture(
                          GetAggregatorSettingsByIdResponse.withPlainInternalServerError(
                              messages.getMessage(lang, MessageConsts.InternalServerError))));
                    } else {
                      List<AggregatorSetting> aggregatorSettingList = (List<AggregatorSetting>) getReply
                          .result()
                          .getResults();
                      if (aggregatorSettingList.size() < 1) {
                        asyncResultHandler.handle(Future.succeededFuture(
                            GetAggregatorSettingsByIdResponse
                                .withPlainNotFound("Aggregator setting" +
                                    messages.getMessage(lang,
                                        MessageConsts.ObjectDoesNotExist))));
                      } else if (aggregatorSettingList.size() > 1) {
                        logger.debug("Multiple aggregator settings found with the same id");
                        asyncResultHandler.handle(Future.succeededFuture(
                            GetAggregatorSettingsByIdResponse.withPlainInternalServerError(
                                messages.getMessage(lang,
                                    MessageConsts.InternalServerError))));
                      } else {
                        asyncResultHandler.handle(Future.succeededFuture(
                            GetAggregatorSettingsByIdResponse
                                .withJsonOK(aggregatorSettingList.get(0))));
                      }
                    }
                  });
        } catch (Exception e) {
          logger.debug("Error occurred: " + e.getMessage());
          asyncResultHandler.handle(Future.succeededFuture(
              GetAggregatorSettingsByIdResponse.withPlainInternalServerError(messages.getMessage(
                  lang, MessageConsts.InternalServerError))));
        }
      });
    } catch (Exception e) {
      asyncResultHandler.handle(Future.succeededFuture(
          GetAggregatorSettingsByIdResponse.withPlainInternalServerError(messages.getMessage(
              lang, MessageConsts.InternalServerError))));
    }
  }

  @Override
  @Validate
  public void deleteAggregatorSettingsById(String id, String lang, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) throws Exception {
    try {
      vertxContext.runOnContext(v -> {
        String tenantId = TenantTool
            .calculateTenantId(okapiHeaders.get(Constants.OKAPI_HEADER_TENANT));
        try {
          PostgresClient.getInstance(vertxContext.owner(), tenantId).delete(
              TABLE_NAME_AGGREGATOR_SETTINGS, id, deleteReply -> {
                if (deleteReply.failed()) {
                  logger.debug("Delete failed: " + deleteReply.cause().getMessage());
                  asyncResultHandler.handle(Future.succeededFuture(
                      DeleteAggregatorSettingsByIdResponse.withPlainNotFound("Delete failed.")));
                } else {
                  asyncResultHandler.handle(Future.succeededFuture(
                      DeleteAggregatorSettingsByIdResponse.withNoContent()));
                }
              });
        } catch (Exception e) {
          logger.debug("Delete failed: " + e.getMessage());
          asyncResultHandler.handle(
              Future.succeededFuture(
                  DeleteAggregatorSettingsByIdResponse.withPlainInternalServerError(
                      messages.getMessage(lang,
                          MessageConsts.InternalServerError))));
        }
      });
    } catch (Exception e) {
      asyncResultHandler.handle(
          Future.succeededFuture(
              DeleteAggregatorSettingsByIdResponse.withPlainInternalServerError(
                  messages.getMessage(lang,
                      MessageConsts.InternalServerError))));
    }
  }

  @Override
  @Validate
  public void putAggregatorSettingsById(String id, String lang, AggregatorSetting entity,
      Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) throws Exception {
    try {
      vertxContext.runOnContext(v -> {
        if (!id.equals(entity.getId())) {
          asyncResultHandler.handle(Future.succeededFuture(
              PutAggregatorSettingsByIdResponse
                  .withPlainBadRequest("You cannot change the value of the id field")));
        } else {
          String tenantId = TenantTool
              .calculateTenantId(okapiHeaders.get(Constants.OKAPI_HEADER_TENANT));
          Criteria nameCrit = new Criteria();
          nameCrit.addField(LABEL_FIELD);
          nameCrit.setOperation("=");
          nameCrit.setValue(entity.getLabel());
          try {
            PostgresClient.getInstance(vertxContext.owner(), tenantId)
                .get(TABLE_NAME_AGGREGATOR_SETTINGS,
                    AggregatorSetting.class, new Criterion(nameCrit), true, false, getReply -> {
                      if (getReply.failed()) {
                        logger.debug(
                            "Error querying existing aggregator settings: " + getReply.cause()
                                .getLocalizedMessage());
                        asyncResultHandler.handle(Future.succeededFuture(
                            PutAggregatorSettingsByIdResponse.withPlainInternalServerError(
                                messages.getMessage(lang,
                                    MessageConsts.InternalServerError))));
                      } else {
                        List<AggregatorSetting> aggregatorSettingList = (List<AggregatorSetting>) getReply
                            .result()
                            .getResults();
                        if (aggregatorSettingList.size() > 0 && (!aggregatorSettingList.get(0)
                            .getId()
                            .equals(entity.getId()))) {
                          asyncResultHandler.handle(Future.succeededFuture(
                              PutAggregatorSettingsByIdResponse.withPlainBadRequest(
                                  "Label " + entity.getLabel() + " is already in use")));
                        } else {
                          Date createdDate = null;
                          Date now = new Date();
                          if (aggregatorSettingList.size() > 0) {
                            createdDate = aggregatorSettingList.get(0).getCreatedDate();
                          } else {
                            createdDate = now;
                          }
                          entity.setUpdatedDate(now);
                          entity.setCreatedDate(createdDate);
                          try {
                            PostgresClient.getInstance(vertxContext.owner(), tenantId).update(
                                TABLE_NAME_AGGREGATOR_SETTINGS, entity, id,
                                putReply -> {
                                  try {
                                    if (putReply.failed()) {
                                      asyncResultHandler.handle(Future.succeededFuture(
                                          PutAggregatorSettingsByIdResponse
                                              .withPlainInternalServerError(
                                                  putReply.cause().getMessage())));
                                    } else {
                                      asyncResultHandler.handle(Future.succeededFuture(
                                          PutAggregatorSettingsByIdResponse.withNoContent()));
                                    }
                                  } catch (Exception e) {
                                    asyncResultHandler.handle(Future.succeededFuture(
                                        PutAggregatorSettingsByIdResponse
                                            .withPlainInternalServerError(
                                                messages.getMessage(lang,
                                                    MessageConsts.InternalServerError))));
                                  }
                                });
                          } catch (Exception e) {
                            asyncResultHandler.handle(Future.succeededFuture(
                                PutAggregatorSettingsByIdResponse.withPlainInternalServerError(
                                    messages.getMessage(lang,
                                        MessageConsts.InternalServerError))));
                          }
                        }
                      }
                    });
          } catch (Exception e) {
            logger.debug(e.getLocalizedMessage());
            asyncResultHandler.handle(Future.succeededFuture(
                PutAggregatorSettingsByIdResponse.withPlainInternalServerError(
                    messages.getMessage(lang, MessageConsts.InternalServerError))));
          }
        }
      });
    } catch (Exception e) {
      logger.debug(e.getLocalizedMessage());
      asyncResultHandler.handle(Future.succeededFuture(
          PutAggregatorSettingsByIdResponse.withPlainInternalServerError(
              messages.getMessage(lang, MessageConsts.InternalServerError))));
    }
  }
}
