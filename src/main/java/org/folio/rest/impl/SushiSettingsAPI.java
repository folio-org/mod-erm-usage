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
import javax.ws.rs.core.Response;
import org.folio.rest.annotations.Validate;
import org.folio.rest.jaxrs.model.SushiSetting;
import org.folio.rest.jaxrs.model.SushiSettingsDataCollection;
import org.folio.rest.jaxrs.resource.SushiSettingsResource;
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
import org.z3950.zing.cql.cql2pgjson.CQL2PgJSON;
import org.z3950.zing.cql.cql2pgjson.FieldException;

public class SushiSettingsAPI implements SushiSettingsResource {

  public static final String RAML_PATH = "apidocs/raml";
  private static final String OKAPI_HEADER_TENANT = "x-okapi-tenant";
  private static final String TABLE_NAME_SUSHI_SETTINGS = "sushi_settings";
  private final Messages messages = Messages.getInstance();
  private final Logger logger = LoggerFactory.getLogger(SushiSettingsAPI.class);

  public SushiSettingsAPI(Vertx vertx, String tenantId) {
    PostgresClient.getInstance(vertx, tenantId).setIdField("id");
  }

  private CQLWrapper getCQL(String query, int limit, int offset) throws FieldException {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON(Arrays.asList(TABLE_NAME_SUSHI_SETTINGS + ".jsonb"));
    return new CQLWrapper(cql2pgJson, query).setLimit(new Limit(limit))
        .setOffset(new Offset(offset));
  }

  @Validate
  @Override
  public void getSushisettings(String query, String orderBy, Order order, int offset, int limit,
      String lang, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) throws Exception {
    logger.debug("Getting sushi settings");
    try {
      CQLWrapper cql = getCQL(query, limit, offset);
      vertxContext.runOnContext(v -> {
        String tenantId = TenantTool.calculateTenantId(okapiHeaders.get(OKAPI_HEADER_TENANT));
        logger.debug("Headers present are: " + okapiHeaders.keySet().toString());
        logger.debug("tenantId = " + tenantId);
        String[] fieldList = {"*"};
        try {
          PostgresClient.getInstance(vertxContext.owner(), tenantId).get(TABLE_NAME_SUSHI_SETTINGS,
              SushiSetting.class, fieldList, cql, true, false, reply -> {
                try {
                  if (reply.succeeded()) {
                    SushiSettingsDataCollection sushiSettingsDataCollection = new SushiSettingsDataCollection();
                    List<SushiSetting> sushiSettings = (List<SushiSetting>) reply
                        .result().getResults();
                    sushiSettingsDataCollection.setSushiSettings(sushiSettings);
                    sushiSettingsDataCollection
                        .setTotalRecords(reply.result().getResultInfo().getTotalRecords());
                    asyncResultHandler.handle(Future.succeededFuture(
                        GetSushisettingsResponse.withJsonOK(sushiSettingsDataCollection)
                    ));
                  } else {
                    asyncResultHandler.handle(Future.succeededFuture(
                        GetSushisettingsResponse.withPlainInternalServerError(
                            reply.cause().getMessage()
                        )
                    ));
                  }
                } catch (Exception e) {
                  logger.debug(e.getLocalizedMessage());

                  asyncResultHandler.handle(Future.succeededFuture(
                      GetSushisettingsResponse.withPlainInternalServerError(
                          reply.cause().getMessage()
                      )
                  ));
                }
              });
        } catch (IllegalStateException e) {
          logger.debug("IllegalStateException: " + e.getLocalizedMessage());
          asyncResultHandler
              .handle(Future.succeededFuture(GetSushisettingsResponse.withPlainBadRequest(
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
            asyncResultHandler
                .handle(Future.succeededFuture(GetSushisettingsResponse.withPlainBadRequest(
                    "CQL Parsing Error for '" + "" + "': " + cause.getLocalizedMessage())));
          } else {
            asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
                GetSushisettingsResponse.withPlainInternalServerError(
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
            .handle(Future.succeededFuture(GetSushisettingsResponse.withPlainBadRequest(
                "CQL Parsing Error for '" + "" + "': " + e.getLocalizedMessage())));
      } else {
        asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
            GetSushisettingsResponse.withPlainInternalServerError(
                messages.getMessage(lang,
                    MessageConsts.InternalServerError))));
      }
    }
  }

  @Override
  @Validate
  public void postSushisettings(String lang, SushiSetting
      entity, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) throws
      Exception {
    try {
      vertxContext.runOnContext(v -> {
        String tenantId = TenantTool.calculateTenantId(okapiHeaders.get(OKAPI_HEADER_TENANT));
        try {
          Criteria idCrit = new Criteria(RAML_PATH + "/schemas/sushiSettingsData.json");
          idCrit.addField("'id'");
          idCrit.setOperation("=");
          idCrit.setValue(entity.getId());
          Criteria labelCrit = new Criteria(RAML_PATH + "/schemas/sushiSettingsData.json");
          labelCrit.addField("'label'");
          labelCrit.setOperation("=");
          labelCrit.setValue(entity.getLabel());
          Criterion crit = new Criterion();
          crit.addCriterion(idCrit, "OR", labelCrit);

          try {
            PostgresClient.getInstance(vertxContext.owner(),
                TenantTool.calculateTenantId(tenantId)).get("sushi_settings",
                SushiSetting.class, crit, true, getReply -> {
                  logger.debug("Attempting to get existing sushisettings of same id and/or label");
                  if (getReply.failed()) {
                    logger.debug("Attempt to get users failed: " +
                        getReply.cause().getMessage());
                    asyncResultHandler.handle(Future.succeededFuture(
                        PostSushisettingsResponse.withPlainInternalServerError(
                            getReply.cause().getMessage())));
                  } else {
                    List<SushiSetting> sushiList = (List<SushiSetting>) getReply.result()
                        .getResults();
                    if (sushiList.size() > 0) {
                      logger.debug("Sushi Setting with this id already exists");
                      asyncResultHandler.handle(Future.succeededFuture(
                          PostSushisettingsResponse.withJsonUnprocessableEntity(
                              ValidationHelper.createValidationErrorMessage(
                                  "'label'", entity.getLabel(),
                                  "Sushisetting with this id already exists"))));
                    } else {
                      PostgresClient postgresClient = PostgresClient
                          .getInstance(vertxContext.owner(), tenantId);
                      postgresClient.startTx(connection -> {
                        logger.debug("Attempting to save new record");
                        try {
                          Date now = new Date();
                          entity.setCreatedDate(now);
                          entity.setUpdatedDate(now);
                          postgresClient.save(connection, "sushi_settings", entity,
                              reply -> {
                                try {
                                  if (reply.succeeded()) {
                                    logger.debug("save successful");
                                    final SushiSetting sushiSetting = entity;
                                    sushiSetting.setId(entity.getId());
                                    OutStream stream = new OutStream();
                                    stream.setData(sushiSetting);
                                    postgresClient.endTx(connection, done -> {
                                      asyncResultHandler.handle(
                                          Future.succeededFuture(
                                              PostSushisettingsResponse
                                                  .withJsonCreated(
                                                      reply.result(), stream)));
                                    });
                                  } else {
                                    postgresClient.rollbackTx(connection, rollback -> {
                                      asyncResultHandler.handle(Future.succeededFuture(
                                          PostSushisettingsResponse.withPlainBadRequest(
                                              messages.getMessage(lang,
                                                  MessageConsts.UnableToProcessRequest))));
                                    });
                                  }
                                } catch (Exception e) {
                                  asyncResultHandler.handle(Future.succeededFuture(
                                      PostSushisettingsResponse.withPlainInternalServerError(
                                          e.getMessage())));
                                }
                              });
                        } catch (Exception e) {
                          postgresClient.rollbackTx(connection, rollback -> {
                            asyncResultHandler.handle(Future.succeededFuture(
                                PostSushisettingsResponse.withPlainInternalServerError(
                                    getReply.cause().getMessage())));
                          });
                        }
                      });
                    }
                  }
                });
          } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            asyncResultHandler.handle(Future.succeededFuture(
                PostSushisettingsResponse.withPlainInternalServerError(
                    messages.getMessage(lang, MessageConsts.InternalServerError))));

          }
        } catch (Exception e) {
          asyncResultHandler.handle(Future.succeededFuture(
              PostSushisettingsResponse.withPlainInternalServerError(
                  messages.getMessage(lang, MessageConsts.InternalServerError))));
        }
      });
    } catch (Exception e) {
      asyncResultHandler.handle(Future.succeededFuture(
          PostSushisettingsResponse.withPlainInternalServerError(
              messages.getMessage(lang, MessageConsts.InternalServerError))));
    }
  }

  @Override
  @Validate
  public void getSushisettingsById(String id, String lang, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) throws
      Exception {
    try {
      vertxContext.runOnContext(v -> {
        String tenantId = TenantTool.calculateTenantId(okapiHeaders.get(OKAPI_HEADER_TENANT));
        try {
          Criteria idCrit = new Criteria(RAML_PATH + "/schemas/sushiSettingsData.json");
          idCrit.addField("'id'");
          idCrit.setOperation("=");
          idCrit.setValue(id);
          Criterion criterion = new Criterion(idCrit);
          logger.debug("Using criterion: " + criterion.toString());
          PostgresClient.getInstance(vertxContext.owner(), tenantId)
              .get("sushi_settings", SushiSetting.class, criterion,
                  true, false, getReply -> {
                    if (getReply.failed()) {
                      asyncResultHandler.handle(Future.succeededFuture(
                          GetSushisettingsByIdResponse.withPlainInternalServerError(
                              messages.getMessage(lang, MessageConsts.InternalServerError))));
                    } else {
                      List<SushiSetting> userList = (List<SushiSetting>) getReply.result()
                          .getResults();
                      if (userList.size() < 1) {
                        asyncResultHandler.handle(Future.succeededFuture(
                            GetSushisettingsByIdResponse.withPlainNotFound("Sushi setting" +
                                messages.getMessage(lang,
                                    MessageConsts.ObjectDoesNotExist))));
                      } else if (userList.size() > 1) {
                        logger.debug("Multiple sushi settings found with the same id");
                        asyncResultHandler.handle(Future.succeededFuture(
                            GetSushisettingsByIdResponse.withPlainInternalServerError(
                                messages.getMessage(lang,
                                    MessageConsts.InternalServerError))));
                      } else {
                        asyncResultHandler.handle(Future.succeededFuture(
                            GetSushisettingsByIdResponse.withJsonOK(userList.get(0))));
                      }
                    }
                  });
        } catch (Exception e) {
          logger.debug("Error occurred: " + e.getMessage());
          asyncResultHandler.handle(Future.succeededFuture(
              GetSushisettingsByIdResponse.withPlainInternalServerError(messages.getMessage(
                  lang, MessageConsts.InternalServerError))));
        }
      });
    } catch (Exception e) {
      asyncResultHandler.handle(Future.succeededFuture(
          GetSushisettingsByIdResponse.withPlainInternalServerError(messages.getMessage(
              lang, MessageConsts.InternalServerError))));
    }

  }

  @Override
  @Validate
  public void deleteSushisettingsById(String id, String
      lang, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) throws
      Exception {
    try {
      vertxContext.runOnContext(v -> {
        String tenantId = TenantTool.calculateTenantId(okapiHeaders.get(OKAPI_HEADER_TENANT));
        Criteria idCrit = new Criteria();
        idCrit.addField("'id'");
        idCrit.setOperation("=");
        idCrit.setValue(id);

        try {
          PostgresClient.getInstance(vertxContext.owner(), tenantId).delete(
              "sushi_settings", new Criterion(idCrit), deleteReply -> {
                if (deleteReply.failed()) {
                  logger.debug("Delete failed: " + deleteReply.cause().getMessage());
                  asyncResultHandler.handle(Future.succeededFuture(
                      DeleteSushisettingsByIdResponse.withPlainNotFound("Not found")));
                } else {
                  asyncResultHandler.handle(Future.succeededFuture(
                      DeleteSushisettingsByIdResponse.withNoContent()));
                }
              });
        } catch (Exception e) {
          logger.debug("Delete failed: " + e.getMessage());
          asyncResultHandler.handle(
              Future.succeededFuture(
                  DeleteSushisettingsByIdResponse.withPlainInternalServerError(
                      messages.getMessage(lang,
                          MessageConsts.InternalServerError))));
        }
      });
    } catch (Exception e) {
      asyncResultHandler.handle(
          Future.succeededFuture(
              DeleteSushisettingsByIdResponse.withPlainInternalServerError(
                  messages.getMessage(lang,
                      MessageConsts.InternalServerError))));
    }
  }

  @Override
  @Validate
  public void putSushisettingsById(String id, String lang, SushiSetting entity,
      Map<String, String> okapiHeaders, Handler<AsyncResult
      <Response>> asyncResultHandler,
      Context vertxContext) throws Exception {
    try {
      vertxContext.runOnContext(v -> {
        if (!id.equals(entity.getId())) {
          asyncResultHandler.handle(Future.succeededFuture(
              PutSushisettingsByIdResponse
                  .withPlainBadRequest("You cannot change the value of the id field")));
        } else {
          String tenantId = TenantTool.calculateTenantId(okapiHeaders.get(OKAPI_HEADER_TENANT));
          Criteria nameCrit = new Criteria();
          nameCrit.addField("'label'");
          nameCrit.setOperation("=");
          nameCrit.setValue(entity.getLabel());
          try {
            PostgresClient.getInstance(vertxContext.owner(), tenantId).get("sushi_settings",
                SushiSetting.class, new Criterion(nameCrit), true, false, getReply -> {
                  if (getReply.failed()) {
                    logger.debug("Error querying existing sushi settings: " + getReply.cause()
                        .getLocalizedMessage());
                    asyncResultHandler.handle(Future.succeededFuture(
                        PutSushisettingsByIdResponse.withPlainInternalServerError(
                            messages.getMessage(lang,
                                MessageConsts.InternalServerError))));
                  } else {
                    List<SushiSetting> sushiSettingList = (List<SushiSetting>) getReply.result()
                        .getResults();
                    if (sushiSettingList.size() > 0 && (!sushiSettingList.get(0).getId()
                        .equals(entity.getId()))) {
                      asyncResultHandler.handle(Future.succeededFuture(
                          PutSushisettingsByIdResponse.withPlainBadRequest(
                              "Label " + entity.getLabel() + " is already in use")));
                    } else {
                      Date createdDate = null;
                      Date now = new Date();
                      if (sushiSettingList.size() > 0) {
                        createdDate = sushiSettingList.get(0).getCreatedDate();
                      } else {
                        createdDate = now;
                      }
                      Criteria idCrit = new Criteria();
                      idCrit.addField("'id'");
                      idCrit.setOperation("=");
                      idCrit.setValue(id);
                      entity.setUpdatedDate(now);
                      entity.setCreatedDate(createdDate);
                      try {
                        PostgresClient.getInstance(vertxContext.owner(), tenantId).update(
                            "sushi_settings", entity, new Criterion(idCrit), true, putReply -> {
                              try {
                                if (putReply.failed()) {
                                  asyncResultHandler.handle(Future.succeededFuture(
                                      PutSushisettingsByIdResponse.withPlainInternalServerError(
                                          putReply.cause().getMessage())));
                                } else {
                                  asyncResultHandler.handle(Future.succeededFuture(
                                      PutSushisettingsByIdResponse.withNoContent()));
                                }
                              } catch (Exception e) {
                                asyncResultHandler.handle(Future.succeededFuture(
                                    PutSushisettingsByIdResponse.withPlainInternalServerError(
                                        messages.getMessage(lang,
                                            MessageConsts.InternalServerError))));
                              }
                            });
                      } catch (Exception e) {
                        asyncResultHandler.handle(Future.succeededFuture(
                            PutSushisettingsByIdResponse.withPlainInternalServerError(
                                messages.getMessage(lang,
                                    MessageConsts.InternalServerError))));
                      }
                    }
                  }
                });
          } catch (Exception e) {
            logger.debug(e.getLocalizedMessage());
            asyncResultHandler.handle(Future.succeededFuture(
                PutSushisettingsByIdResponse.withPlainInternalServerError(
                    messages.getMessage(lang, MessageConsts.InternalServerError))));
          }
        }
      });
    } catch (Exception e) {
      logger.debug(e.getLocalizedMessage());
      asyncResultHandler.handle(Future.succeededFuture(
          PutSushisettingsByIdResponse.withPlainInternalServerError(
              messages.getMessage(lang, MessageConsts.InternalServerError))));
    }
  }
}
