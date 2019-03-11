package org.folio.rest.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
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
import java.util.stream.Collectors;
import javax.ws.rs.core.Response;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.rest.annotations.Validate;
import org.folio.rest.jaxrs.model.AggregatorSetting;
import org.folio.rest.jaxrs.model.AggregatorSettings;
import org.folio.rest.jaxrs.model.AggregatorSettingsGetOrder;
import org.folio.rest.jaxrs.model.SushiCredentials;
import org.folio.rest.jaxrs.model.UsageDataProvider;
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

public class AggregatorSettingsAPI implements org.folio.rest.jaxrs.resource.AggregatorSettings {

  private static final String ID_FIELD = "_id";
  private static final String LABEL_FIELD = "'label'";
  private static final String SCHEMA_PATH = "/schemas/aggregatorSettingsData.json";
  private static final String TABLE_NAME_AGGREGATOR_SETTINGS = "aggregator_settings";
  private static final String LOCATION_PREFIX = "/aggregator-settings/";

  private final Messages messages = Messages.getInstance();
  private final Logger logger = LoggerFactory.getLogger(AggregatorSettingsAPI.class);

  public AggregatorSettingsAPI(Vertx vertx, String tenantId) {
    PostgresClient.getInstance(vertx, tenantId).setIdField(ID_FIELD);
  }

  private CQLWrapper getCQL(String query, int limit, int offset) throws FieldException {
    CQL2PgJSON cql2pgJson =
        new CQL2PgJSON(Arrays.asList(TABLE_NAME_AGGREGATOR_SETTINGS + ".jsonb"));
    return new CQLWrapper(cql2pgJson, query)
        .setLimit(new Limit(limit))
        .setOffset(new Offset(offset));
  }

  @Override
  @Validate
  public void getAggregatorSettings(
      String query,
      String orderBy,
      AggregatorSettingsGetOrder order,
      int offset,
      int limit,
      String lang,
      Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {
    logger.debug("Getting aggregator settings");
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
                      TABLE_NAME_AGGREGATOR_SETTINGS,
                      AggregatorSetting.class,
                      fieldList,
                      cql,
                      true,
                      false,
                      reply -> {
                        try {
                          if (reply.succeeded()) {
                            AggregatorSettings aggregatorSettingsDataCollection =
                                new AggregatorSettings();
                            List<AggregatorSetting> aggregatorSettings =
                                (List<AggregatorSetting>) reply.result().getResults();
                            aggregatorSettingsDataCollection.setAggregatorSettings(
                                aggregatorSettings);
                            aggregatorSettingsDataCollection.setTotalRecords(
                                reply.result().getResultInfo().getTotalRecords());
                            asyncResultHandler.handle(
                                Future.succeededFuture(
                                    GetAggregatorSettingsResponse.respond200WithApplicationJson(
                                        aggregatorSettingsDataCollection)));
                          } else {
                            asyncResultHandler.handle(
                                Future.succeededFuture(
                                    GetAggregatorSettingsResponse.respond500WithTextPlain(
                                        reply.cause().getMessage())));
                          }
                        } catch (Exception e) {
                          logger.debug(e.getLocalizedMessage());

                          asyncResultHandler.handle(
                              Future.succeededFuture(
                                  GetAggregatorSettingsResponse.respond500WithTextPlain(
                                      reply.cause().getMessage())));
                        }
                      });
            } catch (IllegalStateException e) {
              logger.debug("IllegalStateException: " + e.getLocalizedMessage());
              asyncResultHandler.handle(
                  Future.succeededFuture(
                      GetAggregatorSettingsResponse.respond400WithTextPlain(
                          "CQL Illegal State Error for '"
                              + query
                              + "': "
                              + e.getLocalizedMessage())));
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
                        GetAggregatorSettingsResponse.respond400WithTextPlain(
                            "CQL Parsing Error for '"
                                + query
                                + "': "
                                + cause.getLocalizedMessage())));
              } else {
                asyncResultHandler.handle(
                    io.vertx.core.Future.succeededFuture(
                        GetAggregatorSettingsResponse.respond500WithTextPlain(
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
                GetAggregatorSettingsResponse.respond400WithTextPlain(
                    "CQL Parsing Error for '" + query + "': " + e.getLocalizedMessage())));
      } else {
        asyncResultHandler.handle(
            io.vertx.core.Future.succeededFuture(
                GetAggregatorSettingsResponse.respond500WithTextPlain(
                    messages.getMessage(lang, MessageConsts.InternalServerError))));
      }
    }
  }

  @Override
  @Validate
  public void postAggregatorSettings(
      String lang,
      AggregatorSetting entity,
      Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {
    try {
      vertxContext.runOnContext(
          v -> {
            String tenantId = TenantTool.calculateTenantId(okapiHeaders.get(XOkapiHeaders.TENANT));
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
                        TABLE_NAME_AGGREGATOR_SETTINGS,
                        AggregatorSetting.class,
                        crit,
                        true,
                        getReply -> {
                          logger.debug(
                              "Attempting to get existing aggregator settings of same id and/or label");
                          if (getReply.failed()) {
                            logger.debug(
                                "Attempt to get aggregator settings failed: "
                                    + getReply.cause().getMessage());
                            asyncResultHandler.handle(
                                Future.succeededFuture(
                                    PostAggregatorSettingsResponse.respond500WithTextPlain(
                                        getReply.cause().getMessage())));
                          } else {
                            List<AggregatorSetting> aggregatorList =
                                (List<AggregatorSetting>) getReply.result().getResults();
                            if (aggregatorList.size() > 0) {
                              logger.debug("Aggregator setting with this id/label already exists");
                              asyncResultHandler.handle(
                                  Future.succeededFuture(
                                      PostAggregatorSettingsResponse.respond422WithApplicationJson(
                                          ValidationHelper.createValidationErrorMessage(
                                              "'label'",
                                              entity.getLabel(),
                                              "Aggregator setting with this id/label already exists"))));
                            } else {
                              PostgresClient postgresClient =
                                  PostgresClient.getInstance(vertxContext.owner(), tenantId);
                              postgresClient.save(
                                  TABLE_NAME_AGGREGATOR_SETTINGS,
                                  entity.getId(),
                                  entity,
                                  reply -> {
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
                                                    .respond201WithApplicationJson(
                                                        entity,
                                                        PostAggregatorSettingsResponse
                                                            .headersFor201()
                                                            .withLocation(
                                                                LOCATION_PREFIX
                                                                    + entity.getId()))));
                                      } else {
                                        asyncResultHandler.handle(
                                            Future.succeededFuture(
                                                PostAggregatorSettingsResponse
                                                    .respond500WithTextPlain(
                                                        reply.cause().toString())));
                                      }
                                    } catch (Exception e) {
                                      asyncResultHandler.handle(
                                          io.vertx.core.Future.succeededFuture(
                                              PostAggregatorSettingsResponse
                                                  .respond500WithTextPlain(e.getMessage())));
                                    }
                                  });
                            }
                          }
                        });
              } catch (Exception e) {
                logger.error(e.getLocalizedMessage(), e);
                asyncResultHandler.handle(
                    Future.succeededFuture(
                        PostAggregatorSettingsResponse.respond500WithTextPlain(
                            messages.getMessage(lang, MessageConsts.InternalServerError))));
              }
            } catch (Exception e) {
              asyncResultHandler.handle(
                  Future.succeededFuture(
                      PostAggregatorSettingsResponse.respond500WithTextPlain(
                          messages.getMessage(lang, MessageConsts.InternalServerError))));
            }
          });
    } catch (Exception e) {
      asyncResultHandler.handle(
          Future.succeededFuture(
              PostAggregatorSettingsResponse.respond500WithTextPlain(
                  messages.getMessage(lang, MessageConsts.InternalServerError))));
    }
  }

  @Override
  @Validate
  public void getAggregatorSettingsById(
      String id,
      String lang,
      Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {
    try {
      vertxContext.runOnContext(
          v -> {
            String tenantId = TenantTool.calculateTenantId(okapiHeaders.get(XOkapiHeaders.TENANT));
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
                      TABLE_NAME_AGGREGATOR_SETTINGS,
                      AggregatorSetting.class,
                      criterion,
                      true,
                      false,
                      getReply -> {
                        if (getReply.failed()) {
                          asyncResultHandler.handle(
                              Future.succeededFuture(
                                  GetAggregatorSettingsByIdResponse.respond500WithTextPlain(
                                      messages.getMessage(
                                          lang, MessageConsts.InternalServerError))));
                        } else {
                          List<AggregatorSetting> aggregatorSettingList =
                              (List<AggregatorSetting>) getReply.result().getResults();
                          if (aggregatorSettingList.size() < 1) {
                            asyncResultHandler.handle(
                                Future.succeededFuture(
                                    GetAggregatorSettingsByIdResponse.respond404WithTextPlain(
                                        "Aggregator setting"
                                            + messages.getMessage(
                                                lang, MessageConsts.ObjectDoesNotExist))));
                          } else if (aggregatorSettingList.size() > 1) {
                            logger.debug("Multiple aggregator settings found with the same id");
                            asyncResultHandler.handle(
                                Future.succeededFuture(
                                    GetAggregatorSettingsByIdResponse.respond500WithTextPlain(
                                        messages.getMessage(
                                            lang, MessageConsts.InternalServerError))));
                          } else {
                            asyncResultHandler.handle(
                                Future.succeededFuture(
                                    GetAggregatorSettingsByIdResponse.respond200WithApplicationJson(
                                        aggregatorSettingList.get(0))));
                          }
                        }
                      });
            } catch (Exception e) {
              logger.debug("Error occurred: " + e.getMessage());
              asyncResultHandler.handle(
                  Future.succeededFuture(
                      GetAggregatorSettingsByIdResponse.respond500WithTextPlain(
                          messages.getMessage(lang, MessageConsts.InternalServerError))));
            }
          });
    } catch (Exception e) {
      asyncResultHandler.handle(
          Future.succeededFuture(
              GetAggregatorSettingsByIdResponse.respond500WithTextPlain(
                  messages.getMessage(lang, MessageConsts.InternalServerError))));
    }
  }

  @Override
  @Validate
  public void deleteAggregatorSettingsById(
      String id,
      String lang,
      Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {
    try {
      vertxContext.runOnContext(
          v -> {
            String tenantId = TenantTool.calculateTenantId(okapiHeaders.get(XOkapiHeaders.TENANT));
            try {
              PostgresClient.getInstance(vertxContext.owner(), tenantId)
                  .delete(
                      TABLE_NAME_AGGREGATOR_SETTINGS,
                      id,
                      deleteReply -> {
                        if (deleteReply.failed()) {
                          logger.debug("Delete failed: " + deleteReply.cause().getMessage());
                          asyncResultHandler.handle(
                              Future.succeededFuture(
                                  DeleteAggregatorSettingsByIdResponse.respond400WithTextPlain(
                                      "Delete failed. Maybe aggregator setting is referenced by usage data provider?")));
                        } else {
                          asyncResultHandler.handle(
                              Future.succeededFuture(
                                  DeleteAggregatorSettingsByIdResponse.respond204()));
                        }
                      });
            } catch (Exception e) {
              logger.debug("Delete failed: " + e.getMessage());
              asyncResultHandler.handle(
                  Future.succeededFuture(
                      DeleteAggregatorSettingsByIdResponse.respond500WithTextPlain(
                          messages.getMessage(lang, MessageConsts.InternalServerError))));
            }
          });
    } catch (Exception e) {
      asyncResultHandler.handle(
          Future.succeededFuture(
              DeleteAggregatorSettingsByIdResponse.respond500WithTextPlain(
                  messages.getMessage(lang, MessageConsts.InternalServerError))));
    }
  }

  @Override
  @Validate
  public void putAggregatorSettingsById(
      String id,
      String lang,
      AggregatorSetting entity,
      Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {
    try {
      vertxContext.runOnContext(
          v -> {
            if (!id.equals(entity.getId())) {
              asyncResultHandler.handle(
                  Future.succeededFuture(
                      PutAggregatorSettingsByIdResponse.respond400WithTextPlain(
                          "You cannot change the value of the id field")));
            } else {
              String tenantId =
                  TenantTool.calculateTenantId(okapiHeaders.get(XOkapiHeaders.TENANT));
              Criteria nameCrit = new Criteria();
              nameCrit.addField(LABEL_FIELD);
              nameCrit.setOperation("=");
              nameCrit.setValue(entity.getLabel());
              try {
                PostgresClient.getInstance(vertxContext.owner(), tenantId)
                    .get(
                        TABLE_NAME_AGGREGATOR_SETTINGS,
                        AggregatorSetting.class,
                        new Criterion(nameCrit),
                        true,
                        false,
                        getReply -> {
                          if (getReply.failed()) {
                            logger.debug(
                                "Error querying existing aggregator settings: "
                                    + getReply.cause().getLocalizedMessage());
                            asyncResultHandler.handle(
                                Future.succeededFuture(
                                    PutAggregatorSettingsByIdResponse.respond500WithTextPlain(
                                        messages.getMessage(
                                            lang, MessageConsts.InternalServerError))));
                          } else {
                            List<AggregatorSetting> aggregatorSettingList =
                                (List<AggregatorSetting>) getReply.result().getResults();
                            if (aggregatorSettingList.size() > 0
                                && (!aggregatorSettingList.get(0).getId().equals(entity.getId()))) {
                              asyncResultHandler.handle(
                                  Future.succeededFuture(
                                      PutAggregatorSettingsByIdResponse.respond400WithTextPlain(
                                          "Label " + entity.getLabel() + " is already in use")));
                            } else {
                              try {
                                PostgresClient.getInstance(vertxContext.owner(), tenantId)
                                    .update(
                                        TABLE_NAME_AGGREGATOR_SETTINGS,
                                        entity,
                                        id,
                                        putReply -> {
                                          try {
                                            if (putReply.failed()) {
                                              asyncResultHandler.handle(
                                                  Future.succeededFuture(
                                                      PutAggregatorSettingsByIdResponse
                                                          .respond500WithTextPlain(
                                                              putReply.cause().getMessage())));
                                            } else {
                                              asyncResultHandler.handle(
                                                  Future.succeededFuture(
                                                      PutAggregatorSettingsByIdResponse
                                                          .respond204()));
                                            }
                                          } catch (Exception e) {
                                            asyncResultHandler.handle(
                                                Future.succeededFuture(
                                                    PutAggregatorSettingsByIdResponse
                                                        .respond500WithTextPlain(
                                                            messages.getMessage(
                                                                lang,
                                                                MessageConsts
                                                                    .InternalServerError))));
                                          }
                                        });
                              } catch (Exception e) {
                                asyncResultHandler.handle(
                                    Future.succeededFuture(
                                        PutAggregatorSettingsByIdResponse.respond500WithTextPlain(
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
                        PutAggregatorSettingsByIdResponse.respond500WithTextPlain(
                            messages.getMessage(lang, MessageConsts.InternalServerError))));
              }
            }
          });
    } catch (Exception e) {
      logger.debug(e.getLocalizedMessage());
      asyncResultHandler.handle(
          Future.succeededFuture(
              PutAggregatorSettingsByIdResponse.respond500WithTextPlain(
                  messages.getMessage(lang, MessageConsts.InternalServerError))));
    }
  }

  @Override
  public void getAggregatorSettingsExportcredentialsById(
      String id,
      Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {

    String where =
        String.format(" WHERE (jsonb->'harvestingConfig'->'aggregator'->>'id' = '%s')", id);

    PostgresClient.getInstance(vertxContext.owner(), okapiHeaders.get(XOkapiHeaders.TENANT))
        .get(
            "usage_data_providers",
            UsageDataProvider.class,
            where,
            true,
            true,
            ar -> {
              if (ar.succeeded()) {
                List<UsageDataProvider> providerList = ar.result().getResults();
                List<SushiCredentials> credentialsList =
                    providerList.stream()
                        .map(UsageDataProvider::getSushiCredentials)
                        .collect(Collectors.toList());
                try {
                  CsvMapper csvMapper = new CsvMapper();
                  String resultString =
                      csvMapper
                          .writerFor(SushiCredentials.class)
                          .with(csvMapper.schemaFor(SushiCredentials.class).withHeader())
                          .forType(List.class)
                          .writeValueAsString(credentialsList);
                  asyncResultHandler.handle(
                      Future.succeededFuture(
                          GetAggregatorSettingsExportcredentialsByIdResponse.respond200WithTextCsv(
                              resultString)));
                } catch (JsonProcessingException e) {
                  asyncResultHandler.handle(
                      Future.succeededFuture(
                          Response.status(500).entity("Error creating CSV").build()));
                }
              } else {
                asyncResultHandler.handle(Future.succeededFuture(Response.status(500).build()));
              }
            });
  }
}
