package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import javax.ws.rs.core.Response;
import org.folio.rest.annotations.Validate;
import org.folio.rest.jaxrs.model.UdProvidersDataCollection;
import org.folio.rest.jaxrs.model.UsageDataProvider;
import org.folio.rest.jaxrs.resource.UsageDataProvidersResource;
import org.folio.rest.persist.Criteria.Criteria;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.Criteria.Limit;
import org.folio.rest.persist.Criteria.Offset;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.cql.CQLWrapper;
import org.folio.rest.tools.client.HttpClientFactory;
import org.folio.rest.tools.client.interfaces.HttpClientInterface;
import org.folio.rest.tools.messages.MessageConsts;
import org.folio.rest.tools.messages.Messages;
import org.folio.rest.tools.utils.OutStream;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.rest.tools.utils.ValidationHelper;
import org.folio.rest.util.Constants;
import org.z3950.zing.cql.cql2pgjson.CQL2PgJSON;
import org.z3950.zing.cql.cql2pgjson.FieldException;

public class UsageDataProvidersAPI implements UsageDataProvidersResource {

  private static final String ID_FIELD = "_id";
  private static final String LABEL_FIELD = "'label'";
  private static final String SCHEMA_PATH = "/schemas/udProvidersData.json";
  private static final String TABLE_NAME_SUSHI_SETTINGS = "usage_data_providers";

  private final Messages messages = Messages.getInstance();
  private final Logger logger = LoggerFactory.getLogger(UsageDataProvidersAPI.class);

  public UsageDataProvidersAPI(Vertx vertx, String tenantId) {
    PostgresClient.getInstance(vertx, tenantId).setIdField(ID_FIELD);
  }

  private CQLWrapper getCQL(String query, int limit, int offset) throws FieldException {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON(Arrays.asList(TABLE_NAME_SUSHI_SETTINGS + ".jsonb"));
    return new CQLWrapper(cql2pgJson, query).setLimit(new Limit(limit))
        .setOffset(new Offset(offset));
  }

  @Override
  @Validate
  public void getUsageDataProviders(String query, String orderBy, Order order, int offset,
      int limit, String lang, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    logger.debug("Getting usage data providers");
    try {
      CQLWrapper cql = getCQL(query, limit, offset);
      vertxContext.runOnContext(v -> {
        String tenantId = TenantTool
            .calculateTenantId(okapiHeaders.get(Constants.OKAPI_HEADER_TENANT));
        logger.debug("Headers present are: " + okapiHeaders.keySet().toString());
        logger.debug("tenantId = " + tenantId);
        String[] fieldList = {"*"};
        try {
          PostgresClient.getInstance(vertxContext.owner(), tenantId).get(TABLE_NAME_SUSHI_SETTINGS,
              UsageDataProvider.class, fieldList, cql, true, false, reply -> {
                try {
                  if (reply.succeeded()) {
                    UdProvidersDataCollection udProvidersDataCollection = new UdProvidersDataCollection();
                    List<UsageDataProvider> dataProviders = (List<UsageDataProvider>) reply
                        .result().getResults();
                    udProvidersDataCollection.setUsageDataProviders(dataProviders);
                    udProvidersDataCollection
                        .setTotalRecords(reply.result().getResultInfo().getTotalRecords());
                    asyncResultHandler.handle(Future.succeededFuture(
                        GetUsageDataProvidersResponse.withJsonOK(udProvidersDataCollection)
                    ));
                  } else {
                    asyncResultHandler.handle(Future.succeededFuture(
                        GetUsageDataProvidersResponse.withPlainInternalServerError(
                            reply.cause().getMessage()
                        )
                    ));
                  }
                } catch (Exception e) {
                  logger.debug(e.getLocalizedMessage());

                  asyncResultHandler.handle(Future.succeededFuture(
                      GetUsageDataProvidersResponse.withPlainInternalServerError(
                          reply.cause().getMessage()
                      )
                  ));
                }
              });
        } catch (IllegalStateException e) {
          logger.debug("IllegalStateException: " + e.getLocalizedMessage());
          asyncResultHandler
              .handle(Future.succeededFuture(GetUsageDataProvidersResponse.withPlainBadRequest(
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
                .handle(Future.succeededFuture(GetUsageDataProvidersResponse.withPlainBadRequest(
                    "CQL Parsing Error for '" + "" + "': " + cause.getLocalizedMessage())));
          } else {
            asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
                GetUsageDataProvidersResponse.withPlainInternalServerError(
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
            .handle(Future.succeededFuture(GetUsageDataProvidersResponse.withPlainBadRequest(
                "CQL Parsing Error for '" + "" + "': " + e.getLocalizedMessage())));
      } else {
        asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
            GetUsageDataProvidersResponse.withPlainInternalServerError(
                messages.getMessage(lang,
                    MessageConsts.InternalServerError))));
      }
    }
  }

  @Override
  @Validate
  public void postUsageDataProviders(String lang, UsageDataProvider entity,
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
                TenantTool.calculateTenantId(tenantId)).get(TABLE_NAME_SUSHI_SETTINGS,
                UsageDataProvider.class, crit, true, getReply -> {
                  logger.debug("Attempting to get existing sushi settings of same id and/or label");
                  if (getReply.failed()) {
                    logger.debug("Attempt to get sushi settings failed: " +
                        getReply.cause().getMessage());
                    asyncResultHandler.handle(Future.succeededFuture(
                        PostUsageDataProvidersResponse.withPlainInternalServerError(
                            getReply.cause().getMessage())));
                  } else {
                    List<UsageDataProvider> dataProviders = (List<UsageDataProvider>) getReply
                        .result()
                        .getResults();
                    if (dataProviders.size() > 0) {
                      logger.debug("Usage data provider with this label/id already exists");
                      asyncResultHandler.handle(Future.succeededFuture(
                          PostUsageDataProvidersResponse.withJsonUnprocessableEntity(
                              ValidationHelper.createValidationErrorMessage(
                                  "'label'", entity.getLabel(),
                                  "Usage data provider with this label/id already exists"))));
                    } else {
                      fetchVendorName(entity, okapiHeaders)
                          .thenAccept(vendorName -> {
                            entity.setVendorName(vendorName);
                            fetchAggregatorName(entity, okapiHeaders)
                                .thenAccept(aggregatorName -> {
                                  if (entity.getAggregator() != null) {
                                    entity.getAggregator().setName(aggregatorName);
                                  }
                                  PostgresClient postgresClient = PostgresClient
                                      .getInstance(vertxContext.owner(), tenantId);
                                  postgresClient
                                      .save(TABLE_NAME_SUSHI_SETTINGS, entity.getId(), entity,
                                          reply -> {
                                            try {
                                              if (reply.succeeded()) {
                                                logger.debug("save successful");
                                                final UsageDataProvider udp = entity;
                                                udp.setId(entity.getId());
                                                OutStream stream = new OutStream();
                                                stream.setData(udp);
                                                asyncResultHandler.handle(
                                                    Future.succeededFuture(
                                                        PostUsageDataProvidersResponse
                                                            .withJsonCreated(
                                                                reply.result(), stream)));
                                              } else {
                                                asyncResultHandler.handle(Future.succeededFuture(
                                                    PostUsageDataProvidersResponse
                                                        .withPlainInternalServerError(
                                                            reply.cause().toString())));
                                              }
                                            } catch (Exception e) {
                                              asyncResultHandler
                                                  .handle(io.vertx.core.Future.succeededFuture(
                                                      PostUsageDataProvidersResponse
                                                          .withPlainInternalServerError(
                                                              e.getMessage())));
                                            }
                                          });
                                })
                                .exceptionally(throwable -> {
                                  logger.error("Cannot fetch aggregator name for id: " + entity
                                      .getAggregator().getId(), throwable);
                                  asyncResultHandler.handle(Future.succeededFuture(
                                      PostUsageDataProvidersResponse.withPlainBadRequest(
                                          "Error while fetching aggregator name. " + throwable
                                              .getMessage())
                                  ));
                                  return null;
                                });
                          })
                          .exceptionally(o -> {
                            logger
                                .error("Cannot fetch vendor name for id: " + entity.getVendorId());
                            asyncResultHandler.handle(Future.succeededFuture(
                                PostUsageDataProvidersResponse
                                    .withPlainBadRequest("Error while fetching vendor name.")));
                            return null;
                          });
                    }
                  }
                });
          } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            asyncResultHandler.handle(Future.succeededFuture(
                PostUsageDataProvidersResponse.withPlainInternalServerError(
                    messages.getMessage(lang, MessageConsts.InternalServerError))));
          }
        } catch (Exception e) {
          asyncResultHandler.handle(Future.succeededFuture(
              PostUsageDataProvidersResponse.withPlainInternalServerError(
                  messages.getMessage(lang, MessageConsts.InternalServerError))));
        }
      });
    } catch (Exception e) {
      asyncResultHandler.handle(Future.succeededFuture(
          PostUsageDataProvidersResponse.withPlainInternalServerError(
              messages.getMessage(lang, MessageConsts.InternalServerError))));
    }
  }

  @Override
  @Validate
  public void getUsageDataProvidersById(String id, String lang, Map<String, String> okapiHeaders,
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
              .get(TABLE_NAME_SUSHI_SETTINGS, UsageDataProvider.class, criterion,
                  true, false, getReply -> {
                    if (getReply.failed()) {
                      asyncResultHandler.handle(Future.succeededFuture(
                          GetUsageDataProvidersByIdResponse.withPlainInternalServerError(
                              messages.getMessage(lang, MessageConsts.InternalServerError))));
                    } else {
                      List<UsageDataProvider> dataProviders = (List<UsageDataProvider>) getReply
                          .result()
                          .getResults();
                      if (dataProviders.size() < 1) {
                        asyncResultHandler.handle(Future.succeededFuture(
                            GetUsageDataProvidersByIdResponse
                                .withPlainNotFound("Usage data provider " +
                                    messages.getMessage(lang,
                                        MessageConsts.ObjectDoesNotExist))));
                      } else if (dataProviders.size() > 1) {
                        logger.debug("Multiple usage data providers found with the same id");
                        asyncResultHandler.handle(Future.succeededFuture(
                            GetUsageDataProvidersByIdResponse.withPlainInternalServerError(
                                messages.getMessage(lang,
                                    MessageConsts.InternalServerError))));
                      } else {
                        asyncResultHandler.handle(Future.succeededFuture(
                            GetUsageDataProvidersByIdResponse.withJsonOK(dataProviders.get(0))));
                      }
                    }
                  });
        } catch (Exception e) {
          logger.debug("Error occurred: " + e.getMessage());
          asyncResultHandler.handle(Future.succeededFuture(
              GetUsageDataProvidersByIdResponse.withPlainInternalServerError(messages.getMessage(
                  lang, MessageConsts.InternalServerError))));
        }
      });
    } catch (Exception e) {
      asyncResultHandler.handle(Future.succeededFuture(
          GetUsageDataProvidersByIdResponse.withPlainInternalServerError(messages.getMessage(
              lang, MessageConsts.InternalServerError))));
    }
  }

  @Override
  @Validate
  public void deleteUsageDataProvidersById(String id, String lang, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) throws Exception {
    try {
      vertxContext.runOnContext(v -> {
        String tenantId = TenantTool
            .calculateTenantId(okapiHeaders.get(Constants.OKAPI_HEADER_TENANT));
        Criteria idCrit = new Criteria()
            .addField(ID_FIELD)
            .setJSONB(false)
            .setOperation("=")
            .setValue("'" + id + "'");
        try {
          PostgresClient.getInstance(vertxContext.owner(), tenantId).delete(
              TABLE_NAME_SUSHI_SETTINGS, new Criterion(idCrit), deleteReply -> {
                if (deleteReply.failed()) {
                  logger.debug("Delete failed: " + deleteReply.cause().getMessage());
                  asyncResultHandler.handle(Future.succeededFuture(
                      DeleteUsageDataProvidersByIdResponse.withPlainNotFound("Not found")));
                } else {
                  asyncResultHandler.handle(Future.succeededFuture(
                      DeleteUsageDataProvidersByIdResponse.withNoContent()));
                }
              });
        } catch (Exception e) {
          logger.debug("Delete failed: " + e.getMessage());
          asyncResultHandler.handle(
              Future.succeededFuture(
                  DeleteUsageDataProvidersByIdResponse.withPlainInternalServerError(
                      messages.getMessage(lang,
                          MessageConsts.InternalServerError))));
        }
      });
    } catch (Exception e) {
      asyncResultHandler.handle(
          Future.succeededFuture(
              DeleteUsageDataProvidersByIdResponse.withPlainInternalServerError(
                  messages.getMessage(lang,
                      MessageConsts.InternalServerError))));
    }
  }

  @Override
  @Validate
  public void putUsageDataProvidersById(String id, String lang, UsageDataProvider entity,
      Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) throws Exception {
    try {
      vertxContext.runOnContext(v -> {
        if (!id.equals(entity.getId())) {
          asyncResultHandler.handle(Future.succeededFuture(
              PutUsageDataProvidersByIdResponse
                  .withPlainBadRequest("You cannot change the value of the id field")));
        } else {
          String tenantId = TenantTool
              .calculateTenantId(okapiHeaders.get(Constants.OKAPI_HEADER_TENANT));
          Criteria nameCrit = new Criteria();
          nameCrit.addField(LABEL_FIELD);
          nameCrit.setOperation("=");
          nameCrit.setValue("'" + entity.getLabel() + "'");
          try {
            PostgresClient.getInstance(vertxContext.owner(), tenantId)
                .get(TABLE_NAME_SUSHI_SETTINGS,
                    UsageDataProvider.class, new Criterion(nameCrit), true, false, getReply -> {
                      if (getReply.failed()) {
                        logger.debug("Error querying existing sushi settings: " + getReply.cause()
                            .getLocalizedMessage());
                        asyncResultHandler.handle(Future.succeededFuture(
                            PutUsageDataProvidersByIdResponse.withPlainInternalServerError(
                                messages.getMessage(lang,
                                    MessageConsts.InternalServerError))));
                      } else {
                        List<UsageDataProvider> dataProviders = (List<UsageDataProvider>) getReply
                            .result()
                            .getResults();
                        if (dataProviders.size() > 0 && (!dataProviders.get(0).getId()
                            .equals(entity.getId()))) {
                          asyncResultHandler.handle(Future.succeededFuture(
                              PutUsageDataProvidersByIdResponse.withPlainBadRequest(
                                  "Label " + entity.getLabel() + " is already in use")));
                        } else {
                          Date createdDate = null;
                          Date now = new Date();
                          if (dataProviders.size() > 0) {
                            createdDate = dataProviders.get(0).getCreatedDate();
                          } else {
                            createdDate = now;
                          }
                          Criteria idCrit = new Criteria()
                              .addField(ID_FIELD)
                              .setJSONB(false)
                              .setOperation("=")
                              .setValue("'" + id + "'");
                          entity.setUpdatedDate(now);
                          entity.setCreatedDate(createdDate);
                          try {

                            fetchVendorName(entity, okapiHeaders)
                                .thenAccept(vendorName -> {
                                  entity.setVendorName(vendorName);
                                  fetchAggregatorName(entity, okapiHeaders)
                                      .thenAccept(aggregatorName -> {
                                        if (entity.getAggregator() != null) {
                                          entity.getAggregator().setName(aggregatorName);
                                        }
                                        PostgresClient.getInstance(vertxContext.owner(), tenantId)
                                            .update(
                                                TABLE_NAME_SUSHI_SETTINGS, entity,
                                                new Criterion(idCrit), true,
                                                putReply -> {
                                                  try {
                                                    if (putReply.failed()) {
                                                      asyncResultHandler
                                                          .handle(Future.succeededFuture(
                                                              PutUsageDataProvidersByIdResponse
                                                                  .withPlainInternalServerError(
                                                                      putReply.cause()
                                                                          .getMessage())));
                                                    } else {
                                                      asyncResultHandler
                                                          .handle(Future.succeededFuture(
                                                              PutUsageDataProvidersByIdResponse
                                                                  .withNoContent()));
                                                    }
                                                  } catch (Exception e) {
                                                    asyncResultHandler
                                                        .handle(Future.succeededFuture(
                                                            PutUsageDataProvidersByIdResponse
                                                                .withPlainInternalServerError(
                                                                    messages.getMessage(lang,
                                                                        MessageConsts.InternalServerError))));
                                                  }
                                                });
                                      })
                                      .exceptionally(throwable -> {
                                        logger
                                            .error("Cannot fetch aggregator name for id: " + entity
                                                .getAggregator().getId(), throwable);
                                        asyncResultHandler.handle(Future.succeededFuture(
                                            PostUsageDataProvidersResponse.withPlainBadRequest(
                                                "Error while fetching aggregator name. " + throwable
                                                    .getMessage())
                                        ));
                                        return null;
                                      });
                                })
                                .exceptionally(o -> {
                                  logger
                                      .error("Cannot fetch vendor name for id: " + entity
                                          .getVendorId());
                                  asyncResultHandler.handle(Future.succeededFuture(
                                      PostUsageDataProvidersResponse
                                          .withPlainBadRequest(
                                              "Error while fetching vendor name.")));
                                  return null;
                                });
                          } catch (Exception e) {
                            asyncResultHandler.handle(Future.succeededFuture(
                                PutUsageDataProvidersByIdResponse.withPlainInternalServerError(
                                    messages.getMessage(lang,
                                        MessageConsts.InternalServerError))));
                          }
                        }
                      }
                    });
          } catch (Exception e) {
            logger.debug(e.getLocalizedMessage());
            asyncResultHandler.handle(Future.succeededFuture(
                PutUsageDataProvidersByIdResponse.withPlainInternalServerError(
                    messages.getMessage(lang, MessageConsts.InternalServerError))));
          }
        }
      });
    } catch (Exception e) {
      logger.debug(e.getLocalizedMessage());
      asyncResultHandler.handle(Future.succeededFuture(
          PutUsageDataProvidersByIdResponse.withPlainInternalServerError(
              messages.getMessage(lang, MessageConsts.InternalServerError))));
    }
  }

  private String getOkapiUrl(Map<String, String> headers) {
    return "http://localhost:9130";

    /*if (headers.containsKey("x-okapi-url")) {
      return headers.get("x-okapi-url");
    }
    return "";*/
  }

  private String getOkapiTenant(Map<String, String> headers) {
    if (headers.containsKey("x-okapi-tenant")) {
      return headers.get("x-okapi-tenant");
    }
    return "";
  }

  private CompletableFuture<String> fetchVendorName(
      UsageDataProvider entity,
      Map<String, String> okapiHeaders) {
    if (entity.getVendorId() == null) {
      CompletableFuture<String> res = new CompletableFuture<>();
      res.complete("");
      return res;
    } else {
      HttpClientInterface httpClient = HttpClientFactory
          .getHttpClient(getOkapiUrl(okapiHeaders), getOkapiTenant(okapiHeaders));
      httpClient.setDefaultHeaders(okapiHeaders);
      try {
        String vendorId = entity.getVendorId();
        String vendorUrl = "/vendor/" + vendorId;
        CompletableFuture<org.folio.rest.tools.client.Response> response = httpClient
            .request(vendorUrl);
        return response.thenApply(vendorResponse -> {
          if (org.folio.rest.tools.client.Response
              .isSuccess(vendorResponse.getCode())) {
            JsonObject json = vendorResponse.getBody();
            return json.getString("name");
          } else {
            throw new RuntimeException("Cannot get vendor for id: " + entity.getVendorId());
          }
        });
      } catch (Exception e) {
        throw new RuntimeException(
            "Cannot get vendor for id: " + entity.getVendorId() + ": " + e.getMessage());
      }
    }

  }

  private CompletableFuture<String> fetchAggregatorName(
      UsageDataProvider entity,
      Map<String, String> okapiHeaders) {
    if (entity.getAggregator() == null || entity.getAggregator().getId() == null) {
      CompletableFuture<String> res = new CompletableFuture<>();
      res.complete("");
      return res;
    } else {
      HttpClientInterface httpClient = HttpClientFactory
          .getHttpClient(getOkapiUrl(okapiHeaders), getOkapiTenant(okapiHeaders));
      httpClient.setDefaultHeaders(okapiHeaders);
      try {
        String aggregatorId = entity.getAggregator().getId();
        String aggregatorUrl = "/aggregator-settings/" + aggregatorId;
        CompletableFuture<org.folio.rest.tools.client.Response> response = httpClient
            .request(aggregatorUrl);
        return response.thenApply(vendorResponse -> {
          if (org.folio.rest.tools.client.Response
              .isSuccess(vendorResponse.getCode())) {
            JsonObject json = vendorResponse.getBody();
            return json.getString("label");
          } else {
            throw new RuntimeException(
                "Cannot get aggregator for id: " + entity.getAggregator().getId());
          }
        });
      } catch (Exception e) {
        throw new RuntimeException(
            "Cannot get aggregator for id: " + entity.getAggregator().getId() + ": " + e
                .getMessage());
      }
    }

  }

}
