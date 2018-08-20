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
import org.folio.rest.jaxrs.model.CounterReport;
import org.folio.rest.jaxrs.model.CounterReportDataDataCollection;
import org.folio.rest.jaxrs.resource.CounterReportsResource;
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

public class CounterReportAPI implements CounterReportsResource {

  private static final String ID_FIELD = "_id";
  private static final String TABLE_NAME_COUNTER_REPORTS = "counter_reports";
  private final Messages messages = Messages.getInstance();
  private final Logger logger = LoggerFactory.getLogger(CounterReportAPI.class);

  public CounterReportAPI(Vertx vertx, String tenantId) {
    PostgresClient.getInstance(vertx, tenantId).setIdField(ID_FIELD);
  }

  private CQLWrapper getCQL(String query, int limit, int offset) throws FieldException {
    CQL2PgJSON cql2PgJSON = new CQL2PgJSON(Arrays.asList(TABLE_NAME_COUNTER_REPORTS + ".jsonb"));
    return new CQLWrapper(cql2PgJSON, query)
        .setLimit(new Limit(limit))
        .setOffset(new Offset(offset));
  }

  @Override
  @Validate
  public void getCounterReports(String query, String orderBy, Order order, int offset, int limit,
      String lang, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) throws Exception {
    logger.debug("Getting counter reports");
    try {
      CQLWrapper cql = getCQL(query, limit, offset);
      vertxContext.runOnContext(v -> {
        String tenantId = TenantTool
            .calculateTenantId(okapiHeaders.get(Constants.OKAPI_HEADER_TENANT));
        logger.debug("Headers present are: " + okapiHeaders.keySet().toString());
        logger.debug("tenantId = " + tenantId);
        String[] fieldList = {"*"};
        try {
          PostgresClient.getInstance(vertxContext.owner(), tenantId).get(TABLE_NAME_COUNTER_REPORTS,
              CounterReport.class, fieldList, cql, true, false, reply -> {
                try {
                  if (reply.succeeded()) {
                    CounterReportDataDataCollection counterReportDataDataCollection = new CounterReportDataDataCollection();
                    List<CounterReport> reports = (List<CounterReport>) reply
                        .result().getResults();
                    counterReportDataDataCollection.setCounterReports(reports);
                    counterReportDataDataCollection
                        .setTotalRecords(reply.result().getResultInfo().getTotalRecords());
                    asyncResultHandler.handle(Future.succeededFuture(
                        GetCounterReportsResponse.withJsonOK(counterReportDataDataCollection)
                    ));
                  } else {
                    asyncResultHandler.handle(Future.succeededFuture(
                        GetCounterReportsResponse.withPlainInternalServerError(
                            reply.cause().getMessage()
                        )
                    ));
                  }
                } catch (Exception e) {
                  logger.debug(e.getLocalizedMessage());
                  asyncResultHandler.handle(Future.succeededFuture(
                      GetCounterReportsResponse.withPlainInternalServerError(
                          reply.cause().getMessage()
                      )
                  ));
                }
              });
        } catch (IllegalStateException e) {
          logger.debug("IllegalStateException: " + e.getLocalizedMessage());
          asyncResultHandler
              .handle(Future.succeededFuture(GetCounterReportsResponse.withPlainBadRequest(
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
                .handle(Future.succeededFuture(GetCounterReportsResponse.withPlainBadRequest(
                    "CQL Parsing Error for '" + "" + "': " + cause.getLocalizedMessage())));
          } else {
            asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
                GetCounterReportsResponse.withPlainInternalServerError(
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
            .handle(Future.succeededFuture(GetCounterReportsResponse.withPlainBadRequest(
                "CQL Parsing Error for '" + "" + "': " + e.getLocalizedMessage())));
      } else {
        asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
            GetCounterReportsResponse.withPlainInternalServerError(
                messages.getMessage(lang,
                    MessageConsts.InternalServerError))));
      }
    }
  }

  @Override
  @Validate
  public void postCounterReports(String lang, CounterReport entity,
      Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) throws Exception {
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
          try {
            PostgresClient.getInstance(vertxContext.owner(),
                TenantTool.calculateTenantId(tenantId)).get(TABLE_NAME_COUNTER_REPORTS,
                CounterReport.class, true, getReply -> {
                  logger.debug("Attempting to get existing counter report of same id");
                  if (getReply.failed()) {
                    logger.debug("Attempt to get counter report failed: " +
                        getReply.cause().getMessage());
                    asyncResultHandler.handle(Future.succeededFuture(
                        PostCounterReportsResponse.withPlainInternalServerError(
                            getReply.cause().getMessage())));
                  } else {
                    List<CounterReport> reportList = (List<CounterReport>) getReply.result()
                        .getResults();
                    if (reportList.size() > 0) {
                      logger.debug("Counter report with this id already exists");
                      asyncResultHandler.handle(Future.succeededFuture(
                          PostCounterReportsResponse.withJsonUnprocessableEntity(
                              ValidationHelper.createValidationErrorMessage(
                                  "'id'", entity.getId(),
                                  "Counter report with this id already exists"))));
                    } else {
                      PostgresClient postgresClient = PostgresClient
                          .getInstance(vertxContext.owner(), tenantId);
                      postgresClient
                          .save(TABLE_NAME_COUNTER_REPORTS, entity.getId(), entity, reply -> {
                            try {
                              if (reply.succeeded()) {
                                logger.debug("save successful");
                                final CounterReport report = entity;
                                report.setId(entity.getId());
                                OutStream stream = new OutStream();
                                stream.setData(report);
                                asyncResultHandler.handle(
                                    Future.succeededFuture(
                                        PostCounterReportsResponse
                                            .withJsonCreated(
                                                reply.result(), stream)));
                              } else {
                                asyncResultHandler.handle(Future.succeededFuture(
                                    PostCounterReportsResponse.withPlainInternalServerError(
                                        reply.cause().toString())));
                              }
                            } catch (Exception e) {
                              asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
                                  PostCounterReportsResponse
                                      .withPlainInternalServerError(e.getMessage())));
                            }
                          });
                    }
                  }
                });
          } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            asyncResultHandler.handle(Future.succeededFuture(
                PostCounterReportsResponse.withPlainInternalServerError(
                    messages.getMessage(lang, MessageConsts.InternalServerError))));
          }
        } catch (Exception e) {
          asyncResultHandler.handle(Future.succeededFuture(
              PostCounterReportsResponse.withPlainInternalServerError(
                  messages.getMessage(lang, MessageConsts.InternalServerError))));
        }
      });
    } catch (Exception e) {
      asyncResultHandler.handle(Future.succeededFuture(
          PostCounterReportsResponse.withPlainInternalServerError(
              messages.getMessage(lang, MessageConsts.InternalServerError))));
    }
  }

  @Override
  @Validate
  public void getCounterReportsById(String id, String lang, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) throws Exception {
    try {
      vertxContext.runOnContext(v -> {
        String tenantId = TenantTool
            .calculateTenantId(okapiHeaders.get(Constants.OKAPI_HEADER_TENANT));
        try {
          Criteria idCrit = new Criteria()
              .addField("_id")
              .setJSONB(false)
              .setOperation("=")
              .setValue("'" + id + "'");
          Criterion criterion = new Criterion(idCrit);
          logger.debug("Using criterion: " + criterion.toString());
          PostgresClient.getInstance(vertxContext.owner(), tenantId)
              .get(TABLE_NAME_COUNTER_REPORTS, CounterReport.class, criterion,
                  true, false, getReply -> {
                    if (getReply.failed()) {
                      asyncResultHandler.handle(Future.succeededFuture(
                          GetCounterReportsByIdResponse.withPlainInternalServerError(
                              messages.getMessage(lang, MessageConsts.InternalServerError))));
                    } else {
                      List<CounterReport> reportList = (List<CounterReport>) getReply.result()
                          .getResults();
                      if (reportList.size() < 1) {
                        asyncResultHandler.handle(Future.succeededFuture(
                            GetCounterReportsByIdResponse.withPlainNotFound("Counter report: " +
                                messages.getMessage(lang,
                                    MessageConsts.ObjectDoesNotExist))));
                      } else if (reportList.size() > 1) {
                        logger.debug("Multiple counter reports found with the same id");
                        asyncResultHandler.handle(Future.succeededFuture(
                            GetCounterReportsByIdResponse.withPlainInternalServerError(
                                messages.getMessage(lang,
                                    MessageConsts.InternalServerError))));
                      } else {
                        asyncResultHandler.handle(Future.succeededFuture(
                            GetCounterReportsByIdResponse.withJsonOK(reportList.get(0))));
                      }
                    }
                  });
        } catch (Exception e) {
          logger.info("Error occurred: " + e.getMessage());
          asyncResultHandler.handle(Future.succeededFuture(
              GetCounterReportsByIdResponse.withPlainInternalServerError(messages.getMessage(
                  lang, MessageConsts.InternalServerError))));
        }
      });
    } catch (Exception e) {
      asyncResultHandler.handle(Future.succeededFuture(
          GetCounterReportsByIdResponse.withPlainInternalServerError(messages.getMessage(
              lang, MessageConsts.InternalServerError))));
    }
  }

  @Override
  @Validate
  public void deleteCounterReportsById(String id, String lang, Map<String, String> okapiHeaders,
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
              TABLE_NAME_COUNTER_REPORTS, new Criterion(idCrit), deleteReply -> {
                if (deleteReply.failed()) {
                  logger.debug("Delete failed: " + deleteReply.cause().getMessage());
                  asyncResultHandler.handle(Future.succeededFuture(
                      DeleteCounterReportsByIdResponse.withPlainNotFound("Not found")));
                } else {
                  asyncResultHandler.handle(Future.succeededFuture(
                      DeleteCounterReportsByIdResponse.withNoContent()));
                }
              });
        } catch (Exception e) {
          logger.debug("Delete failed: " + e.getMessage());
          asyncResultHandler.handle(
              Future.succeededFuture(
                  DeleteCounterReportsByIdResponse.withPlainInternalServerError(
                      messages.getMessage(lang,
                          MessageConsts.InternalServerError))));
        }
      });
    } catch (Exception e) {
      asyncResultHandler.handle(
          Future.succeededFuture(
              DeleteCounterReportsByIdResponse.withPlainInternalServerError(
                  messages.getMessage(lang,
                      MessageConsts.InternalServerError))));
    }
  }

  @Override
  @Validate
  public void putCounterReportsById(String id, String lang, CounterReport entity,
      Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) throws Exception {
    try {
      vertxContext.runOnContext(v -> {
        if (!id.equals(entity.getId())) {
          asyncResultHandler.handle(Future.succeededFuture(
              PutCounterReportsByIdResponse
                  .withPlainBadRequest("You cannot change the value of the id field")));
        } else {
          String tenantId = TenantTool
              .calculateTenantId(okapiHeaders.get(Constants.OKAPI_HEADER_TENANT));
          try {
            PostgresClient.getInstance(vertxContext.owner(), tenantId)
                .get(TABLE_NAME_COUNTER_REPORTS,
                    CounterReport.class, true, false, getReply -> {
                      if (getReply.failed()) {
                        logger.debug("Error querying existing counter report: " + getReply.cause()
                            .getLocalizedMessage());
                        asyncResultHandler.handle(Future.succeededFuture(
                            PutCounterReportsByIdResponse.withPlainInternalServerError(
                                messages.getMessage(lang,
                                    MessageConsts.InternalServerError))));
                      } else {
                        List<CounterReport> counterReportList = (List<CounterReport>) getReply
                            .result()
                            .getResults();
                        Date createdDate = null;
                        Date now = new Date();
                        if (counterReportList.size() > 0) {
                          createdDate = counterReportList.get(0).getCreatedDate();
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
                          PostgresClient.getInstance(vertxContext.owner(), tenantId).update(
                              TABLE_NAME_COUNTER_REPORTS, entity, new Criterion(idCrit), true,
                              putReply -> {
                                try {
                                  if (putReply.failed()) {
                                    asyncResultHandler.handle(Future.succeededFuture(
                                        PutCounterReportsByIdResponse
                                            .withPlainInternalServerError(
                                                putReply.cause().getMessage())));
                                  } else {
                                    asyncResultHandler.handle(Future.succeededFuture(
                                        PutCounterReportsByIdResponse.withNoContent()));
                                  }
                                } catch (Exception e) {
                                  asyncResultHandler.handle(Future.succeededFuture(
                                      PutCounterReportsByIdResponse.withPlainInternalServerError(
                                          messages.getMessage(lang,
                                              MessageConsts.InternalServerError))));
                                }
                              });
                        } catch (Exception e) {
                          asyncResultHandler.handle(Future.succeededFuture(
                              PutCounterReportsByIdResponse.withPlainInternalServerError(
                                  messages.getMessage(lang,
                                      MessageConsts.InternalServerError))));
                        }
                      }
                    });
          } catch (Exception e) {
            logger.debug(e.getLocalizedMessage());
            asyncResultHandler.handle(Future.succeededFuture(
                PutCounterReportsByIdResponse.withPlainInternalServerError(
                    messages.getMessage(lang, MessageConsts.InternalServerError))));
          }
        }
      });
    } catch (Exception e) {
      logger.debug(e.getLocalizedMessage());
      asyncResultHandler.handle(Future.succeededFuture(
          PutCounterReportsByIdResponse.withPlainInternalServerError(
              messages.getMessage(lang, MessageConsts.InternalServerError))));
    }
  }

}

