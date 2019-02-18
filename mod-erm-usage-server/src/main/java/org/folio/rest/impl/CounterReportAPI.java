package org.folio.rest.impl;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import javax.ws.rs.core.Response;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.rest.annotations.Validate;
import org.folio.rest.jaxrs.model.CounterReport;
import org.folio.rest.jaxrs.model.CounterReports;
import org.folio.rest.jaxrs.model.CounterReportsGetOrder;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.Criteria.Criteria;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.Criteria.Limit;
import org.folio.rest.persist.Criteria.Offset;
import org.folio.rest.persist.cql.CQLWrapper;
import org.folio.rest.tools.messages.MessageConsts;
import org.folio.rest.tools.messages.Messages;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.rest.tools.utils.ValidationHelper;
import org.olf.erm.usage.counter41.Counter4Utils;
import org.z3950.zing.cql.cql2pgjson.CQL2PgJSON;
import org.z3950.zing.cql.cql2pgjson.FieldException;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class CounterReportAPI implements org.folio.rest.jaxrs.resource.CounterReports {

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

  @Validate
  @Override
  public void getCounterReports(
      boolean tiny,
      String query,
      String orderBy,
      CounterReportsGetOrder order,
      int offset,
      int limit,
      String lang,
      Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {
    logger.debug("Getting counter reports");
    try {
      CQLWrapper cql = getCQL(query, limit, offset);
      vertxContext.runOnContext(
          v -> {
            String tenantId = TenantTool.calculateTenantId(okapiHeaders.get(XOkapiHeaders.TENANT));
            logger.debug("Headers present are: " + okapiHeaders.keySet().toString());
            logger.debug("tenantId = " + tenantId);

            String field = (tiny) ? "jsonb - 'report' AS jsonb" : "*";
            String[] fieldList = {field};
            try {
              PostgresClient.getInstance(vertxContext.owner(), tenantId)
                  .get(
                      TABLE_NAME_COUNTER_REPORTS,
                      CounterReport.class,
                      fieldList,
                      cql,
                      true,
                      false,
                      reply -> {
                        try {
                          if (reply.succeeded()) {
                            CounterReports counterReportDataDataCollection = new CounterReports();
                            List<CounterReport> reports =
                                (List<CounterReport>) reply.result().getResults();
                            counterReportDataDataCollection.setCounterReports(reports);
                            counterReportDataDataCollection.setTotalRecords(
                                reply.result().getResultInfo().getTotalRecords());
                            asyncResultHandler.handle(
                                Future.succeededFuture(
                                    GetCounterReportsResponse.respond200WithApplicationJson(
                                        counterReportDataDataCollection)));
                          } else {
                            asyncResultHandler.handle(
                                Future.succeededFuture(
                                    GetCounterReportsResponse.respond500WithTextPlain(
                                        reply.cause().getMessage())));
                          }
                        } catch (Exception e) {
                          logger.debug(e.getLocalizedMessage());
                          asyncResultHandler.handle(
                              Future.succeededFuture(
                                  GetCounterReportsResponse.respond500WithTextPlain(
                                      reply.cause().getMessage())));
                        }
                      });
            } catch (IllegalStateException e) {
              logger.debug("IllegalStateException: " + e.getLocalizedMessage());
              asyncResultHandler.handle(
                  Future.succeededFuture(
                      GetCounterReportsResponse.respond400WithTextPlain(
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
                        GetCounterReportsResponse.respond400WithTextPlain(
                            "CQL Parsing Error for '" + "" + "': " + cause.getLocalizedMessage())));
              } else {
                asyncResultHandler.handle(
                    io.vertx.core.Future.succeededFuture(
                        GetCounterReportsResponse.respond500WithTextPlain(
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
                GetCounterReportsResponse.respond400WithTextPlain(
                    "CQL Parsing Error for '" + "" + "': " + e.getLocalizedMessage())));
      } else {
        asyncResultHandler.handle(
            io.vertx.core.Future.succeededFuture(
                GetCounterReportsResponse.respond500WithTextPlain(
                    messages.getMessage(lang, MessageConsts.InternalServerError))));
      }
    }
  }

  @Override
  @Validate
  public void postCounterReports(
      String lang,
      CounterReport entity,
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
              labelCrit.addField("'id'");
              labelCrit.setOperation("=");
              labelCrit.setValue(entity.getId());
              Criterion crit = new Criterion(labelCrit);
              try {
                PostgresClient.getInstance(
                        vertxContext.owner(), TenantTool.calculateTenantId(tenantId))
                    .get(
                        TABLE_NAME_COUNTER_REPORTS,
                        CounterReport.class,
                        crit,
                        true,
                        getReply -> {
                          logger.debug("Attempting to get existing counter report of same id");
                          if (getReply.failed()) {
                            logger.debug(
                                "Attempt to get counter report failed: "
                                    + getReply.cause().getMessage());
                            asyncResultHandler.handle(
                                Future.succeededFuture(
                                    PostCounterReportsResponse.respond500WithTextPlain(
                                        getReply.cause().getMessage())));
                          } else {
                            List<CounterReport> reportList =
                                (List<CounterReport>) getReply.result().getResults();
                            if (reportList.size() > 0) {
                              logger.debug("Counter report with this id already exists");
                              asyncResultHandler.handle(
                                  Future.succeededFuture(
                                      PostCounterReportsResponse.respond422WithApplicationJson(
                                          ValidationHelper.createValidationErrorMessage(
                                              "'id'",
                                              entity.getId(),
                                              "Counter report with this id already exists"))));
                            } else {
                              PostgresClient postgresClient =
                                  PostgresClient.getInstance(vertxContext.owner(), tenantId);
                              postgresClient.save(
                                  TABLE_NAME_COUNTER_REPORTS,
                                  entity.getId(),
                                  entity,
                                  reply -> {
                                    try {
                                      if (reply.succeeded()) {
                                        logger.debug("save successful");
                                        asyncResultHandler.handle(
                                            Future.succeededFuture(
                                                PostCounterReportsResponse
                                                    .respond201WithApplicationJson(
                                                        entity,
                                                        PostCounterReportsResponse.headersFor201()
                                                            .withLocation(
                                                                "/counter-reports/"
                                                                    + entity.getId()))));
                                      } else {
                                        asyncResultHandler.handle(
                                            Future.succeededFuture(
                                                PostCounterReportsResponse.respond500WithTextPlain(
                                                    reply.cause().toString())));
                                      }
                                    } catch (Exception e) {
                                      asyncResultHandler.handle(
                                          io.vertx.core.Future.succeededFuture(
                                              PostCounterReportsResponse.respond500WithTextPlain(
                                                  e.getMessage())));
                                    }
                                  });
                            }
                          }
                        });
              } catch (Exception e) {
                logger.error(e.getLocalizedMessage(), e);
                asyncResultHandler.handle(
                    Future.succeededFuture(
                        PostCounterReportsResponse.respond500WithTextPlain(
                            messages.getMessage(lang, MessageConsts.InternalServerError))));
              }
            } catch (Exception e) {
              asyncResultHandler.handle(
                  Future.succeededFuture(
                      PostCounterReportsResponse.respond500WithTextPlain(
                          messages.getMessage(lang, MessageConsts.InternalServerError))));
            }
          });
    } catch (Exception e) {
      asyncResultHandler.handle(
          Future.succeededFuture(
              PostCounterReportsResponse.respond500WithTextPlain(
                  messages.getMessage(lang, MessageConsts.InternalServerError))));
    }
  }

  @Override
  @Validate
  public void getCounterReportsById(
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
                      .addField("_id")
                      .setJSONB(false)
                      .setOperation("=")
                      .setValue("'" + id + "'");
              Criterion criterion = new Criterion(idCrit);
              logger.debug("Using criterion: " + criterion.toString());
              PostgresClient.getInstance(vertxContext.owner(), tenantId)
                  .get(
                      TABLE_NAME_COUNTER_REPORTS,
                      CounterReport.class,
                      criterion,
                      true,
                      false,
                      getReply -> {
                        if (getReply.failed()) {
                          asyncResultHandler.handle(
                              Future.succeededFuture(
                                  GetCounterReportsByIdResponse.respond500WithTextPlain(
                                      messages.getMessage(
                                          lang, MessageConsts.InternalServerError))));
                        } else {
                          List<CounterReport> reportList =
                              (List<CounterReport>) getReply.result().getResults();
                          if (reportList.size() < 1) {
                            asyncResultHandler.handle(
                                Future.succeededFuture(
                                    GetCounterReportsByIdResponse.respond404WithTextPlain(
                                        "Counter report: "
                                            + messages.getMessage(
                                                lang, MessageConsts.ObjectDoesNotExist))));
                          } else if (reportList.size() > 1) {
                            logger.debug("Multiple counter reports found with the same id");
                            asyncResultHandler.handle(
                                Future.succeededFuture(
                                    GetCounterReportsByIdResponse.respond500WithTextPlain(
                                        messages.getMessage(
                                            lang, MessageConsts.InternalServerError))));
                          } else {
                            asyncResultHandler.handle(
                                Future.succeededFuture(
                                    GetCounterReportsByIdResponse.respond200WithApplicationJson(
                                        reportList.get(0))));
                          }
                        }
                      });
            } catch (Exception e) {
              logger.info("Error occurred: " + e.getMessage());
              asyncResultHandler.handle(
                  Future.succeededFuture(
                      GetCounterReportsByIdResponse.respond500WithTextPlain(
                          messages.getMessage(lang, MessageConsts.InternalServerError))));
            }
          });
    } catch (Exception e) {
      asyncResultHandler.handle(
          Future.succeededFuture(
              GetCounterReportsByIdResponse.respond500WithTextPlain(
                  messages.getMessage(lang, MessageConsts.InternalServerError))));
    }
  }

  @Override
  @Validate
  public void deleteCounterReportsById(
      String id,
      String lang,
      Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {
    try {
      vertxContext.runOnContext(
          v -> {
            String tenantId = TenantTool.calculateTenantId(okapiHeaders.get(XOkapiHeaders.TENANT));
            Criteria idCrit =
                new Criteria()
                    .addField(ID_FIELD)
                    .setJSONB(false)
                    .setOperation("=")
                    .setValue("'" + id + "'");
            try {
              PostgresClient.getInstance(vertxContext.owner(), tenantId)
                  .delete(
                      TABLE_NAME_COUNTER_REPORTS,
                      new Criterion(idCrit),
                      deleteReply -> {
                        if (deleteReply.failed()) {
                          logger.debug("Delete failed: " + deleteReply.cause().getMessage());
                          asyncResultHandler.handle(
                              Future.succeededFuture(
                                  DeleteCounterReportsByIdResponse.respond404WithTextPlain(
                                      "Not found")));
                        } else {
                          asyncResultHandler.handle(
                              Future.succeededFuture(
                                  DeleteCounterReportsByIdResponse.respond204()));
                        }
                      });
            } catch (Exception e) {
              logger.debug("Delete failed: " + e.getMessage());
              asyncResultHandler.handle(
                  Future.succeededFuture(
                      DeleteCounterReportsByIdResponse.respond500WithTextPlain(
                          messages.getMessage(lang, MessageConsts.InternalServerError))));
            }
          });
    } catch (Exception e) {
      asyncResultHandler.handle(
          Future.succeededFuture(
              DeleteCounterReportsByIdResponse.respond500WithTextPlain(
                  messages.getMessage(lang, MessageConsts.InternalServerError))));
    }
  }

  @Override
  @Validate
  public void putCounterReportsById(
      String id,
      String lang,
      CounterReport entity,
      Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {
    try {
      vertxContext.runOnContext(
          v -> {
            if (!id.equals(entity.getId())) {
              asyncResultHandler.handle(
                  Future.succeededFuture(
                      PutCounterReportsByIdResponse.respond400WithTextPlain(
                          "You cannot change the value of the id field")));
            } else {
              String tenantId =
                  TenantTool.calculateTenantId(okapiHeaders.get(XOkapiHeaders.TENANT));
              Criteria labelCrit = new Criteria();
              labelCrit.addField("'id'");
              labelCrit.setOperation("=");
              labelCrit.setValue(entity.getId());
              Criterion crit = new Criterion(labelCrit);
              try {
                PostgresClient.getInstance(vertxContext.owner(), tenantId)
                    .get(
                        TABLE_NAME_COUNTER_REPORTS,
                        CounterReport.class,
                        crit,
                        false,
                        getReply -> {
                          if (getReply.failed()) {
                            logger.debug(
                                "Error querying existing counter report: "
                                    + getReply.cause().getLocalizedMessage());
                            asyncResultHandler.handle(
                                Future.succeededFuture(
                                    PutCounterReportsByIdResponse.respond500WithTextPlain(
                                        messages.getMessage(
                                            lang, MessageConsts.InternalServerError))));
                          } else {
                            Criteria idCrit =
                                new Criteria()
                                    .addField(ID_FIELD)
                                    .setJSONB(false)
                                    .setOperation("=")
                                    .setValue("'" + id + "'");
                            try {
                              PostgresClient.getInstance(vertxContext.owner(), tenantId)
                                  .update(
                                      TABLE_NAME_COUNTER_REPORTS,
                                      entity,
                                      new Criterion(idCrit),
                                      true,
                                      putReply -> {
                                        try {
                                          if (putReply.failed()) {
                                            asyncResultHandler.handle(
                                                Future.succeededFuture(
                                                    PutCounterReportsByIdResponse
                                                        .respond500WithTextPlain(
                                                            putReply.cause().getMessage())));
                                          } else {
                                            asyncResultHandler.handle(
                                                Future.succeededFuture(
                                                    PutCounterReportsByIdResponse.respond204()));
                                          }
                                        } catch (Exception e) {
                                          asyncResultHandler.handle(
                                              Future.succeededFuture(
                                                  PutCounterReportsByIdResponse
                                                      .respond500WithTextPlain(
                                                          messages.getMessage(
                                                              lang,
                                                              MessageConsts.InternalServerError))));
                                        }
                                      });
                            } catch (Exception e) {
                              asyncResultHandler.handle(
                                  Future.succeededFuture(
                                      PutCounterReportsByIdResponse.respond500WithTextPlain(
                                          messages.getMessage(
                                              lang, MessageConsts.InternalServerError))));
                            }
                          }
                        });
              } catch (Exception e) {
                logger.debug(e.getLocalizedMessage());
                asyncResultHandler.handle(
                    Future.succeededFuture(
                        PutCounterReportsByIdResponse.respond500WithTextPlain(
                            messages.getMessage(lang, MessageConsts.InternalServerError))));
              }
            }
          });
    } catch (Exception e) {
      logger.debug(e.getLocalizedMessage());
      asyncResultHandler.handle(
          Future.succeededFuture(
              PutCounterReportsByIdResponse.respond500WithTextPlain(
                  messages.getMessage(lang, MessageConsts.InternalServerError))));
    }
  }

  private Optional<String> csvMapper(CounterReport cr) {
    if (cr.getRelease().equals("4") && cr.getReport() != null) {
      return Optional.of(Counter4Utils.toCSV(Counter4Utils.fromJSON(Json.encode(cr.getReport()))));
    }
    return Optional.empty();
  }

  @Override
  public void getCounterReportsCsvById(
      String id,
      Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {

    PostgresClient.getInstance(vertxContext.owner(), okapiHeaders.get(XOkapiHeaders.TENANT))
        .getById(
            TABLE_NAME_COUNTER_REPORTS,
            id,
            CounterReport.class,
            ar -> {
              if (ar.succeeded()) {
                Optional<String> csvResult = csvMapper(ar.result());
                if (csvResult.isPresent()) {
                  if (csvResult.get() != null) {
                    asyncResultHandler.handle(
                        Future.succeededFuture(
                            GetCounterReportsCsvByIdResponse.respond200WithTextCsv(
                                csvResult.get())));
                  } else {
                    asyncResultHandler.handle(
                        Future.succeededFuture(
                            GetCounterReportsCsvByIdResponse.respond500WithTextPlain(
                                "Error while tranforming report to CSV")));
                  }
                } else {
                  asyncResultHandler.handle(
                      Future.succeededFuture(
                          GetCounterReportsCsvByIdResponse.respond500WithTextPlain(
                              "Empty report data or no mapper available")));
                }
              } else {
                ValidationHelper.handleError(ar.cause(), asyncResultHandler);
              }
            });
  }
}
