package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.Json;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.cql2pgjson.CQL2PgJSON;
import org.folio.cql2pgjson.exception.FieldException;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.rest.annotations.Validate;
import org.folio.rest.jaxrs.model.CounterReport;
import org.folio.rest.jaxrs.model.CounterReports;
import org.folio.rest.jaxrs.model.CounterReportsGetOrder;
import org.folio.rest.persist.Criteria.Criteria;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.Criteria.Limit;
import org.folio.rest.persist.Criteria.Offset;
import org.folio.rest.persist.PgUtil;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.cql.CQLWrapper;
import org.folio.rest.tools.messages.MessageConsts;
import org.folio.rest.tools.messages.Messages;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.rest.util.Constants;
import org.folio.rest.util.PgHelper;
import org.folio.rest.util.UploadHelper;
import org.niso.schemas.counter.Report;
import org.olf.erm.usage.counter41.Counter4Utils;

import javax.ws.rs.core.Response;
import java.io.InputStream;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

import static org.folio.rest.util.Constants.TABLE_NAME_COUNTER_REPORTS;

public class CounterReportAPI implements org.folio.rest.jaxrs.resource.CounterReports {

  private final Messages messages = Messages.getInstance();
  private final Logger logger = LoggerFactory.getLogger(CounterReportAPI.class);

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
                            List<CounterReport> reports = reply.result().getResults();
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

    PgUtil.post(
        TABLE_NAME_COUNTER_REPORTS,
        entity,
        okapiHeaders,
        vertxContext,
        PostCounterReportsResponse.class,
        asyncResultHandler);
  }

  @Override
  @Validate
  public void getCounterReportsById(
      String id,
      String lang,
      Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {

    PgUtil.getById(
        TABLE_NAME_COUNTER_REPORTS,
        CounterReport.class,
        id,
        okapiHeaders,
        vertxContext,
        GetCounterReportsByIdResponse.class,
        asyncResultHandler);
  }

  @Override
  @Validate
  public void deleteCounterReportsById(
      String id,
      String lang,
      Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {

    PgUtil.deleteById(
        TABLE_NAME_COUNTER_REPORTS,
        id,
        okapiHeaders,
        vertxContext,
        DeleteCounterReportsByIdResponse.class,
        asyncResultHandler);
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

    PgUtil.put(
        TABLE_NAME_COUNTER_REPORTS,
        entity,
        id,
        okapiHeaders,
        vertxContext,
        PutCounterReportsByIdResponse.class,
        asyncResultHandler);
  }

  private Optional<String> csvMapper(CounterReport cr) {
    if (cr.getRelease().equals("4") && cr.getReport() != null) {
      return Optional.ofNullable(
          Counter4Utils.toCSV(Counter4Utils.fromJSON(Json.encode(cr.getReport()))));
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
                  asyncResultHandler.handle(
                      Future.succeededFuture(
                          GetCounterReportsCsvByIdResponse.respond200WithTextCsv(csvResult.get())));
                } else {
                  asyncResultHandler.handle(
                      Future.succeededFuture(
                          GetCounterReportsCsvByIdResponse.respond500WithTextPlain(
                              "No report data or no mapper available")));
                }
              } else {
                asyncResultHandler.handle(
                    Future.succeededFuture(
                        GetCounterReportsCsvByIdResponse.respond500WithTextPlain(
                            ar.cause().getMessage())));
              }
            });
  }

  @Override
  public void postCounterReportsUploadProviderById(
      String id,
      boolean overwrite,
      InputStream entity,
      Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {

    String tenantId = okapiHeaders.get(XOkapiHeaders.TENANT);

    List<CounterReport> counterReports;
    try {
      counterReports = UploadHelper.getCounterReportsFromInputStream(entity);
    } catch (Exception e) {
      asyncResultHandler.handle(
          Future.succeededFuture(
              PostCounterReportsUploadProviderByIdResponse.respond500WithTextPlain(
                  String.format("Error uploading file: %s", e.getMessage()))));
      return;
    }

    PgHelper.getUDPfromDbById(vertxContext.owner(), tenantId, id)
        .compose(
            udp -> {
              counterReports.forEach(
                  cr -> cr.withProviderId(udp.getId()).withDownloadTime(Date.from(Instant.now())));
              return Future.succeededFuture(counterReports);
            })
        .compose(crs -> PgHelper.saveCounterReportsToDb(vertxContext, tenantId, crs, overwrite))
        .setHandler(
            ar -> {
              if (ar.succeeded()) {
                asyncResultHandler.handle(
                    Future.succeededFuture(
                        PostCounterReportsUploadProviderByIdResponse.respond200WithTextPlain(
                            String.format(
                                "Saved report with ids: %s", String.join(",", ar.result())))));
              } else {
                asyncResultHandler.handle(
                    Future.succeededFuture(
                        PostCounterReportsUploadProviderByIdResponse.respond500WithTextPlain(
                            String.format("Error saving report: %s", ar.cause()))));
              }
            });
  }

  @Override
  public void getCounterReportsCsvProviderReportVersionFromToByIdAndNameAndVersionAndBeginAndEnd(
      String id,
      String name,
      String version,
      String begin,
      String end,
      Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {

    Criteria providerCrit =
        new Criteria()
            .addField(Constants.FIELD_NAME_PROVIDER_ID)
            .setOperation(Constants.OPERATOR_EQUALS)
            .setVal(id);
    Criteria reportNameCrit =
        new Criteria()
            .addField(Constants.FIELD_NAME_REPORT_NAME)
            .setOperation(Constants.OPERATOR_EQUALS)
            .setVal(name);
    Criteria releaseCrit =
        new Criteria()
            .addField(Constants.FIELD_NAME_RELEASE)
            .setOperation(Constants.OPERATOR_EQUALS)
            .setVal(version);
    Criteria reportCrit =
        new Criteria().addField(Constants.FIELD_NAME_REPORT).setOperation("IS NOT NULL");
    Criteria yearMonthBeginCrit =
        new Criteria().addField(Constants.FIELD_NAME_YEAR_MONTH).setOperation(">=").setVal(begin);
    Criteria yearMonthEndCrit =
        new Criteria().addField(Constants.FIELD_NAME_YEAR_MONTH).setOperation("<=").setVal(end);
    Criterion criterion =
        new Criterion()
            .addCriterion(providerCrit)
            .addCriterion(reportNameCrit)
            .addCriterion(releaseCrit)
            .addCriterion(reportCrit)
            .addCriterion(yearMonthBeginCrit)
            .addCriterion(yearMonthEndCrit);
    CQLWrapper cql = new CQLWrapper(criterion);

    PostgresClient.getInstance(vertxContext.owner(), okapiHeaders.get(XOkapiHeaders.TENANT))
        .get(
            TABLE_NAME_COUNTER_REPORTS,
            CounterReport.class,
            cql,
            true,
            true,
            ar -> {
              if (ar.succeeded()) {
                List<Report> reports =
                    ar.result().getResults().stream()
                        .map(
                            cr -> {
                              if (version.equals("4")) {
                                return Counter4Utils.fromJSON(Json.encode(cr.getReport()));
                              } else {
                                return null;
                              }
                            })
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());

                if (reports.isEmpty()) {
                  asyncResultHandler.handle(
                      Future.succeededFuture(
                          GetCounterReportsCsvProviderReportVersionFromToByIdAndNameAndVersionAndBeginAndEndResponse
                              .respond500WithTextPlain("No valid reports found in period")));
                  return;
                }

                String csv;
                try {
                  Report merge = Counter4Utils.merge(reports);
                  csv = Counter4Utils.toCSV(merge);
                } catch (Exception e) {
                  asyncResultHandler.handle(
                      Future.succeededFuture(
                          GetCounterReportsCsvProviderReportVersionFromToByIdAndNameAndVersionAndBeginAndEndResponse
                              .respond500WithTextPlain(e.getMessage())));
                  return;
                }

                asyncResultHandler.handle(
                    Future.succeededFuture(
                        GetCounterReportsCsvProviderReportVersionFromToByIdAndNameAndVersionAndBeginAndEndResponse
                            .respond200WithTextCsv(csv)));
              } else {
                asyncResultHandler.handle(
                    Future.succeededFuture(
                        GetCounterReportsCsvProviderReportVersionFromToByIdAndNameAndVersionAndBeginAndEndResponse
                            .respond500WithTextPlain("Query Error: " + ar.cause())));
              }
            });
  }
}
