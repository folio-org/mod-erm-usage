package org.folio.rest.impl;

import static io.vertx.core.Future.succeededFuture;
import static org.folio.rest.util.Constants.TABLE_NAME_COUNTER_REPORTS;
import static org.folio.rest.util.ReportExportHelper.createDownloadResponseByReportVersion;
import static org.folio.rest.util.ReportExportHelper.createExportMultipleMonthsResponseByReportVersion;
import static org.folio.rest.util.ReportExportHelper.createExportResponseByFormat;
import static org.folio.rest.util.ReportExportHelper.createGetMultipleReportsCQL;
import static org.folio.rest.util.VertxUtil.executeBlocking;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.sqlclient.Tuple;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.ws.rs.core.Response;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.cql2pgjson.CQL2PgJSON;
import org.folio.cql2pgjson.exception.FieldException;
import org.folio.rest.annotations.Validate;
import org.folio.rest.jaxrs.model.CounterReport;
import org.folio.rest.jaxrs.model.CounterReportDocument;
import org.folio.rest.jaxrs.model.CounterReports;
import org.folio.rest.jaxrs.model.CounterReportsGetOrder;
import org.folio.rest.jaxrs.model.CounterReportsPerYear;
import org.folio.rest.jaxrs.model.CounterReportsSorted;
import org.folio.rest.jaxrs.model.ReportsPerType;
import org.folio.rest.persist.Criteria.Criteria;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.PgUtil;
import org.folio.rest.persist.cql.CQLWrapper;
import org.folio.rest.tools.utils.ValidationHelper;
import org.folio.rest.util.PgHelper;
import org.folio.rest.util.UploadHelper;

public class CounterReportAPI implements org.folio.rest.jaxrs.resource.CounterReports {

  private final Logger logger = LogManager.getLogger(CounterReportAPI.class);

  private final Comparator<CounterReportsPerYear> compareByYear =
      Comparator.comparing(CounterReportsPerYear::getYear);

  private CQLWrapper getCQL(String query, int limit, int offset) throws FieldException {
    return new CQLWrapper(
        new CQL2PgJSON(TABLE_NAME_COUNTER_REPORTS + ".jsonb"), query, limit, offset);
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
    logger.debug("Headers present are: {}", okapiHeaders::toString);

    CQLWrapper cql;
    try {
      cql = getCQL(query, limit, offset);
    } catch (FieldException e) {
      ValidationHelper.handleError(e, asyncResultHandler);
      return;
    }

    String field = (tiny) ? "jsonb - 'report' AS jsonb" : "*";
    String[] fieldList = {field};

    PgUtil.postgresClient(vertxContext, okapiHeaders)
        .get(
            TABLE_NAME_COUNTER_REPORTS,
            CounterReport.class,
            fieldList,
            cql,
            true,
            false,
            ar -> {
              if (ar.succeeded()) {
                CounterReports counterReports = new CounterReports();
                List<CounterReport> reportList = ar.result().getResults();
                counterReports.setCounterReports(reportList);
                counterReports.setTotalRecords(ar.result().getResultInfo().getTotalRecords());
                asyncResultHandler.handle(
                    succeededFuture(
                        GetCounterReportsResponse.respond200WithApplicationJson(counterReports)));
              } else {
                ValidationHelper.handleError(ar.cause(), asyncResultHandler);
              }
            });
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

  @Override
  public void getCounterReportsDownloadById(
      String id,
      Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {

    Promise<Response> promise = Promise.promise();
    getCounterReportsById(id, null, okapiHeaders, promise, vertxContext);

    promise
        .future()
        .compose(
            resp -> {
              Object entity = resp.getEntity();
              if (entity instanceof CounterReport) {
                CounterReport report = (CounterReport) entity;
                return executeBlocking(
                    vertxContext,
                    () ->
                        Optional.ofNullable(createDownloadResponseByReportVersion(report))
                            .orElse(
                                GetCounterReportsDownloadByIdResponse.respond500WithTextPlain(
                                    "Error while downloading report")));
              } else {
                return succeededFuture(resp);
              }
            })
        .onComplete(asyncResultHandler);
  }

  // index: counter_reports_custom_getcsv_idx
  @Override
  @Validate
  public void getCounterReportsSortedByUdpId(
      String udpId,
      Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {
    logger.debug("Getting sorted counter reports");
    logger.debug("Headers present are: {}", okapiHeaders::toString);

    Criteria updCrit = new Criteria();
    updCrit.addField("'providerId'").setOperation("=").setVal(udpId).setJSONB(true);
    Criterion criterion = new Criterion(updCrit);
    CQLWrapper cql = new CQLWrapper(criterion);

    PgUtil.postgresClient(vertxContext, okapiHeaders)
        .get(
            TABLE_NAME_COUNTER_REPORTS,
            CounterReport.class,
            new String[] {"jsonb - 'report' AS jsonb"},
            cql,
            true,
            false,
            reply -> {
              if (reply.succeeded()) {
                List<CounterReport> reports = reply.result().getResults();
                CounterReportsSorted counterReportsSorted = sortByYearAndType(reports);
                asyncResultHandler.handle(
                    succeededFuture(
                        GetCounterReportsSortedByUdpIdResponse.respond200WithApplicationJson(
                            counterReportsSorted)));
              } else {
                ValidationHelper.handleError(reply.cause(), asyncResultHandler);
              }
            });
  }

  private CounterReportsSorted sortByYearAndType(List<CounterReport> reports) {
    CounterReportsSorted result = new CounterReportsSorted();

    Map<String, List<CounterReport>> groupedPerYear =
        reports.stream()
            .collect(Collectors.groupingBy(report -> report.getYearMonth().substring(0, 4)));

    List<CounterReportsPerYear> reportsYear = new ArrayList<>();
    groupedPerYear.forEach(
        (year, reportsOfYear) -> {
          CounterReportsPerYear counterReportsPerYear = new CounterReportsPerYear();
          counterReportsPerYear.setYear(Integer.parseInt(year));

          Map<String, List<CounterReport>> groupedPerType =
              reportsOfYear.stream().collect(Collectors.groupingBy(CounterReport::getReportName));
          List<ReportsPerType> typedReports = new ArrayList<>();
          groupedPerType.forEach(
              (type, reportsTyped) -> {
                ReportsPerType reportsPerType = new ReportsPerType();
                reportsPerType.setReportType(type);
                reportsPerType.setCounterReports(reportsTyped);
                typedReports.add(reportsPerType);
              });
          counterReportsPerYear.setReportsPerType(typedReports);
          reportsYear.add(counterReportsPerYear);
        });
    reportsYear.sort(compareByYear);
    result.setCounterReportsPerYear(reportsYear);
    return result;
  }

  @Override
  @Validate
  public void postCounterReportsUploadProviderById(
      String id,
      boolean overwrite,
      CounterReportDocument entity,
      Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {

    Boolean isEditedManually = entity.getReportMetadata().getReportEditedManually();
    String editReason = entity.getReportMetadata().getEditReason();
    decodeBase64Report(entity.getContents().getData(), vertxContext)
        .onSuccess(
            counterReports ->
                PgHelper.getUDPfromDbById(vertxContext, okapiHeaders, id)
                    .compose(
                        udp -> {
                          counterReports.forEach(
                              cr -> {
                                cr.setEditReason(editReason);
                                cr.setReportEditedManually(isEditedManually);
                                cr.withProviderId(udp.getId())
                                    .withDownloadTime(Date.from(Instant.now()));
                              });
                          return succeededFuture(counterReports);
                        })
                    .compose(
                        crs ->
                            PgHelper.saveCounterReportsToDb(
                                vertxContext, okapiHeaders, crs, overwrite))
                    .onSuccess(
                        reportIds ->
                            asyncResultHandler.handle(
                                succeededFuture(
                                    PostCounterReportsUploadProviderByIdResponse
                                        .respond200WithTextPlain(
                                            String.format(
                                                "Saved report with ids: %s",
                                                String.join(",", reportIds))))))
                    .onFailure(
                        throwable ->
                            asyncResultHandler.handle(
                                succeededFuture(
                                    PostCounterReportsUploadProviderByIdResponse
                                        .respond500WithTextPlain(
                                            String.format("Error saving report: %s", throwable))))))
        .onFailure(
            throwable ->
                asyncResultHandler.handle(
                    succeededFuture(
                        PostCounterReportsUploadProviderByIdResponse.respond400WithTextPlain(
                            String.format("Error saving report: %s", throwable)))));
  }

  @Override
  public void getCounterReportsErrorsCodes(
      Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {
    PgHelper.getErrorCodes(vertxContext, okapiHeaders)
        .onComplete(
            ar -> {
              if (ar.succeeded()) {
                org.folio.rest.jaxrs.model.ErrorCodes result = ar.result();
                asyncResultHandler.handle(
                    succeededFuture(
                        GetCounterReportsErrorsCodesResponse.respond200WithApplicationJson(
                            result)));
              } else {
                asyncResultHandler.handle(
                    succeededFuture(
                        GetCounterReportsErrorsCodesResponse.respond500WithTextPlain(ar.cause())));
              }
            });
  }

  @Override
  public void getCounterReportsReportsTypes(
      Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {
    PgHelper.getReportTypes(vertxContext, okapiHeaders)
        .onComplete(
            ar -> {
              if (ar.succeeded()) {
                org.folio.rest.jaxrs.model.ReportTypes result = ar.result();
                asyncResultHandler.handle(
                    succeededFuture(
                        GetCounterReportsReportsTypesResponse.respond200WithApplicationJson(
                            result)));
              } else {
                asyncResultHandler.handle(
                    succeededFuture(
                        GetCounterReportsReportsTypesResponse.respond500WithTextPlain(ar.cause())));
              }
            });
  }

  @Override
  public void postCounterReportsReportsDelete(
      List<String> entity,
      Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {

    UUID[] uuids;
    try {
      uuids =
          entity.stream().map(UUID::fromString).collect(Collectors.toList()).toArray(UUID[]::new);
    } catch (Exception e) {
      asyncResultHandler.handle(
          succeededFuture(
              PostCounterReportsReportsDeleteResponse.respond400WithTextPlain(e.toString())));
      return;
    }

    PgUtil.postgresClient(vertxContext, okapiHeaders)
        .execute(
            "DELETE FROM " + TABLE_NAME_COUNTER_REPORTS + " WHERE id = ANY ($1)",
            Tuple.of(uuids),
            h -> {
              if (h.succeeded()) {
                asyncResultHandler.handle(
                    succeededFuture(PostCounterReportsReportsDeleteResponse.respond204()));
              } else {
                asyncResultHandler.handle(
                    succeededFuture(
                        PostCounterReportsReportsDeleteResponse.respond500WithTextPlain(
                            h.cause().toString())));
              }
            });
  }

  @Override
  public void getCounterReportsExportById(
      String id,
      String format,
      Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {

    PgUtil.postgresClient(vertxContext, okapiHeaders)
        .getById(
            TABLE_NAME_COUNTER_REPORTS,
            id,
            CounterReport.class,
            ar -> {
              if (ar.succeeded()) {
                executeBlocking(
                        vertxContext, () -> createExportResponseByFormat(ar.result(), format))
                    .onSuccess(resp -> asyncResultHandler.handle(succeededFuture(resp)))
                    .onFailure(
                        t ->
                            asyncResultHandler.handle(
                                succeededFuture(
                                    GetCounterReportsExportByIdResponse.respond500WithTextPlain(
                                        t.getMessage()))));
              } else {
                ValidationHelper.handleError(ar.cause(), asyncResultHandler);
              }
            });
  }

  @Override
  public void
      getCounterReportsExportProviderReportVersionFromToByIdAndNameAndAversionAndBeginAndEnd(
          String id,
          String name,
          String aversion,
          String begin,
          String end,
          String format,
          Map<String, String> okapiHeaders,
          Handler<AsyncResult<Response>> asyncResultHandler,
          Context vertxContext) {

    CQLWrapper cql = createGetMultipleReportsCQL(id, name, aversion, begin, end);
    PgUtil.postgresClient(vertxContext, okapiHeaders)
        .get(
            TABLE_NAME_COUNTER_REPORTS,
            CounterReport.class,
            cql,
            false,
            ar -> {
              if (ar.succeeded()) {
                executeBlocking(
                        vertxContext,
                        () ->
                            createExportMultipleMonthsResponseByReportVersion(
                                ar.result().getResults(), name, format, aversion))
                    .onSuccess(resp -> asyncResultHandler.handle(succeededFuture(resp)))
                    .onFailure(
                        t ->
                            asyncResultHandler.handle(
                                succeededFuture(
                                    GetCounterReportsExportProviderReportVersionFromToByIdAndNameAndAversionAndBeginAndEndResponse
                                        .respond500WithTextPlain(t.getMessage()))));
              } else {
                ValidationHelper.handleError(ar.cause(), asyncResultHandler);
              }
            });
  }

  private Future<List<CounterReport>> decodeBase64Report(String encodedData, Context vertxContext) {
    Promise<List<CounterReport>> result = Promise.promise();
    executeBlocking(
            vertxContext,
            () -> {
              try {
                String[] base64Splitted = encodedData.split(",");
                if (base64Splitted.length < 2) {
                  throw new ReportUploadException(new Throwable("Report is empty."));
                }
                byte[] reportAsBytes = Base64.getDecoder().decode(base64Splitted[1]);
                return UploadHelper.getCounterReportsFromString(new String(reportAsBytes));
              } catch (Exception e) {
                throw new ReportUploadException(e);
              }
            })
        .onSuccess(result::complete)
        .onFailure(result::fail);
    return result.future();
  }

  private static class ReportUploadException extends RuntimeException {

    public ReportUploadException(Throwable cause) {
      super(cause);
    }
  }
}
