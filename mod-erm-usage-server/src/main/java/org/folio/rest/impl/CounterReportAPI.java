package org.folio.rest.impl;

import static io.vertx.core.Future.succeededFuture;
import static org.folio.rest.util.Constants.TABLE_NAME_COUNTER_REPORTS;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.json.Json;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.ws.rs.core.Response;
import org.folio.cql2pgjson.CQL2PgJSON;
import org.folio.cql2pgjson.exception.FieldException;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.rest.annotations.Validate;
import org.folio.rest.jaxrs.model.CounterReport;
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
import org.folio.rest.util.Constants;
import org.folio.rest.util.PgHelper;
import org.folio.rest.util.UploadHelper;
import org.niso.schemas.counter.Report;
import org.olf.erm.usage.counter41.Counter4Utils;
import org.olf.erm.usage.counter41.Counter4Utils.ReportMergeException;
import org.olf.erm.usage.counter50.Counter5Utils;
import org.olf.erm.usage.counter50.Counter5Utils.Counter5UtilsException;

public class CounterReportAPI implements org.folio.rest.jaxrs.resource.CounterReports {

  private final Logger logger = LoggerFactory.getLogger(CounterReportAPI.class);

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
    logger.debug("Headers present are: " + okapiHeaders.toString());

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
  @Validate
  public void getCounterReportsSortedByUdpId(
      String udpId,
      Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {
    logger.debug("Getting sorted counter reports");
    logger.debug("Headers present are: " + okapiHeaders.toString());

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

  private Optional<String> csvMapper(CounterReport cr) {
    if (cr.getRelease().equals("4") && cr.getReport() != null) {
      return Optional.ofNullable(
          Counter4Utils.toCSV(Counter4Utils.fromJSON(Json.encode(cr.getReport()))));
    } else if (cr.getRelease().equals("5") && cr.getReport() != null) {
      return Optional.ofNullable(
          Counter5Utils.toCSV(Counter5Utils.fromJSON(Json.encode(cr.getReport()))));
    }
    return Optional.empty();
  }

  @Override
  public void getCounterReportsCsvById(
      String id,
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
                asyncResultHandler.handle(
                    csvMapper(ar.result())
                        .<AsyncResult<Response>>map(
                            csv ->
                                succeededFuture(
                                    GetCounterReportsCsvByIdResponse.respond200WithTextCsv(csv)))
                        .orElse(
                            succeededFuture(
                                GetCounterReportsCsvByIdResponse.respond500WithTextPlain(
                                    "No report data or no mapper available"))));
              } else {
                ValidationHelper.handleError(ar.cause(), asyncResultHandler);
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
          succeededFuture(
              PostCounterReportsUploadProviderByIdResponse.respond500WithTextPlain(
                  String.format("Error uploading file: %s", e.getMessage()))));
      return;
    }

    PgHelper.getUDPfromDbById(vertxContext.owner(), tenantId, id)
        .compose(
            udp -> {
              counterReports.forEach(
                  cr -> cr.withProviderId(udp.getId()).withDownloadTime(Date.from(Instant.now())));
              return succeededFuture(counterReports);
            })
        .compose(crs -> PgHelper.saveCounterReportsToDb(vertxContext, tenantId, crs, overwrite))
        .setHandler(
            ar -> {
              if (ar.succeeded()) {
                asyncResultHandler.handle(
                    succeededFuture(
                        PostCounterReportsUploadProviderByIdResponse.respond200WithTextPlain(
                            String.format(
                                "Saved report with ids: %s", String.join(",", ar.result())))));
              } else {
                asyncResultHandler.handle(
                    succeededFuture(
                        PostCounterReportsUploadProviderByIdResponse.respond500WithTextPlain(
                            String.format("Error saving report: %s", ar.cause()))));
              }
            });
  }

  @Override
  public void getCounterReportsErrorsCodes(
      Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {
    String tenantId = okapiHeaders.get(XOkapiHeaders.TENANT);
    PgHelper.getErrorCodes(vertxContext, tenantId)
        .setHandler(
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
        new Criteria().addField("jsonb").setJSONB(false).setOperation("?").setVal("report");
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

    PgUtil.postgresClient(vertxContext, okapiHeaders)
        .get(
            TABLE_NAME_COUNTER_REPORTS,
            CounterReport.class,
            cql,
            false,
            ar -> {
              if (ar.succeeded()) {
                String csv;
                try {
                  if (version.equals("4")) {
                    csv = counter4ReportsToCsv(ar.result().getResults());
                  } else if (version.equals("5")) {
                    csv = counter5ReportsToCsv(ar.result().getResults());
                  } else {
                    throw new CounterReportAPIException("Unknown counter version:" + version);
                  }
                } catch (Exception e) {
                  ValidationHelper.handleError(e, asyncResultHandler);
                  return;
                }
                asyncResultHandler.handle(
                    succeededFuture(
                        GetCounterReportsCsvProviderReportVersionFromToByIdAndNameAndVersionAndBeginAndEndResponse
                            .respond200WithTextCsv(csv)));
              } else {
                ValidationHelper.handleError(ar.cause(), asyncResultHandler);
              }
            });
  }

  private String counter4ReportsToCsv(List<CounterReport> reports) throws ReportMergeException {
    List<Report> c4Reports =
        reports.stream()
            .map(cr -> Counter4Utils.fromJSON(Json.encode(cr.getReport())))
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    Report merge = Counter4Utils.merge(c4Reports);
    return Counter4Utils.toCSV(merge);
  }

  private String counter5ReportsToCsv(List<CounterReport> reports) throws Counter5UtilsException {
    List<Object> c5Reports =
        reports.stream()
            .map(cr -> Counter5Utils.fromJSON(Json.encode(cr.getReport())))
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    Object merge = Counter5Utils.merge(c5Reports);
    return Counter5Utils.toCSV(merge);
  }

  private static class CounterReportAPIException extends Exception {

    public CounterReportAPIException(String message) {
      super(message);
    }
  }
}
