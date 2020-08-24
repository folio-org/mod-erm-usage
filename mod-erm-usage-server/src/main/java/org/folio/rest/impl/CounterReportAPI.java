package org.folio.rest.impl;

import static io.vertx.core.Future.succeededFuture;
import static org.folio.rest.util.Constants.TABLE_NAME_COUNTER_REPORTS;

import com.google.common.io.ByteStreams;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.json.Json;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.folio.rest.tools.utils.BinaryOutStream;
import org.folio.rest.tools.utils.ValidationHelper;
import org.folio.rest.util.Constants;
import org.folio.rest.util.PgHelper;
import org.folio.rest.util.UploadHelper;
import org.niso.schemas.counter.Report;
import org.olf.erm.usage.counter.common.ExcelUtil;
import org.olf.erm.usage.counter41.Counter4Utils;
import org.olf.erm.usage.counter41.Counter4Utils.ReportMergeException;
import org.olf.erm.usage.counter50.Counter5Utils;
import org.olf.erm.usage.counter50.Counter5Utils.Counter5UtilsException;

public class CounterReportAPI implements org.folio.rest.jaxrs.resource.CounterReports {

  private static final List<String> SUPPORTED_FORMATS = Arrays.asList("csv", "xlsx");
  private static final String UNSUPPORTED_MSG = "Requested format \"%s\" is not supported.";
  private static final String UNSUPPORTED_COUNTER_VERSION_MSG =
      "Requested counter version \"%s\" is not supported.";
  private static final String XLSX_ERR_MSG = "An error occured while creating xlsx data: %s";
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

  private Response createDownloadResponseByReportVersion(CounterReport report) {
    if (report.getRelease().equals("4")) {
      String xmlReport = Counter4Utils.toXML(Json.encode(report.getReport()));
      return Optional.ofNullable(xmlReport)
          .map(r -> GetCounterReportsDownloadByIdResponse.respond200WithApplicationXml(xmlReport))
          .orElse(null);
    } else if (report.getRelease().equals("5")) {
      String jsonReport = Json.encode(report.getReport());
      return Optional.ofNullable(jsonReport)
          .map(r -> GetCounterReportsDownloadByIdResponse.respond200WithApplicationJson(jsonReport))
          .orElse(null);
    } else {
      return GetCounterReportsDownloadByIdResponse.respond500WithTextPlain(
          String.format("Unsupported report version '%s'", report.getRelease()));
    }
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
        .map(
            resp -> {
              Object entity = resp.getEntity();
              if (entity instanceof CounterReport) {
                CounterReport report = (CounterReport) entity;
                return Optional.ofNullable(createDownloadResponseByReportVersion(report))
                    .orElse(
                        GetCounterReportsDownloadByIdResponse.respond500WithTextPlain(
                            "Error while downloading report"));
              } else {
                return resp;
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

  private Optional<String> csvMapper(CounterReport cr) throws Counter5UtilsException {
    if (cr.getRelease().equals("4") && cr.getReport() != null) {
      return Optional.ofNullable(
          Counter4Utils.toCSV(Counter4Utils.fromJSON(Json.encode(cr.getReport()))));
    } else if (cr.getRelease().equals("5") && cr.getReport() != null) {
      return Optional.ofNullable(
          Counter5Utils.toCSV(Counter5Utils.fromJSON(Json.encode(cr.getReport()))));
    }
    return Optional.empty();
  }

  /** @deprecated As of 2.9.0 */
  @Deprecated
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

                try {
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
                } catch (Counter5UtilsException e) {
                  ValidationHelper.handleError(e, asyncResultHandler);
                }
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

    PgHelper.getUDPfromDbById(vertxContext, okapiHeaders, id)
        .compose(
            udp -> {
              counterReports.forEach(
                  cr -> cr.withProviderId(udp.getId()).withDownloadTime(Date.from(Instant.now())));
              return succeededFuture(counterReports);
            })
        .compose(crs -> PgHelper.saveCounterReportsToDb(vertxContext, okapiHeaders, crs, overwrite))
        .onComplete(
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

  // index: counter_reports_custom_getcsv_idx
  /** @deprecated As of 2.9.0 */
  @Deprecated
  @Override
  public void getCounterReportsCsvProviderReportVersionFromToByIdAndNameAndAversionAndBeginAndEnd(
      String id,
      String name,
      String aversion,
      String begin,
      String end,
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
                String csv;
                try {
                  if (aversion.equals("4")) {
                    csv = counter4ReportsToCsv(ar.result().getResults());
                  } else if (aversion.equals("5")) {
                    csv = counter5ReportsToCsv(ar.result().getResults());
                  } else {
                    throw new CounterReportAPIException("Unknown counter version:" + aversion);
                  }
                } catch (Exception e) {
                  ValidationHelper.handleError(e, asyncResultHandler);
                  return;
                }
                asyncResultHandler.handle(
                    succeededFuture(
                        GetCounterReportsCsvProviderReportVersionFromToByIdAndNameAndAversionAndBeginAndEndResponse
                            .respond200WithTextCsv(csv)));
              } else {
                ValidationHelper.handleError(ar.cause(), asyncResultHandler);
              }
            });
  }

  private Response createExportResponseByFormat(CounterReport cr, String format) {
    try {
      return csvMapper(cr)
          .map(
              csvString -> {
                if ("xlsx".equals(format)) {
                  try {
                    InputStream in = ExcelUtil.fromCSV(csvString);
                    BinaryOutStream bos = new BinaryOutStream();
                    bos.setData(ByteStreams.toByteArray(in));
                    return GetCounterReportsExportByIdResponse
                        .respond200WithApplicationVndOpenxmlformatsOfficedocumentSpreadsheetmlSheet(
                            bos);
                  } catch (IOException e) {
                    return GetCounterReportsExportByIdResponse.respond500WithTextPlain(
                        String.format(XLSX_ERR_MSG, e.getMessage()));
                  }
                }
                return GetCounterReportsExportByIdResponse.respond200WithTextCsv(csvString);
              })
          .orElse(
              GetCounterReportsExportByIdResponse.respond500WithTextPlain(
                  "No report data or no mapper available"));
    } catch (Counter5UtilsException e) {
      return GetCounterReportsExportByIdResponse.respond500WithTextPlain(e.getMessage());
    }
  }

  private Response createExportMultipleMonthsResponseByFormat(String csvString, String format) {
    if ("xlsx".equals(format)) {
      try {
        InputStream in = ExcelUtil.fromCSV(csvString);
        BinaryOutStream bos = new BinaryOutStream();
        bos.setData(ByteStreams.toByteArray(in));
        return GetCounterReportsExportProviderReportVersionFromToByIdAndNameAndAversionAndBeginAndEndResponse
            .respond200WithApplicationVndOpenxmlformatsOfficedocumentSpreadsheetmlSheet(bos);
      } catch (IOException e) {
        return GetCounterReportsExportProviderReportVersionFromToByIdAndNameAndAversionAndBeginAndEndResponse
            .respond500WithTextPlain(String.format(XLSX_ERR_MSG, e.getMessage()));
      }
    }
    return GetCounterReportsExportProviderReportVersionFromToByIdAndNameAndAversionAndBeginAndEndResponse
        .respond200WithTextCsv(csvString);
  }

  @Override
  public void getCounterReportsExportById(
      String id,
      String format,
      Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {

    if (SUPPORTED_FORMATS.contains(format)) {
      PgUtil.postgresClient(vertxContext, okapiHeaders)
          .getById(
              TABLE_NAME_COUNTER_REPORTS,
              id,
              CounterReport.class,
              ar -> {
                if (ar.succeeded()) {
                  Response response = createExportResponseByFormat(ar.result(), format);
                  asyncResultHandler.handle(succeededFuture(response));
                } else {
                  ValidationHelper.handleError(ar.cause(), asyncResultHandler);
                }
              });
    } else {
      asyncResultHandler.handle(
          succeededFuture(
              GetCounterReportsExportByIdResponse.respond400WithTextPlain(
                  String.format(UNSUPPORTED_MSG, format))));
    }
  }

  private Response createExportMultipleMonthsResponseByReportVersion(
      List<CounterReport> reportList, String format, String version) {
    String csv;
    try {
      if (version.equals("4")) {
        csv = counter4ReportsToCsv(reportList);
      } else if (version.equals("5")) {
        csv = counter5ReportsToCsv(reportList);
      } else {
        return GetCounterReportsExportProviderReportVersionFromToByIdAndNameAndAversionAndBeginAndEndResponse
            .respond400WithTextPlain(String.format(UNSUPPORTED_COUNTER_VERSION_MSG, version));
      }
    } catch (Exception e) {
      return GetCounterReportsExportProviderReportVersionFromToByIdAndNameAndAversionAndBeginAndEndResponse
          .respond500WithTextPlain(e.getMessage());
    }
    return createExportMultipleMonthsResponseByFormat(csv, format);
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

    if (SUPPORTED_FORMATS.contains(format)) {
      CQLWrapper cql = createGetMultipleReportsCQL(id, name, aversion, begin, end);
      PgUtil.postgresClient(vertxContext, okapiHeaders)
          .get(
              TABLE_NAME_COUNTER_REPORTS,
              CounterReport.class,
              cql,
              false,
              ar -> {
                if (ar.succeeded()) {
                  Response response =
                      createExportMultipleMonthsResponseByReportVersion(
                          ar.result().getResults(), format, aversion);
                  asyncResultHandler.handle(succeededFuture(response));
                } else {
                  ValidationHelper.handleError(ar.cause(), asyncResultHandler);
                }
              });
    } else {
      asyncResultHandler.handle(
          succeededFuture(
              GetCounterReportsExportProviderReportVersionFromToByIdAndNameAndAversionAndBeginAndEndResponse
                  .respond400WithTextPlain(String.format(UNSUPPORTED_MSG, format))));
    }
  }

  private CQLWrapper createGetMultipleReportsCQL(
      String providerId,
      String reportName,
      String reportVersion,
      String beginMonth,
      String endMonth) {
    Criteria providerCrit =
        new Criteria()
            .addField(Constants.FIELD_NAME_PROVIDER_ID)
            .setOperation(Constants.OPERATOR_EQUALS)
            .setVal(providerId);
    Criteria reportNameCrit =
        new Criteria()
            .addField(Constants.FIELD_NAME_REPORT_NAME)
            .setOperation(Constants.OPERATOR_EQUALS)
            .setVal(reportName);
    Criteria releaseCrit =
        new Criteria()
            .addField(Constants.FIELD_NAME_RELEASE)
            .setOperation(Constants.OPERATOR_EQUALS)
            .setVal(reportVersion);
    Criteria reportCrit =
        new Criteria().addField("jsonb").setJSONB(false).setOperation("?").setVal("report");
    Criteria yearMonthBeginCrit =
        new Criteria()
            .addField(Constants.FIELD_NAME_YEAR_MONTH)
            .setOperation(">=")
            .setVal(beginMonth);
    Criteria yearMonthEndCrit =
        new Criteria()
            .addField(Constants.FIELD_NAME_YEAR_MONTH)
            .setOperation("<=")
            .setVal(endMonth);
    Criterion criterion =
        new Criterion()
            .addCriterion(providerCrit)
            .addCriterion(reportNameCrit)
            .addCriterion(releaseCrit)
            .addCriterion(reportCrit)
            .addCriterion(yearMonthBeginCrit)
            .addCriterion(yearMonthEndCrit);
    return new CQLWrapper(criterion);
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
            .map(this::internalReportToCOP5Report)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    Object merge = Counter5Utils.merge(c5Reports);
    return Counter5Utils.toCSV(merge);
  }

  private Object internalReportToCOP5Report(CounterReport report) {
    try {
      return Counter5Utils.fromJSON(Json.encode(report.getReport()));
    } catch (Counter5UtilsException e) {
      throw new CounterReportAPIRuntimeException(e);
    }
  }

  private static class CounterReportAPIException extends Exception {

    public CounterReportAPIException(String message) {
      super(message);
    }
  }

  private static class CounterReportAPIRuntimeException extends RuntimeException {

    public CounterReportAPIRuntimeException(Throwable cause) {
      super(cause);
    }
  }
}
