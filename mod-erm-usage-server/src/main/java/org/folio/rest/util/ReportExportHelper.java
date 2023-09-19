package org.folio.rest.util;

import static io.vertx.core.Future.succeededFuture;
import static org.folio.rest.util.VertxUtil.executeBlocking;

import com.google.common.io.ByteStreams;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.Json;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;
import javax.ws.rs.core.Response;
import org.folio.rest.jaxrs.model.CounterReport;
import org.folio.rest.jaxrs.resource.CounterReports.GetCounterReportsDownloadByIdResponse;
import org.folio.rest.jaxrs.resource.CounterReports.GetCounterReportsExportByIdResponse;
import org.folio.rest.jaxrs.resource.CounterReports.GetCounterReportsExportProviderReportVersionFromToByIdAndNameAndAversionAndBeginAndEndResponse;
import org.folio.rest.persist.Criteria.Criteria;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.cql.CQLWrapper;
import org.folio.rest.tools.utils.BinaryOutStream;
import org.niso.schemas.counter.Report;
import org.olf.erm.usage.counter.common.ExcelUtil;
import org.olf.erm.usage.counter41.Counter4Utils;
import org.olf.erm.usage.counter50.Counter5Utils;
import org.olf.erm.usage.counter50.Counter5Utils.Counter5UtilsException;

public class ReportExportHelper {

  public static final String CREATED_BY_SUFFIX = "via FOLIO eUsage app";
  public static final List<String> SUPPORTED_VIEWS =
      List.of("DR_D1", "TR_B1", "TR_B3", "TR_J1", "TR_J3", "TR_J4");
  private static final List<String> SUPPORTED_FORMATS = List.of("csv", "xlsx");
  private static final String UNSUPPORTED_COUNTER_VERSION_MSG =
      "Requested counter version \"%s\" is not supported.";
  private static final String UNSUPPORTED_FORMAT_MSG = "Requested format \"%s\" is not supported.";
  private static final String XLSX_ERR_MSG = "An error occured while creating xlsx data: %s";

  private ReportExportHelper() {}

  public static CQLWrapper createGetMultipleReportsCQL(
      String providerId,
      String reportName,
      String reportVersion,
      String beginMonth,
      String endMonth) {
    // fetch the master report if a view is requested
    if ("5".equals(reportVersion) && SUPPORTED_VIEWS.contains(reportName.toUpperCase())) {
      reportName = reportName.substring(0, 2);
    }
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

  public static Response createDownloadResponseByReportVersion(CounterReport report) {
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

  private static Optional<String> csvMapper(CounterReport cr) {
    return Optional.ofNullable(cr)
        .map(CounterReport::getReport)
        .map(
            report -> {
              if ("4".equals(cr.getRelease())) return counter4ReportToCsv(cr);
              if ("5".equals(cr.getRelease())) return counter5ReportToCsv(cr);
              return null;
            });
  }

  private static Object internalReportToCOP5Report(CounterReport report) {
    try {
      return Counter5Utils.fromJSON(Json.encode(report.getReport()));
    } catch (Counter5UtilsException e) {
      throw new CounterReportAPIRuntimeException(e);
    }
  }

  private static String counter4ReportToCsv(CounterReport counterReport) {
    Report report = Counter4Utils.fromJSON(Json.encode(counterReport.getReport()));
    return report == null ? null : Counter4Utils.toCSV(report);
  }

  private static String counter5ReportToCsv(CounterReport counterReport) {
    Object report = ReportExportHelper.internalReportToCOP5Report(counterReport);
    return report == null ? null : replaceCreatedBy(Counter5Utils.toCSV(report));
  }

  public static String replaceCreatedBy(String csvReport) {
    if (csvReport == null) return null;
    return csvReport.replaceFirst(
        "(?!.*" + CREATED_BY_SUFFIX + ".*)(Created_By,)(\"?)(.*(?=\")|.*)(\"?)",
        "$1$2$3 " + CREATED_BY_SUFFIX + "$4");
  }

  private static Response createExportMultipleMonthsResponseByFormat(
      String csvString, String format) {
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

  public static Future<Response> createExportMultipleMonthsResponseByReportVersion(
      Context vertxContext,
      RowStream<Row> rowStream,
      String reportName,
      String format,
      String version) {
    if (!SUPPORTED_FORMATS.contains(format)) {
      return succeededFuture(
          GetCounterReportsExportProviderReportVersionFromToByIdAndNameAndAversionAndBeginAndEndResponse
              .respond400WithTextPlain(String.format(UNSUPPORTED_FORMAT_MSG, format)));
    }

    if ("4".equals(version)) {
      Promise<Report> mergedResult = Promise.promise();
      new RowStreamHandlerR4(vertxContext, mergedResult).handle(rowStream);
      return mergedResult
          .future()
          .compose(report -> executeCounter4ToCsv(vertxContext, report))
          .compose(
              csv -> executeCreateExportMultipleMonthsResponseByFormat(vertxContext, format, csv));
    } else if ("5".equals(version)) {
      Promise<Object> mergedResult = Promise.promise();
      new RowStreamHandlerR5(vertxContext, reportName, mergedResult).handle(rowStream);
      return mergedResult
          .future()
          .compose(obj -> executeCounter5ToCsv(vertxContext, obj))
          .compose(str -> executeReplaceCreatedBy(vertxContext, str))
          .compose(
              csv -> executeCreateExportMultipleMonthsResponseByFormat(vertxContext, format, csv));
    } else {
      return succeededFuture(
          GetCounterReportsExportProviderReportVersionFromToByIdAndNameAndAversionAndBeginAndEndResponse
              .respond400WithTextPlain(String.format(UNSUPPORTED_COUNTER_VERSION_MSG, version)));
    }
  }

  private static Future<String> executeReplaceCreatedBy(Context vertxContext, String str) {
    return executeBlocking(vertxContext, () -> ReportExportHelper.replaceCreatedBy(str));
  }

  private static Future<String> executeCounter5ToCsv(Context vertxContext, Object obj) {
    return executeBlocking(vertxContext, () -> Counter5Utils.toCSV(obj));
  }

  private static Future<Response> executeCreateExportMultipleMonthsResponseByFormat(
      Context vertxContext, String format, String csv) {
    return executeBlocking(
        vertxContext, () -> createExportMultipleMonthsResponseByFormat(csv, format));
  }

  private static Future<String> executeCounter4ToCsv(Context vertxContext, Report report) {
    return executeBlocking(vertxContext, () -> Counter4Utils.toCSV(report));
  }

  public static Response createExportResponseByFormat(CounterReport cr, String format) {
    if (!SUPPORTED_FORMATS.contains(format)) {
      return GetCounterReportsExportByIdResponse.respond400WithTextPlain(
          String.format(UNSUPPORTED_FORMAT_MSG, format));
    }
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
                "No report data or no csv mapper available"));
  }

  private static class CounterReportAPIRuntimeException extends RuntimeException {

    public CounterReportAPIRuntimeException(Throwable cause) {
      super(cause);
    }
  }
}
