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
import java.io.StringWriter;
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
import org.olf.erm.usage.counter51.Counter51Utils;

public class ReportExportHelper {

  public static final String CREATED_BY_SUFFIX = "via FOLIO eUsage app";
  public static final List<String> SUPPORTED_VIEWS =
      List.of("DR_D1", "TR_B1", "TR_B3", "TR_J1", "TR_J3", "TR_J4");
  public static final String UNSUPPORTED_FORMAT_MSG = "Requested format \"%s\" is not supported.";
  public static final String NO_REPORT_DATA = "Entity does not contain report data";
  public static final String NO_CSV_MAPPER_AVAILABLE = "No csv mapper available";
  public static final String UNSUPPORTED_COUNTER_VERSION_MSG =
      "Requested counter version \"%s\" is not supported.";
  private static final List<String> SUPPORTED_FORMATS = List.of("csv", "xlsx");
  private static final String XLSX_ERR_MSG = "An error occured while creating xlsx data: %s";

  private ReportExportHelper() {}

  public static CQLWrapper createGetMultipleReportsCQL(
      String providerId,
      String reportName,
      String reportVersion,
      String beginMonth,
      String endMonth) {
    // fetch the master report if a view is requested
    reportName = reportName.split("_", 2)[0];
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
      return Optional.ofNullable(report.getReport())
          .map(Json::encode)
          .map(Counter4Utils::toXML)
          .map(GetCounterReportsDownloadByIdResponse::respond200WithApplicationXml)
          .orElse(null);
    }
    return Optional.ofNullable(report.getReport())
        .map(Json::encode)
        .map(GetCounterReportsDownloadByIdResponse::respond200WithApplicationJson)
        .orElse(null);
  }

  private static String createCsvFromCounterReport(CounterReport cr) throws IOException {
    return switch (cr.getRelease()) {
      case "4" -> counter4ReportToCsv(cr);
      case "5" -> counter5ReportToCsv(cr);
      case "5.1" -> counter51ReportToCsv(cr);
      default -> null;
    };
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

  private static String counter51ReportToCsv(CounterReport counterReport) throws IOException {
    StringWriter stringWriter = new StringWriter();
    Counter51Utils.writeReportAsCsv(counterReport.getReport(), stringWriter);
    return replaceCreatedBy(stringWriter.toString());
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
    } else if ("5.1".equals(version)) {
      Promise<Object> mergedResult = Promise.promise();
      new RowStreamHandlerR51(vertxContext, reportName, mergedResult).handle(rowStream);
      return mergedResult
          .future()
          .compose(obj -> executeCounter51ToCsv(vertxContext, obj))
          .compose(str -> executeReplaceCreatedBy(vertxContext, str))
          .compose(
              csv -> executeCreateExportMultipleMonthsResponseByFormat(vertxContext, format, csv));
    } else {
      Promise<Response> promise = Promise.promise();
      rowStream.handler(row -> {});
      rowStream.endHandler(
          v ->
              promise.complete(
                  GetCounterReportsExportProviderReportVersionFromToByIdAndNameAndAversionAndBeginAndEndResponse
                      .respond400WithTextPlain(
                          String.format(UNSUPPORTED_COUNTER_VERSION_MSG, version))));
      return promise.future();
    }
  }

  private static Future<String> executeReplaceCreatedBy(Context vertxContext, String str) {
    return executeBlocking(vertxContext, () -> ReportExportHelper.replaceCreatedBy(str));
  }

  private static Future<String> executeCounter5ToCsv(Context vertxContext, Object obj) {
    return executeBlocking(vertxContext, () -> Counter5Utils.toCSV(obj));
  }

  private static Future<String> executeCounter51ToCsv(Context vertxContext, Object obj) {
    return executeBlocking(
        vertxContext,
        () -> {
          StringWriter stringWriter = new StringWriter();
          try {
            Counter51Utils.writeReportAsCsv(obj, stringWriter);
            return stringWriter.toString();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
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

    if (cr == null) {
      return GetCounterReportsExportByIdResponse.respond404();
    }

    if (cr.getReport() == null) {
      return GetCounterReportsExportByIdResponse.respond422WithTextPlain(NO_REPORT_DATA);
    }

    String csvString;
    try {
      csvString = createCsvFromCounterReport(cr);
    } catch (Exception e) {
      return GetCounterReportsExportByIdResponse.respond500WithTextPlain(e.getMessage());
    }

    return Optional.ofNullable(csvString)
        .map(
            s -> {
              if ("xlsx".equals(format)) {
                try {
                  InputStream in = ExcelUtil.fromCSV(s);
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
              return GetCounterReportsExportByIdResponse.respond200WithTextCsv(s);
            })
        .orElse(
            GetCounterReportsExportByIdResponse.respond500WithTextPlain(NO_CSV_MAPPER_AVAILABLE));
  }

  private static class CounterReportAPIRuntimeException extends RuntimeException {

    public CounterReportAPIRuntimeException(Throwable cause) {
      super(cause);
    }
  }
}
