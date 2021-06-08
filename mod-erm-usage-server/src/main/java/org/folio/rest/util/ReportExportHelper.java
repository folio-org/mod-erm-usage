package org.folio.rest.util;

import com.google.common.io.ByteStreams;
import io.vertx.core.json.Json;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.ws.rs.core.Response;
import org.folio.rest.jaxrs.model.CounterReport;
import org.folio.rest.jaxrs.resource.CounterReports.GetCounterReportsDownloadByIdResponse;
import org.folio.rest.jaxrs.resource.CounterReports.GetCounterReportsExportByIdResponse;
import org.folio.rest.jaxrs.resource.CounterReports.GetCounterReportsExportProviderReportVersionFromToByIdAndNameAndAversionAndBeginAndEndResponse;
import org.folio.rest.tools.utils.BinaryOutStream;
import org.niso.schemas.counter.Report;
import org.olf.erm.usage.counter.common.ExcelUtil;
import org.olf.erm.usage.counter41.Counter4Utils;
import org.olf.erm.usage.counter41.Counter4Utils.ReportMergeException;
import org.olf.erm.usage.counter50.Counter5Utils;
import org.olf.erm.usage.counter50.Counter5Utils.Counter5UtilsException;

public class ReportExportHelper {

  private static final String UNSUPPORTED_COUNTER_VERSION_MSG =
      "Requested counter version \"%s\" is not supported.";
  private static final String XLSX_ERR_MSG = "An error occured while creating xlsx data: %s";

  private ReportExportHelper() {}

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

  private static Optional<String> csvMapper(CounterReport cr) throws Counter5UtilsException {
    if (cr.getRelease().equals("4") && cr.getReport() != null) {
      return Optional.ofNullable(
          Counter4Utils.toCSV(Counter4Utils.fromJSON(Json.encode(cr.getReport()))));
    } else if (cr.getRelease().equals("5") && cr.getReport() != null) {
      return Optional.ofNullable(
          Counter5Utils.toCSV(Counter5Utils.fromJSON(Json.encode(cr.getReport()))));
    }
    return Optional.empty();
  }

  private static Object internalReportToCOP5Report(CounterReport report) {
    try {
      return Counter5Utils.fromJSON(Json.encode(report.getReport()));
    } catch (Counter5UtilsException e) {
      throw new CounterReportAPIRuntimeException(e);
    }
  }

  private static String counter4ReportsToCsv(List<CounterReport> reports)
      throws ReportMergeException {
    List<Report> c4Reports =
        reports.stream()
            .map(cr -> Counter4Utils.fromJSON(Json.encode(cr.getReport())))
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    Report merge = Counter4Utils.merge(c4Reports);
    return Counter4Utils.toCSV(merge);
  }

  private static String counter5ReportsToCsv(List<CounterReport> reports)
      throws Counter5UtilsException {
    List<Object> c5Reports =
        reports.stream()
            .map(ReportExportHelper::internalReportToCOP5Report)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    Object merge = Counter5Utils.merge(c5Reports);
    return Counter5Utils.toCSV(merge);
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

  public static Response createExportMultipleMonthsResponseByReportVersion(
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

  public static Response createExportResponseByFormat(CounterReport cr, String format) {
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

  private static class CounterReportAPIRuntimeException extends RuntimeException {

    public CounterReportAPIRuntimeException(Throwable cause) {
      super(cause);
    }
  }
}
