package org.folio.rest.util;

import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
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
import org.folio.rest.persist.Criteria.Criteria;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.cql.CQLWrapper;
import org.folio.rest.tools.utils.BinaryOutStream;
import org.niso.schemas.counter.Report;
import org.olf.erm.usage.counter.common.ExcelUtil;
import org.olf.erm.usage.counter41.Counter4Utils;
import org.olf.erm.usage.counter41.Counter4Utils.ReportMergeException;
import org.olf.erm.usage.counter50.Counter5Utils;
import org.olf.erm.usage.counter50.Counter5Utils.Counter5UtilsException;
import org.olf.erm.usage.counter50.converter.Converter;
import org.olf.erm.usage.counter50.converter.ReportConverter;

public class ReportExportHelper {

  public static final String CREATED_BY_SUFFIX = "via FOLIO eUsage app";
  private static final List<String> SUPPORTED_FORMATS = List.of("csv", "xlsx");
  private static final List<String> SUPPORTED_VIEWS =
      List.of("DR_D1", "TR_B1", "TR_B3", "TR_J1", "TR_J3", "TR_J4");
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

  private static Optional<String> csvMapper(CounterReport cr)
      throws Counter5UtilsException, ReportMergeException {
    if (cr.getRelease().equals("4") && cr.getReport() != null) {
      return Optional.ofNullable(counter4ReportsToCsv(List.of(cr)));
    } else if (cr.getRelease().equals("5") && cr.getReport() != null) {
      return Optional.ofNullable(counter5ReportsToCsv(List.of(cr)));
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
    return replaceCreatedBy(Counter5Utils.toCSV(merge));
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

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static void convertR5Reports(List<CounterReport> reports, String reportName) {
    Gson gson = new Gson();
    Converter converter = ReportConverter.create(reportName);
    reports.forEach(
        r -> {
          r.setReportName(reportName);
          Object report = internalReportToCOP5Report(r);
          Object converted = converter.convert(report);
          r.setReport(
              Json.decodeValue(gson.toJson(converted), org.folio.rest.jaxrs.model.Report.class));
        });
  }

  public static Response createExportMultipleMonthsResponseByReportVersion(
      List<CounterReport> reportList, String reportName, String format, String version) {
    if (!SUPPORTED_FORMATS.contains(format)) {
      return GetCounterReportsExportProviderReportVersionFromToByIdAndNameAndAversionAndBeginAndEndResponse
          .respond400WithTextPlain(String.format(UNSUPPORTED_FORMAT_MSG, format));
    }

    String csv;
    try {
      if (version.equals("4")) {
        csv = counter4ReportsToCsv(reportList);
      } else if (version.equals("5")) {
        if (SUPPORTED_VIEWS.contains(reportName.toUpperCase())) {
          convertR5Reports(reportList, reportName);
        }
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

    if (!SUPPORTED_FORMATS.contains(format)) {
      return GetCounterReportsExportByIdResponse.respond400WithTextPlain(
          String.format(UNSUPPORTED_FORMAT_MSG, format));
    }
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
    } catch (Counter5UtilsException | ReportMergeException e) {
      return GetCounterReportsExportByIdResponse.respond500WithTextPlain(e.getMessage());
    }
  }

  private static class CounterReportAPIRuntimeException extends RuntimeException {

    public CounterReportAPIRuntimeException(Throwable cause) {
      super(cause);
    }
  }
}