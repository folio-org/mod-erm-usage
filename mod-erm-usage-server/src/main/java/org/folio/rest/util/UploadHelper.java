package org.folio.rest.util;

import io.vertx.core.json.Json;
import java.io.IOException;
import java.time.YearMonth;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.folio.rest.jaxrs.model.CounterReport;
import org.niso.schemas.counter.Report;
import org.olf.erm.usage.counter41.Counter4Utils;
import org.olf.erm.usage.counter41.Counter4Utils.ReportSplitException;
import org.olf.erm.usage.counter41.csv.mapper.MapperException;
import org.olf.erm.usage.counter50.Counter5Utils;
import org.olf.erm.usage.counter50.Counter5Utils.Counter5UtilsException;
import org.openapitools.client.model.COUNTERDatabaseReport;
import org.openapitools.client.model.COUNTERItemReport;
import org.openapitools.client.model.COUNTERPlatformReport;
import org.openapitools.client.model.COUNTERTitleReport;
import org.openapitools.client.model.SUSHIReportHeader;
import org.openapitools.client.model.SUSHIReportHeaderReportAttributes;

public class UploadHelper {

  private static final String ATTRIBUTES_TO_SHOW = "Attributes_To_Show";
  public static final Map<String, List<SUSHIReportHeaderReportAttributes>> SUPPORTED_REPORTS =
      Map.of(
          "Title Master Report",
              List.of(
                  new SUSHIReportHeaderReportAttributes()
                      .name(ATTRIBUTES_TO_SHOW)
                      .value("Data_Type|Section_Type|YOP|Access_Type|Access_Method")),
          "Item Master Report",
              List.of(
                  new SUSHIReportHeaderReportAttributes()
                      .name(ATTRIBUTES_TO_SHOW)
                      .value(
                          "Authors|Publication_Date|Article_Version|Data_Type|YOP|Access_Type|Access_Method"),
                  new SUSHIReportHeaderReportAttributes()
                      .name("Include_Parent_Details")
                      .value("True")),
          "Platform Master Report",
              List.of(
                  new SUSHIReportHeaderReportAttributes()
                      .name(ATTRIBUTES_TO_SHOW)
                      .value("Data_Type|Access_Method")),
          "Database Master Report",
              List.of(
                  new SUSHIReportHeaderReportAttributes()
                      .name(ATTRIBUTES_TO_SHOW)
                      .value("Data_Type|Access_Method")));
  public static final String MSG_WRONG_FORMAT = "Wrong format supplied";
  public static final String MSG_UNSUPPORTED_REPORT = "Unsupported report";

  public static List<CounterReport> getCounterReportsFromString(String content)
      throws FileUploadException, Counter5UtilsException, ReportSplitException {
    String release = getCounterRelease(content);

    List<CounterReport> counterReports;
    if (release.equals("5")) {
      counterReports = getCOP5Reports(content);
    } else {
      counterReports = getCOP4Reports(content);
    }

    if (counterReports.isEmpty()) {
      throw new FileUploadException("No months to process.");
    } else if (counterReports.contains(null)) {
      throw new FileUploadException("Error processing at least one month from supplied report.");
    } else {
      return counterReports;
    }
  }

  private static List<CounterReport> getCOP4Reports(String content)
      throws ReportSplitException, FileUploadException {
    List<CounterReport> result;
    Report report = Counter4Utils.fromString(content);
    if (report == null) {
      try {
        report = Counter4Utils.fromCsvString(content);
      } catch (IOException | MapperException e) {
        throw new FileUploadException(MSG_WRONG_FORMAT + ": " + e.getMessage(), e);
      }
    }

    List<YearMonth> yearMonthsFromReport = Counter4Utils.getYearMonthsFromReport(report);

    List<Report> reports = Collections.singletonList(report);
    if (yearMonthsFromReport.size() != 1) {
      reports = Counter4Utils.split(report);
    }

    Report finalReport = report;
    String reportName =
        Optional.ofNullable(Counter4Utils.getNameForReportTitle(finalReport.getName()))
            .orElseThrow(() -> new FileUploadException(MSG_UNSUPPORTED_REPORT));

    result =
        reports.stream()
            .map(
                r -> {
                  List<YearMonth> months = Counter4Utils.getYearMonthsFromReport(r);

                  if (!months.isEmpty()) {
                    return new CounterReport()
                        .withRelease(finalReport.getVersion())
                        .withReportName(reportName)
                        .withReport(
                            Json.decodeValue(
                                Counter4Utils.toJSON(r), org.folio.rest.jaxrs.model.Report.class))
                        .withYearMonth(months.get(0).toString());
                  } else {
                    return null;
                  }
                })
            .collect(Collectors.toList());
    return result;
  }

  private static void checkThatReportIsSupported(SUSHIReportHeader header)
      throws FileUploadException {
    if (!SUPPORTED_REPORTS.containsKey(header.getReportName())) {
      throw new FileUploadException(MSG_UNSUPPORTED_REPORT);
    }

    List<SUSHIReportHeaderReportAttributes> expectedAttributes =
        SUPPORTED_REPORTS.get(header.getReportName());
    List<SUSHIReportHeaderReportAttributes> actualAttributes = header.getReportAttributes();
    if (!(actualAttributes != null
        && actualAttributes.size() == expectedAttributes.size()
        && actualAttributes.containsAll(expectedAttributes))) {
      throw new FileUploadException(MSG_UNSUPPORTED_REPORT);
    }
  }

  private static List<CounterReport> getCOP5Reports(String content)
      throws FileUploadException, Counter5UtilsException {
    // Counter 5
    Object cop5Report = createCOP5Report(content);
    SUSHIReportHeader header = Counter5Utils.getSushiReportHeaderFromReportObject(cop5Report);

    checkThatReportIsSupported(header);

    List<YearMonth> yearMonthsFromReportCOP5 = Counter5Utils.getYearMonthsFromReportHeader(header);
    List<Object> reports = Collections.singletonList(cop5Report);
    if (yearMonthsFromReportCOP5.size() != 1) {
      reports = Counter5Utils.split(cop5Report);
    }

    return reports.stream()
        .map(
            r -> {
              List<YearMonth> ym = Counter5Utils.getYearMonthFromReport(r);
              if (!ym.isEmpty()) {
                return new CounterReport()
                    .withRelease("5")
                    .withReportName(header.getReportID())
                    .withReport(
                        Json.decodeValue(Json.encode(r), org.folio.rest.jaxrs.model.Report.class))
                    .withYearMonth(ym.get(0).toString());
              } else {
                return null;
              }
            })
        .collect(Collectors.toList());
  }

  private static Object createCOP5Report(String content) throws FileUploadException {
    Object cop5Report = null;
    try {
      cop5Report = Counter5Utils.fromJSON(content);
    } catch (Exception e) {
      // bad practice, i know...
    }
    if (cop5Report == null) {
      try {
        cop5Report = Counter5Utils.fromCSV(content);
      } catch (org.olf.erm.usage.counter50.csv.mapper.MapperException e) {
        throw new FileUploadException(MSG_WRONG_FORMAT + ": " + e.getMessage(), e);
      }
    }
    return cop5Report;
  }

  private static String getCounterRelease(String content) {
    String release = "4";
    try {
      Object r = Counter5Utils.fromCSV(content);
      SUSHIReportHeader header = Counter5Utils.getSushiReportHeaderFromReportObject(r);
      return header.getRelease();
    } catch (org.olf.erm.usage.counter50.csv.mapper.MapperException | Counter5UtilsException e) {
      // bad practice, but we need to check counter version...
    }

    try {
      Object o = Counter5Utils.fromJSON(content);
      if (o instanceof COUNTERDatabaseReport) {
        return ((COUNTERDatabaseReport) o).getReportHeader().getRelease();
      } else if (o instanceof COUNTERItemReport) {
        return ((COUNTERItemReport) o).getReportHeader().getRelease();
      } else if (o instanceof COUNTERPlatformReport) {
        return ((COUNTERPlatformReport) o).getReportHeader().getRelease();
      } else if (o instanceof COUNTERTitleReport) {
        return ((COUNTERTitleReport) o).getReportHeader().getRelease();
      }
    } catch (Counter5UtilsException e) {
      // I know...
    }
    return release;
  }

  public static class FileUploadException extends Exception {

    private static final long serialVersionUID = -3795351043189447151L;

    public FileUploadException(String message) {
      super(message);
    }

    public FileUploadException(String message, Throwable cause) {
      super(message, cause);
    }

    public FileUploadException(Exception e) {
      super(e);
    }
  }

  private UploadHelper() {}
}
