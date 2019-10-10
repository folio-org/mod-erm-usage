package org.folio.rest.util;

import io.vertx.core.json.Json;
import java.io.InputStream;
import java.time.YearMonth;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.codec.Charsets;
import org.apache.commons.io.IOUtils;
import org.folio.rest.jaxrs.model.CounterReport;
import org.niso.schemas.counter.Report;
import org.olf.erm.usage.counter41.Counter4Utils;
import org.olf.erm.usage.counter41.Counter4Utils.ReportSplitException;
import org.olf.erm.usage.counter50.Counter5Utils;
import org.openapitools.client.model.SUSHIReportHeader;

public class UploadHelper {

  private static final String MSG_EXACTLY_ONE_MONTH =
      "Provided report must cover a period of exactly one month";
  private static final String MSG_WRONG_FORMAT = "Wrong format supplied";

  public static List<CounterReport> getCounterReportsFromInputStream(InputStream entity)
      throws FileUploadException, ReportSplitException {
    String content;
    try {
      content = IOUtils.toString(entity, Charsets.UTF_8);
    } catch (Exception e) {
      throw new FileUploadException(e);
    }

    // Counter 5
    SUSHIReportHeader header = Counter5Utils.getReportHeader(content);
    if (Counter5Utils.isValidReportHeader(header)) {
      List<YearMonth> yearMonths = Counter5Utils.getYearMonthsFromReportHeader(header);
      if (yearMonths.size() != 1) {
        throw new FileUploadException(MSG_EXACTLY_ONE_MONTH);
      }
      return Collections.singletonList(
          new CounterReport()
              .withRelease("5")
              .withReportName(header.getReportID())
              .withReport(Json.decodeValue(content, org.folio.rest.jaxrs.model.Report.class))
              .withYearMonth(yearMonths.get(0).toString()));
    }

    // Counter 4
    Report report = Counter4Utils.fromString(content);
    if (report != null) {
      List<YearMonth> yearMonthsFromReport = Counter4Utils.getYearMonthsFromReport(report);

      List<Report> reports = Collections.singletonList(report);
      if (yearMonthsFromReport.size() != 1) {
        reports = Counter4Utils.split(report);
      }

      List<CounterReport> counterReports =
          reports.stream()
              .map(
                  r -> {
                    List<YearMonth> months = Counter4Utils.getYearMonthsFromReport(r);
                    if (!months.isEmpty()) {
                      return new CounterReport()
                          .withRelease(report.getVersion())
                          .withReportName(Counter4Utils.getNameForReportTitle(report.getName()))
                          .withReport(
                              Json.decodeValue(
                                  Counter4Utils.toJSON(report),
                                  org.folio.rest.jaxrs.model.Report.class))
                          .withYearMonth(months.get(0).toString());
                    } else {
                      return null;
                    }
                  })
              .collect(Collectors.toList());

      if (counterReports.isEmpty()) {
        throw new FileUploadException("No months to process.");
      } else if (counterReports.contains(null)) {
        throw new FileUploadException("Error processing at least one month from supplied report.");
      } else {
        return counterReports;
      }
    }

    throw new FileUploadException(MSG_WRONG_FORMAT);
  }

  public static class FileUploadException extends Exception {

    private static final long serialVersionUID = -3795351043189447151L;

    public FileUploadException(String message) {
      super(message);
    }

    public FileUploadException(Exception e) {
      super(e);
    }
  }

  private UploadHelper() {}
}
