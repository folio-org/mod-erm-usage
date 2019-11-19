package org.folio.rest.util;

import io.vertx.core.json.Json;
import java.io.IOException;
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
import org.olf.erm.usage.counter41.csv.mapper.MapperException;
import org.olf.erm.usage.counter50.Counter5Utils;
import org.olf.erm.usage.counter50.Counter5Utils.Counter5UtilsException;
import org.openapitools.client.model.SUSHIReportHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UploadHelper {

  private static final Logger log = LoggerFactory.getLogger(UploadHelper.class);
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
    try {
      SUSHIReportHeader header = Counter5Utils.getSushiReportHeader(content);
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
    } catch (Counter5UtilsException e) {
      log.info("Report does not seem to be a R5 Report: {}", e.getMessage());
    }

    // Counter 4
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
    List<CounterReport> counterReports =
        reports.stream()
            .map(
                r -> {
                  List<YearMonth> months = Counter4Utils.getYearMonthsFromReport(r);
                  if (!months.isEmpty()) {
                    return new CounterReport()
                        .withRelease(finalReport.getVersion())
                        .withReportName(Counter4Utils.getNameForReportTitle(finalReport.getName()))
                        .withReport(
                            Json.decodeValue(
                                Counter4Utils.toJSON(r), org.folio.rest.jaxrs.model.Report.class))
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
