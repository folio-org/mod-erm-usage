package org.folio.rest.util;

import static org.folio.rest.util.ReportUploadErrorCode.INVALID_REPORT_CONTENT;
import static org.folio.rest.util.ReportUploadErrorCode.UNSUPPORTED_REPORT_RELEASE;
import static org.folio.rest.util.UploadHelper.checkThatReportIsSupported;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.folio.rest.jaxrs.model.CounterReport;
import org.niso.schemas.counter.Report;
import org.olf.erm.usage.counter41.Counter4Utils;
import org.olf.erm.usage.counter41.Counter4Utils.ReportSplitException;
import org.olf.erm.usage.counter41.csv.mapper.MapperException;
import org.olf.erm.usage.counter50.Counter5Utils;
import org.olf.erm.usage.counter50.Counter5Utils.Counter5UtilsException;
import org.olf.erm.usage.counter51.Counter51Utils;
import org.openapitools.client.model.SUSHIReportHeader;

public class ReportUploadCsvProcessor implements ReportUploadProcessor {

  private static final String RELEASE_KEY = "Release";
  public static final String UNABLE_TO_DETERMINE_RELEASE_VERSION =
      "Unable to determine the report release version.";

  private final CSVFormat csvFormat;

  public ReportUploadCsvProcessor(CSVFormat csvFormat) {
    this.csvFormat = csvFormat;
  }

  @Override
  public List<CounterReport> process(String reportData) throws ReportUploadException {
    try {
      ReportReleaseVersion reportReleaseVersion =
          getReportReleaseVersionFromCsv(reportData, csvFormat);
      return switch (reportReleaseVersion) {
        case R4 -> processR4CsvReport(reportData);
        case R5 -> processR5CsvReport(reportData);
        case R51 -> processR51CsvReport(reportData, csvFormat);
      };
    } catch (ReportUploadException e) {
      throw e;
    } catch (Exception e) {
      throw new ReportUploadException(INVALID_REPORT_CONTENT, e);
    }
  }

  public static ReportReleaseVersion getReportReleaseVersionFromCsv(
      String content, CSVFormat csvFormat) throws IOException, ReportUploadException {
    List<CSVRecord> firstRows;
    try (CSVParser csvParser = CSVParser.parse(content, csvFormat)) {
      firstRows = csvParser.stream().limit(3).toList();
    }

    if (firstRows.size() == 3) {
      CSVRecord firstRecord = firstRows.get(0);
      if (firstRecord.size() > 0
          && Counter4Utils.getNameForReportTitle(firstRecord.get(0)) != null) {
        return ReportReleaseVersion.R4;
      }

      CSVRecord thirdRecord = firstRows.get(2);
      if (thirdRecord.size() > 1 && thirdRecord.get(0).equals(RELEASE_KEY)) {
        try {
          return ReportReleaseVersion.fromVersion(thirdRecord.get(1));
        } catch (IllegalArgumentException e) {
          throw new ReportUploadException(UNSUPPORTED_REPORT_RELEASE, e);
        }
      }
    }
    throw new ReportUploadException(INVALID_REPORT_CONTENT, UNABLE_TO_DETERMINE_RELEASE_VERSION);
  }

  private static List<CounterReport> processR51CsvReport(String content, CSVFormat csvFormat)
      throws IOException, ReportSplitException, Counter5UtilsException, ReportUploadException {
    JsonNode report = Counter51Utils.createReportFromCsv(new StringReader(content), csvFormat);
    return UploadHelper.processR51JsonReport(report);
  }

  private static List<CounterReport> processR5CsvReport(String content)
      throws Counter5UtilsException,
          org.olf.erm.usage.counter50.csv.mapper.MapperException,
          ReportSplitException {
    Object report = Counter5Utils.fromCSV(content);
    return processR5Report(report);
  }

  private List<CounterReport> processR4CsvReport(String content)
      throws ReportSplitException, MapperException, IOException, Counter5UtilsException {
    Report report = Counter4Utils.fromCsvString(content);
    String reportName = Counter4Utils.getNameForReportTitle(report.getName());
    return UploadHelper.createCounterReports(report, reportName, ReportReleaseVersion.R4);
  }

  private static List<CounterReport> processR5Report(Object report)
      throws Counter5UtilsException, ReportSplitException {
    SUSHIReportHeader header = Counter5Utils.getSushiReportHeaderFromReportObject(report);
    checkThatReportIsSupported(header);
    String reportName = header.getReportID();
    return UploadHelper.createCounterReports(report, reportName, ReportReleaseVersion.R5);
  }
}
