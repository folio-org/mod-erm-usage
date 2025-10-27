package org.folio.rest.util;

import static org.folio.rest.util.ReportUploadErrorCode.INVALID_REPORT_CONTENT;
import static org.folio.rest.util.ReportUploadErrorCode.UNSUPPORTED_REPORT_RELEASE;
import static org.folio.rest.util.ReportUploadErrorCode.UNSUPPORTED_REPORT_TYPE;

import java.util.List;
import org.folio.rest.jaxrs.model.CounterReport;
import org.niso.schemas.counter.Report;
import org.olf.erm.usage.counter41.Counter4Utils;
import org.olf.erm.usage.counter41.Counter4Utils.ReportSplitException;
import org.olf.erm.usage.counter50.Counter5Utils.Counter5UtilsException;

public class ReportUploadXmlProcessor implements ReportUploadProcessor {

  public static final String UNABLE_TO_PARSE_REPORT = "Unable to parse the provided report.";
  public static final String UNSUPPORTED_REPORT_RELEASE_DETAILS =
      "Only COUNTER Release 4 is supported for XML files.";

  @Override
  public List<CounterReport> process(String reportData) throws ReportUploadException {
    try {
      return processXmlReport(reportData);
    } catch (ReportUploadException e) {
      throw e;
    } catch (Exception e) {
      throw new ReportUploadException(INVALID_REPORT_CONTENT, e);
    }
  }

  private static List<CounterReport> processXmlReport(String content)
      throws ReportSplitException, Counter5UtilsException, ReportUploadException {
    Report report = Counter4Utils.fromString(content);
    if (report == null) {
      throw new ReportUploadException(INVALID_REPORT_CONTENT, UNABLE_TO_PARSE_REPORT);
    }
    if (ReportReleaseVersion.fromVersion(report.getVersion()) != ReportReleaseVersion.R4) {
      throw new ReportUploadException(
          UNSUPPORTED_REPORT_RELEASE, UNSUPPORTED_REPORT_RELEASE_DETAILS);
    }
    String reportName = Counter4Utils.getNameForReportTitle(report.getName());
    if (reportName == null) {
      throw new ReportUploadException(UNSUPPORTED_REPORT_TYPE);
    }
    return ProcessorHelper.createCounterReports(report, reportName, ReportReleaseVersion.R4);
  }
}
