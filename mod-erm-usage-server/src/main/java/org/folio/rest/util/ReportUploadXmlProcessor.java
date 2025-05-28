package org.folio.rest.util;

import static org.folio.rest.util.UploadHelper.MSG_WRONG_FORMAT;

import java.util.List;
import org.folio.rest.jaxrs.model.CounterReport;
import org.niso.schemas.counter.Report;
import org.olf.erm.usage.counter41.Counter4Utils;
import org.olf.erm.usage.counter41.Counter4Utils.ReportSplitException;
import org.olf.erm.usage.counter50.Counter5Utils.Counter5UtilsException;

public class ReportUploadXmlProcessor implements ReportUploadProcessor {

  @Override
  public List<CounterReport> process(String reportData) throws Exception {
    try {
      return processXmlReport(reportData);
    } catch (Exception e) {
      throw e;
    }
  }

  private static List<CounterReport> processXmlReport(String content)
      throws ReportSplitException, Counter5UtilsException {
    Report report = Counter4Utils.fromString(content);
    if (report == null
        || ReportReleaseVersion.fromVersion(report.getVersion()) != ReportReleaseVersion.R4) {
      throw new ReportUploadException(MSG_WRONG_FORMAT);
    }
    String reportName = Counter4Utils.getNameForReportTitle(report.getName());
    return UploadHelper.createCounterReports(report, reportName, ReportReleaseVersion.R4);
  }
}
