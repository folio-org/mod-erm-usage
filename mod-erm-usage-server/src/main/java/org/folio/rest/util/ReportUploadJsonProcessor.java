package org.folio.rest.util;

import static org.folio.rest.util.ReportUploadErrorCode.INVALID_REPORT_CONTENT;
import static org.folio.rest.util.ReportUploadErrorCode.UNSUPPORTED_REPORT_RELEASE;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import io.vertx.core.json.JsonObject;
import java.util.List;
import org.folio.rest.jaxrs.model.CounterReport;
import org.olf.erm.usage.counter41.Counter4Utils.ReportSplitException;
import org.olf.erm.usage.counter50.Counter5Utils;
import org.olf.erm.usage.counter50.Counter5Utils.Counter5UtilsException;
import org.olf.erm.usage.counter51.Counter51Utils;
import org.openapitools.client.model.SUSHIReportHeader;

public class ReportUploadJsonProcessor implements ReportUploadProcessor {

  private static final String RELEASE_KEY = "Release";
  private static final String REPORT_HEADER_KEY = "Report_Header";
  public static final String UNSUPPORTED_REPORT_RELEASE_DETAILS =
      "COUNTER Release 4 is not supported for JSON file format.";

  @Override
  public List<CounterReport> process(String reportData) throws ReportUploadException {
    try {
      ReportReleaseVersion reportReleaseVersion = getReportReleaseVersionFromJson(reportData);
      return switch (reportReleaseVersion) {
        case R4 ->
            throw new ReportUploadException(
                UNSUPPORTED_REPORT_RELEASE, UNSUPPORTED_REPORT_RELEASE_DETAILS);
        case R5 -> processR5JsonReport(reportData);
        case R51 -> processR51JsonReport(reportData);
      };
    } catch (ReportUploadException e) {
      throw e;
    } catch (Exception e) {
      throw new ReportUploadException(INVALID_REPORT_CONTENT, e);
    }
  }

  private static ReportReleaseVersion getReportReleaseVersionFromJson(String content) {
    String releaseVersion =
        new JsonObject(content).getJsonObject(REPORT_HEADER_KEY).getString(RELEASE_KEY);
    try {
      return ReportReleaseVersion.fromVersion(releaseVersion);
    } catch (IllegalArgumentException e) {
      throw new ReportUploadException(UNSUPPORTED_REPORT_RELEASE, e);
    }
  }

  private List<CounterReport> processR5JsonReport(String content)
      throws Counter5UtilsException, ReportSplitException {
    Object report = Counter5Utils.fromJSON(content);
    return processR5Report(report);
  }

  private List<CounterReport> processR51JsonReport(String content)
      throws JsonProcessingException,
          ReportSplitException,
          Counter5UtilsException,
          ReportUploadException {
    JsonNode jsonNode = Counter51Utils.getDefaultObjectMapper().readTree(content);
    return ProcessorHelper.processR51JsonReport(jsonNode);
  }

  private List<CounterReport> processR5Report(Object report)
      throws Counter5UtilsException, ReportSplitException {
    SUSHIReportHeader header = Counter5Utils.getSushiReportHeaderFromReportObject(report);
    ProcessorHelper.checkThatReportIsSupported(header);
    String reportName = header.getReportID();
    return ProcessorHelper.createCounterReports(report, reportName, ReportReleaseVersion.R5);
  }
}
