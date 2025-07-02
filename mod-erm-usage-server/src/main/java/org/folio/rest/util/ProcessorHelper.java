
package org.folio.rest.util;

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.folio.rest.util.ReportUploadErrorCode.INVALID_REPORT_CONTENT;
import static org.folio.rest.util.ReportUploadErrorCode.UNSUPPORTED_REPORT_TYPE;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.vertx.core.json.Json;
import java.time.YearMonth;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.folio.rest.jaxrs.model.CounterReport;
import org.niso.schemas.counter.Report;
import org.olf.erm.usage.counter41.Counter4Utils;
import org.olf.erm.usage.counter41.Counter4Utils.ReportSplitException;
import org.olf.erm.usage.counter50.Counter5Utils;
import org.olf.erm.usage.counter50.Counter5Utils.Counter5UtilsException;
import org.olf.erm.usage.counter51.Counter51Utils;
import org.olf.erm.usage.counter51.ReportType;
import org.openapitools.client.model.SUSHIReportHeader;
import org.openapitools.client.model.SUSHIReportHeaderReportAttributes;

/**
 * Utility class for processing COUNTER reports. This class provides methods to validate,
 * process, and convert various report objects into {@link CounterReport} objects.
 */
public class ProcessorHelper {

  private static final String ATTRIBUTES_TO_SHOW = "Attributes_To_Show";
  private static final Map<String, List<SUSHIReportHeaderReportAttributes>> SUPPORTED_REPORTS =
      initSupportedReports();

  private ProcessorHelper() {}

  private static Map<String, List<SUSHIReportHeaderReportAttributes>> initSupportedReports() {
    return Map.of(
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
  }

  /**
   * Processes a COUNTER 5.1 JSON report and converts it into a list of {@link CounterReport} objects.
   *
   * @param jsonNode the JSON representation of the report
   * @return a list of {@link CounterReport} objects
   * @throws ReportSplitException if there's an error splitting the report
   * @throws Counter5UtilsException if there's an error processing the COUNTER 5 report
   * @throws ReportUploadException if the report type is unsupported
   */
  public static List<CounterReport> processR51JsonReport(JsonNode jsonNode)
      throws ReportSplitException, Counter5UtilsException, ReportUploadException {

    ReportType reportType = Counter51Utils.getReportType(jsonNode);

    if (reportType.isMasterReport()) {
      Counter51Utils.validate(jsonNode, reportType);
      return createCounterReports(jsonNode, reportType.name(), ReportReleaseVersion.R51);
    } else {
      throw new ReportUploadException(
          UNSUPPORTED_REPORT_TYPE,
          "Supported report types for COUNTER Release 5 and 5.1 are: "
              + ReportType.getMasterReports());
    }
  }

  /**
   * Validates that a report is supported based on its header information.
   *
   * @param header the SUSHI report header to validate
   * @throws ReportUploadException if the report is not supported
   */
  public static void checkThatReportIsSupported(SUSHIReportHeader header)
      throws ReportUploadException {
    String reportName = header.getReportName();
    if (!SUPPORTED_REPORTS.containsKey(reportName)) {
      throw new ReportUploadException(UNSUPPORTED_REPORT_TYPE);
    }

    List<SUSHIReportHeaderReportAttributes> expectedAttributes = SUPPORTED_REPORTS.get(reportName);
    List<SUSHIReportHeaderReportAttributes> actualAttributes = header.getReportAttributes();
    if (!(actualAttributes != null
        && actualAttributes.size() == expectedAttributes.size()
        && new HashSet<>(actualAttributes).containsAll(expectedAttributes))) {
      throw new ReportUploadException(INVALID_REPORT_CONTENT, "Unsupported report attributes.");
    }
  }

  /**
   * Creates a list of {@link CounterReport} objects from a report object.
   *
   * @param report the report object to process
   * @param reportName the name of the report
   * @param version the COUNTER release version
   * @return a list of {@link CounterReport} objects
   * @throws ReportSplitException if there's an error splitting the report
   * @throws Counter5UtilsException if there's an error processing the COUNTER 5 report
   */
  public static List<CounterReport> createCounterReports(
      Object report, String reportName, ReportReleaseVersion version)
      throws ReportSplitException, Counter5UtilsException {
    if (isEmpty(reportName)) {
      throw new IllegalArgumentException("reportName must not be empty or null.");
    }

    List<?> splitReports = splitReport(report, version);
    return splitReports.stream()
        .map(r -> createCounterReport(r, reportName, version))
        .flatMap(Optional::stream)
        .toList();
  }

  private static List<?> splitReport(Object report, ReportReleaseVersion version)
      throws Counter5UtilsException, ReportSplitException {
    return switch (version) {
      case R4 -> Counter4Utils.split((Report) report);
      case R5 -> Counter5Utils.split(report);
      case R51 -> Counter51Utils.splitReport((ObjectNode) report);
    };
  }

  private static Optional<CounterReport> createCounterReport(
      Object report, String reportName, ReportReleaseVersion version) {
    List<YearMonth> yearMonths = getYearMonthsFromReport(report, version);

    if (yearMonths.isEmpty()) {
      return Optional.empty();
    }

    CounterReport counterReport =
        new CounterReport()
            .withRelease(version.getVersion())
            .withReportName(reportName)
            .withReport(decodeReport(report, version))
            .withYearMonth(yearMonths.get(0).toString());

    return Optional.of(counterReport);
  }

  private static List<YearMonth> getYearMonthsFromReport(
      Object report, ReportReleaseVersion version) {
    return switch (version) {
      case R4 -> Counter4Utils.getYearMonthsFromReport((Report) report);
      case R5 -> Counter5Utils.getYearMonthFromReport(report);
      case R51 -> Counter51Utils.getYearMonths((ObjectNode) report);
    };
  }

  private static org.folio.rest.jaxrs.model.Report decodeReport(
      Object report, ReportReleaseVersion version) {
    return switch (version) {
      case R4 ->
          Json.decodeValue(
              Counter4Utils.toJSON((Report) report), org.folio.rest.jaxrs.model.Report.class);
      case R5 -> Json.decodeValue(Json.encode(report), org.folio.rest.jaxrs.model.Report.class);
      case R51 ->
          Counter51Utils.getDefaultObjectMapper()
              .convertValue(report, org.folio.rest.jaxrs.model.Report.class);
    };
  }
}
