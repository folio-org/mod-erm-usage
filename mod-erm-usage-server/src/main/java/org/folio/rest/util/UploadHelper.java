package org.folio.rest.util;

import static org.apache.commons.lang3.StringUtils.isEmpty;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serial;
import java.io.StringReader;
import java.time.YearMonth;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.folio.rest.jaxrs.model.CounterReport;
import org.niso.schemas.counter.Report;
import org.olf.erm.usage.counter.common.ExcelUtil;
import org.olf.erm.usage.counter41.Counter4Utils;
import org.olf.erm.usage.counter41.Counter4Utils.ReportSplitException;
import org.olf.erm.usage.counter41.csv.mapper.MapperException;
import org.olf.erm.usage.counter50.Counter5Utils;
import org.olf.erm.usage.counter50.Counter5Utils.Counter5UtilsException;
import org.olf.erm.usage.counter51.Counter51Utils;
import org.olf.erm.usage.counter51.ReportType;
import org.openapitools.client.model.SUSHIReportHeader;
import org.openapitools.client.model.SUSHIReportHeaderReportAttributes;

/**
 * Utility class for processing COUNTER reports. This class provides methods to parse and convert
 * various report formats (JSON, CSV, XML) into {@link CounterReport} objects.
 */
public class UploadHelper {

  private static final String MSG_WRONG_FORMAT = "Wrong format supplied";
  private static final String MSG_UNSUPPORTED_REPORT = "Unsupported report";
  private static final String MSG_UNSUPPORTED_REPORT_FORMAT = "Unsupported report format";
  private static final String ATTRIBUTES_TO_SHOW = "Attributes_To_Show";
  private static final String RELEASE_KEY = "Release";
  private static final String REPORT_HEADER_KEY = "Report_Header";
  private static final Map<String, List<SUSHIReportHeaderReportAttributes>> SUPPORTED_REPORTS =
      initSupportedReports();

  private UploadHelper() {}

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
   * Processes a {@link Buffer} that represents a COUNTER report and converts it into a list of
   * {@link CounterReport} objects. This method handles different report formats and COUNTER release
   * versions.
   *
   * @param format the {@link ReportFileFormat} of the input content
   * @param buffer a {@link Buffer} containing the content of the report
   * @return a list of {@link CounterReport} objects parsed from the input
   * @throws FileUploadException if there's an error in processing the file content or if the format
   *     is unsupported
   */
  public static List<CounterReport> getCounterReportsFromBuffer(
      ReportFileFormat format, Buffer buffer) throws FileUploadException {
    try {
      String content = bufferToString(buffer, format);
      return switch (format) {
        case JSON -> processJsonReport(content);
        case CSV, XSLX -> processCsvReport(content, ReportFileFormat.CSV);
        case TSV -> processCsvReport(content, ReportFileFormat.TSV);
        case XML -> processXmlReport(content);
      };
    } catch (Exception e) {
      throw new FileUploadException(MSG_WRONG_FORMAT + ": " + e.getMessage(), e);
    }
  }

  private static String bufferToString(Buffer buffer, ReportFileFormat reportFileFormat)
      throws IOException {
    if (ReportFileFormat.XSLX.equals(reportFileFormat)) {
      return ExcelUtil.toCSV(new ByteArrayInputStream(buffer.getBytes()));
    } else {
      return buffer.toString();
    }
  }

  private static List<CounterReport> processXmlReport(String content)
      throws ReportSplitException, Counter5UtilsException {
    Report report = Counter4Utils.fromString(content);
    if (report == null
        || ReportReleaseVersion.fromVersion(report.getVersion()) != ReportReleaseVersion.R4) {
      throw new FileUploadException(MSG_WRONG_FORMAT);
    }
    String reportName = Counter4Utils.getNameForReportTitle(report.getName());
    return createCounterReports(report, reportName, ReportReleaseVersion.R4);
  }

  private static List<CounterReport> processJsonReport(String content)
      throws Counter5UtilsException, IOException, ReportSplitException {
    ReportReleaseVersion version = getCounterReleaseVersion(ReportFileFormat.JSON, content);
    return switch (version) {
      case R4 -> throw new FileUploadException(MSG_UNSUPPORTED_REPORT_FORMAT);
      case R5 -> processR5JsonReport(content);
      case R51 -> processR51JsonReport(content);
    };
  }

  private static List<CounterReport> processCsvReport(
      String content, ReportFileFormat reportFileFormat)
      throws MapperException,
          Counter5UtilsException,
          ReportSplitException,
          IOException,
          org.olf.erm.usage.counter50.csv.mapper.MapperException {
    ReportReleaseVersion version = getCounterReleaseVersion(reportFileFormat, content);
    return switch (version) {
      case R4 -> processR4CsvReport(content);
      case R5 -> processR5CsvReport(content);
      case R51 -> processR51CsvReport(content, reportFileFormat);
    };
  }

  private static List<CounterReport> processR5JsonReport(String content)
      throws Counter5UtilsException, ReportSplitException {
    Object report = Counter5Utils.fromJSON(content);
    return processR5Report(report);
  }

  private static List<CounterReport> processR51JsonReport(String content)
      throws JsonProcessingException, ReportSplitException, Counter5UtilsException {
    JsonNode jsonNode = Counter51Utils.getDefaultObjectMapper().readTree(content);
    return processR51JsonReport(jsonNode);
  }

  private static List<CounterReport> processR51JsonReport(JsonNode jsonNode)
      throws ReportSplitException, Counter5UtilsException {

    ReportType reportType = Counter51Utils.getReportType(jsonNode);

    if (reportType.isMasterReport()) {
      Counter51Utils.validate(jsonNode, reportType);
      return createCounterReports(jsonNode, reportType.name(), ReportReleaseVersion.R51);
    } else {
      throw new FileUploadException(MSG_UNSUPPORTED_REPORT);
    }
  }

  private static List<CounterReport> processR5CsvReport(String content)
      throws Counter5UtilsException,
          org.olf.erm.usage.counter50.csv.mapper.MapperException,
          ReportSplitException {
    Object report = Counter5Utils.fromCSV(content);
    return processR5Report(report);
  }

  private static List<CounterReport> processR51CsvReport(
      String content, ReportFileFormat reportFileFormat)
      throws IOException, ReportSplitException, Counter5UtilsException {
    CSVFormat csvFormat = CSVFormat.RFC4180;
    if (ReportFileFormat.TSV.equals(reportFileFormat)) {
      csvFormat = CSVFormat.TDF;
    }
    JsonNode report = Counter51Utils.createReportFromCsv(new StringReader(content), csvFormat);
    return processR51JsonReport(report);
  }

  private static List<CounterReport> processR5Report(Object report)
      throws Counter5UtilsException, ReportSplitException {
    SUSHIReportHeader header = Counter5Utils.getSushiReportHeaderFromReportObject(report);
    checkThatReportIsSupported(header);
    String reportName = header.getReportID();
    return createCounterReports(report, reportName, ReportReleaseVersion.R5);
  }

  private static List<CounterReport> processR4CsvReport(String content)
      throws ReportSplitException, MapperException, IOException, Counter5UtilsException {
    Report report = Counter4Utils.fromCsvString(content);
    String reportName = Counter4Utils.getNameForReportTitle(report.getName());
    return createCounterReports(report, reportName, ReportReleaseVersion.R4);
  }

  private static void checkThatReportIsSupported(SUSHIReportHeader header)
      throws FileUploadException {
    String reportName = header.getReportName();
    if (!SUPPORTED_REPORTS.containsKey(reportName)) {
      throw new FileUploadException(MSG_UNSUPPORTED_REPORT);
    }

    List<SUSHIReportHeaderReportAttributes> expectedAttributes = SUPPORTED_REPORTS.get(reportName);
    List<SUSHIReportHeaderReportAttributes> actualAttributes = header.getReportAttributes();
    if (!(actualAttributes != null
        && actualAttributes.size() == expectedAttributes.size()
        && new HashSet<>(actualAttributes).containsAll(expectedAttributes))) {
      throw new FileUploadException(MSG_UNSUPPORTED_REPORT);
    }
  }

  private static List<CounterReport> createCounterReports(
      Object report, String reportName, ReportReleaseVersion version)
      throws ReportSplitException, Counter5UtilsException {
    if (isEmpty(reportName)) {
      throw new FileUploadException(MSG_UNSUPPORTED_REPORT);
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

  private static ReportReleaseVersion getCounterReleaseVersion(
      ReportFileFormat format, String content) throws IOException {
    return switch (format) {
      case CSV, XSLX -> getReportReleaseVersionFromCsv(content, CSVFormat.RFC4180);
      case JSON -> getReportReleaseVersionFromJson(content);
      case TSV -> getReportReleaseVersionFromCsv(content, CSVFormat.TDF);
      case XML -> ReportReleaseVersion.R4;
    };
  }

  private static ReportReleaseVersion getReportReleaseVersionFromJson(String content) {
    return ReportReleaseVersion.fromVersion(
        new JsonObject(content).getJsonObject(REPORT_HEADER_KEY).getString(RELEASE_KEY));
  }

  private static ReportReleaseVersion getReportReleaseVersionFromCsv(
      String content, CSVFormat csvFormat) throws IOException {
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
        return ReportReleaseVersion.fromVersion(thirdRecord.get(1));
      }
    }
    throw new FileUploadException(MSG_WRONG_FORMAT);
  }

  public static class FileUploadException extends RuntimeException {
    @Serial private static final long serialVersionUID = -3795351043189447151L;

    public FileUploadException(String message) {
      super(message);
    }

    public FileUploadException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
