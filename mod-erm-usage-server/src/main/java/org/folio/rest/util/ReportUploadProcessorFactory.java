package org.folio.rest.util;

import org.apache.commons.csv.CSVFormat;

/**
 * Factory class for creating {@link ReportUploadProcessor} instances. This factory encapsulates the
 * logic for instantiating different types of report processors.
 */
public class ReportUploadProcessorFactory {

  private ReportUploadProcessorFactory() {}

  /**
   * Creates a processor instance for the given report file format.
   *
   * @param format the {@link ReportFileFormat} to create a processor for
   * @return a {@link ReportUploadProcessor} configured for the specified format
   * @throws IllegalArgumentException if the format is not supported
   */
  public static ReportUploadProcessor createProcessor(ReportFileFormat format) {
    return switch (format) {
      case CSV -> createCsvProcessor();
      case JSON -> createJsonProcessor();
      case TSV -> createTsvProcessor();
      case XML -> createXmlProcessor();
      case XLSX -> createXlsxProcessor();
    };
  }

  /**
   * Creates a CSV processor.
   *
   * @return a new instance of {@link ReportUploadCsvProcessor} configured for CSV
   */
  public static ReportUploadProcessor createCsvProcessor() {
    return new ReportUploadCsvProcessor(CSVFormat.RFC4180);
  }

  /**
   * Creates a JSON processor.
   *
   * @return a new instance of {@link ReportUploadJsonProcessor}
   */
  public static ReportUploadProcessor createJsonProcessor() {
    return new ReportUploadJsonProcessor();
  }

  /**
   * Creates an XML processor.
   *
   * @return a new instance of {@link ReportUploadXmlProcessor}
   */
  public static ReportUploadProcessor createXmlProcessor() {
    return new ReportUploadXmlProcessor();
  }

  /**
   * Creates a TSV processor.
   *
   * @return a new instance of {@link ReportUploadCsvProcessor} configured for TSV
   */
  public static ReportUploadProcessor createTsvProcessor() {
    return new ReportUploadCsvProcessor(CSVFormat.TDF);
  }

  /**
   * Creates an XLSX processor.
   *
   * @return a new instance of {@link ReportUploadCsvProcessor} configured for CSV
   */
  public static ReportUploadProcessor createXlsxProcessor() {
    return new ReportUploadCsvProcessor(CSVFormat.RFC4180);
  }
}
