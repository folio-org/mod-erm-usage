package org.folio.rest.util;

import java.util.List;
import org.apache.commons.csv.CSVFormat;
import org.folio.rest.jaxrs.model.CounterReport;

public interface ReportUploadProcessor {

  List<CounterReport> process(String reportData) throws Exception;

  static ReportUploadProcessor of(ReportFileFormat reportFileFormat) {
    return switch (reportFileFormat) {
      case JSON -> new ReportUploadJsonProcessor();
      case CSV, XLSX -> new ReportUploadCsvProcessor(CSVFormat.RFC4180);
      case TSV -> new ReportUploadCsvProcessor(CSVFormat.TDF);
      case XML -> new ReportUploadXmlProcessor();
      default ->
          throw new IllegalArgumentException("Unsupported report file format: " + reportFileFormat);
    };
  }
}
