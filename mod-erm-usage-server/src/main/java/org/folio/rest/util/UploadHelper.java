package org.folio.rest.util;

import static org.folio.rest.util.ReportUploadErrorCode.INVALID_REPORT_CONTENT;

import io.vertx.core.buffer.Buffer;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import org.folio.rest.jaxrs.model.CounterReport;
import org.olf.erm.usage.counter.common.ExcelUtil;

public class UploadHelper {

  private UploadHelper() {}

  /**
   * Processes a {@link Buffer} that represents a COUNTER report and converts it into a list of
   * {@link CounterReport} objects. This method handles different report formats and COUNTER release
   * versions.
   *
   * @param format the {@link ReportFileFormat} of the input content
   * @param buffer a {@link Buffer} containing the content of the report
   * @return a list of {@link CounterReport} objects parsed from the input
   * @throws ReportUploadException if there's an error in processing the file content or if the
   *     format is unsupported
   */
  public static List<CounterReport> getCounterReportsFromBuffer(
      ReportFileFormat format, Buffer buffer) throws ReportUploadException {
    try {
      String content = bufferToString(buffer, format);
      return ReportUploadProcessorFactory.createProcessor(format).process(content);
    } catch (ReportUploadException e) {
      throw e;
    } catch (Exception e) {
      throw new ReportUploadException(INVALID_REPORT_CONTENT, e);
    }
  }

  private static String bufferToString(Buffer buffer, ReportFileFormat reportFileFormat)
      throws IOException {
    if (ReportFileFormat.XLSX.equals(reportFileFormat)) {
      return ExcelUtil.toCSV(new ByteArrayInputStream(buffer.getBytes()));
    } else {
      return buffer.toString();
    }
  }
}
