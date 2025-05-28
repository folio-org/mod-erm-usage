package org.folio.rest.util;

public enum ReportUploadErrorCode {
  INVALID_REPORT_CONTENT("The report content is invalid."),
  MAXIMUM_FILESIZE_EXCEEDED("The file size exceeds the maximum allowed size."),
  MULTIPLE_FILES_NOT_SUPPORTED("Upload of multiple files is not supported."),
  OTHER("The report could not be processed."),
  UNSUPPORTED_FILE_FORMAT("The file format is not supported."),
  UNSUPPORTED_REPORT_RELEASE("The COUNTER Release version is not supported."),
  UNSUPPORTED_REPORT_TYPE("The report type is not supported."),
  ;

  private final String message;

  ReportUploadErrorCode(String message) {
    this.message = message;
  }

  public String getMessage() {
    return message;
  }
}
