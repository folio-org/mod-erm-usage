package org.folio.rest.util;

import org.folio.rest.jaxrs.model.ReportUploadError;

public class ReportUploadException extends RuntimeException {

  private final transient ReportUploadError reportUploadError;

  public ReportUploadException(ReportUploadErrorCode errorCode) {
    this(ReportUploadErrorFactory.create(errorCode));
  }

  public ReportUploadException(ReportUploadErrorCode errorCode, Throwable cause) {
    this(ReportUploadErrorFactory.create(errorCode, cause), cause);
  }

  public ReportUploadException(ReportUploadErrorCode errorCode, String details) {
    this(ReportUploadErrorFactory.create(errorCode, details));
  }

  private ReportUploadException(ReportUploadError reportUploadError) {
    super(reportUploadError.getMessage());
    this.reportUploadError = reportUploadError;
  }

  private ReportUploadException(ReportUploadError reportUploadError, Throwable cause) {
    super(reportUploadError.getMessage(), cause);
    this.reportUploadError = reportUploadError;
  }

  public ReportUploadError getReportUploadError() {
    return reportUploadError;
  }
}
