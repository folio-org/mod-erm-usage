package org.folio.rest.util;

import org.folio.rest.jaxrs.model.ReportUploadError;

public class ReportUploadErrorFactory {

  private ReportUploadErrorFactory() {}

  public static ReportUploadError create(ReportUploadErrorCode errorCode) {
    return create(errorCode, (String) null);
  }

  public static ReportUploadError create(ReportUploadErrorCode errorCode, String details) {
    return new ReportUploadError()
        .withCode(errorCode.name())
        .withMessage(errorCode.getMessage())
        .withDetails(details);
  }

  public static ReportUploadError create(ReportUploadErrorCode errorCode, Throwable cause) {
    String details = (cause != null) ? cause.toString() : null;
    return create(errorCode, details);
  }
}
