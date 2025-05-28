package org.folio.rest.util;

import java.io.Serial;

public class ReportUploadException extends RuntimeException {
  @Serial private static final long serialVersionUID = -3795351043189447151L;

  public ReportUploadException(String message) {
    super(message);
  }

  public ReportUploadException(String message, Throwable cause) {
    super(message, cause);
  }
}
