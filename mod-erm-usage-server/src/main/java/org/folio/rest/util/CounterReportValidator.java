package org.folio.rest.util;

import java.util.List;
import org.folio.rest.jaxrs.model.ApiException;
import org.folio.rest.jaxrs.model.CounterReport;
import org.folio.rest.jaxrs.model.Errors;
import org.folio.rest.tools.utils.ValidationHelper;

public class CounterReportValidator {

  private static final String API_EXCEPTIONS = "apiExceptions";

  private CounterReportValidator() {}

  /**
   * Validates a CounterReport, including nested apiExceptions.
   *
   * <p>Checks that if apiExceptions is present, every ApiException in the list has a non-null code.
   *
   * @param report the CounterReport to validate
   * @return validation errors, empty if valid
   */
  public static Errors validate(final CounterReport report) {
    final var errors = new Errors();

    validateApiExceptions(report, errors);

    return errors;
  }

  private static void validateApiExceptions(final CounterReport report, final Errors errors) {
    final List<ApiException> apiExceptions = report.getApiExceptions();
    if (apiExceptions == null) {
      return;
    }

    for (int i = 0; i < apiExceptions.size(); i++) {
      final var exception = apiExceptions.get(i);
      if (exception.getCode() == null) {
        errors
            .getErrors()
            .add(
                ValidationHelper.createValidationErrorMessage(
                        API_EXCEPTIONS + "[" + i + "].code", null, "is required")
                    .getErrors()
                    .getFirst());
      }
    }
  }
}
