package org.folio.rest.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.folio.rest.jaxrs.model.ApiException;
import org.folio.rest.jaxrs.model.CounterReport;
import org.folio.rest.jaxrs.model.Error;
import org.folio.rest.jaxrs.model.Parameter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;

class CounterReportValidatorTest {

  private static List<String> validate(CounterReport report) {
    return CounterReportValidator.validate(report).getErrors().stream()
        .map(Error::getParameters)
        .flatMap(List::stream)
        .map(Parameter::getKey)
        .toList();
  }

  private static CounterReport createValidCounterReport() {
    return new CounterReport()
        .withDownloadTime(new java.util.Date())
        .withRelease("5")
        .withReportName("JR1")
        .withYearMonth("2024-01");
  }

  private static CounterReport createCounterReportWithValidApiException() {
    var report = createValidCounterReport();
    report.setApiExceptions(
        List.of(
            new ApiException().withCode(3000).withMessage("Connection refused"),
            new ApiException().withCode(3031).withMessage("Host not found")));
    return report;
  }

  @Test
  void validCounterReportIsAccepted() {
    assertThat(validate(createValidCounterReport())).isEmpty();
  }

  @Test
  void validCounterReportWithApiExceptionsIsAccepted() {
    assertThat(validate(createCounterReportWithValidApiException())).isEmpty();
  }

  @Test
  void nullApiExceptionsIsAccepted() {
    var report = createValidCounterReport();
    report.setApiExceptions(null);
    assertThat(validate(report)).isEmpty();
  }

  @Test
  void emptyApiExceptionsListIsAccepted() {
    var report = createValidCounterReport();
    report.setApiExceptions(List.of());
    assertThat(validate(report)).isEmpty();
  }

  @Test
  void apiExceptionWithNullCodeIsRejected() {
    var report = createValidCounterReport();
    report.setApiExceptions(List.of(new ApiException().withCode(null)));
    assertThat(validate(report)).containsExactly("apiExceptions[0].code");
  }

  @Test
  void apiExceptionWithNullCodeAmongValidOnesIsRejected() {
    var report = createValidCounterReport();
    report.setApiExceptions(
        List.of(
            new ApiException().withCode(3000),
            new ApiException().withCode(null),
            new ApiException().withCode(3031)));
    assertThat(validate(report)).containsExactly("apiExceptions[1].code");
  }

  @Test
  void allApiExceptionsWithNullCodeAreRejected() {
    var report = createValidCounterReport();
    report.setApiExceptions(
        List.of(new ApiException().withCode(null), new ApiException().withCode(null)));
    assertThat(validate(report)).containsExactly("apiExceptions[0].code", "apiExceptions[1].code");
  }

  @Test
  void apiExceptionWithValidCodeIsNotRejected() {
    var report = createValidCounterReport();
    report.setApiExceptions(List.of(new ApiException().withCode(3000)));
    assertThat(validate(report)).isEmpty();
  }

  @Test
  void apiExceptionWithZeroCodeIsNotRejected() {
    var report = createValidCounterReport();
    report.setApiExceptions(List.of(new ApiException().withCode(0)));
    assertThat(validate(report)).isEmpty();
  }

  @Test
  void apiExceptionWithLargeCodeIsNotRejected() {
    var report = createValidCounterReport();
    report.setApiExceptions(List.of(new ApiException().withCode(9999)));
    assertThat(validate(report)).isEmpty();
  }

  @ParameterizedTest
  @NullAndEmptySource
  void apiExceptionWithNullMessageIsAccepted(String message) {
    var report = createValidCounterReport();
    report.setApiExceptions(List.of(new ApiException().withCode(3000).withMessage(message)));
    assertThat(validate(report)).isEmpty();
  }

  @Test
  void mixedValidAndInvalidApiExceptionsReportsAllErrors() {
    var report = createValidCounterReport();
    report.setApiExceptions(
        List.of(
            new ApiException().withCode(null),
            new ApiException().withCode(3000),
            new ApiException().withCode(null)));
    assertThat(validate(report)).containsExactly("apiExceptions[0].code", "apiExceptions[2].code");
  }
}
