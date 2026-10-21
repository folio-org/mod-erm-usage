package org.folio.rest.util;

import static java.util.function.Predicate.not;

import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import org.folio.rest.jaxrs.model.Error;
import org.folio.rest.jaxrs.model.Errors;
import org.folio.rest.jaxrs.model.HarvestingConfig;
import org.folio.rest.jaxrs.model.HarvestingConfig.HarvestingStatus;
import org.folio.rest.jaxrs.model.Parameter;
import org.folio.rest.jaxrs.model.SushiConfig;
import org.folio.rest.jaxrs.model.SushiCredentials;
import org.folio.rest.jaxrs.model.UsageDataProvider;
import org.folio.rest.jaxrs.model.UsageDataProvider.Status;
import org.folio.rest.resource.DomainModelConsts;

/**
 * Validates a {@link UsageDataProvider} on write, following the rules of the ui-erm-usage form.
 * Existing records are not checked.
 */
@SuppressWarnings({"OptionalUsedAsFieldOrParameterType", "java:S3553"})
public class UsageDataProviderValidator {

  private static final String HARVESTING_STATUS = "harvestingConfig.harvestingStatus";
  private static final String HARVESTING_START = "harvestingConfig.harvestingStart";
  private static final String SERVICE_TYPE = "harvestingConfig.sushiConfig.serviceType";
  private static final String SERVICE_URL = "harvestingConfig.sushiConfig.serviceUrl";
  private static final String CUSTOMER_ID = "sushiCredentials.customerId";

  private static final Pattern SERVICE_URL_PATTERN =
      Pattern.compile("^https?://\\S", Pattern.CASE_INSENSITIVE);
  private static final Pattern HARVESTING_START_PATTERN = Pattern.compile("\\d{4}-(0[1-9]|1[0-2])");

  private UsageDataProviderValidator() {}

  /**
   * @return The violations of {@code udp}. If harvesting is active, the harvesting config needs a
   *     {@code harvestingStart} in the format {@code yyyy-MM}, a non-empty {@code
   *     sushiConfig.serviceType} and a {@code sushiConfig.serviceUrl} starting with {@code
   *     http(s)://}, the provider must not be inactive, and {@code sushiCredentials.customerId}
   *     must be non-empty.
   */
  public static Errors validate(final UsageDataProvider udp) {
    final var errors = new Errors();
    final var harvestingConfig = Optional.ofNullable(udp.getHarvestingConfig());

    if (harvestingConfig
        .map(HarvestingConfig::getHarvestingStatus)
        .filter(HarvestingStatus.ACTIVE::equals)
        .isEmpty()) {
      return errors; // Inactive harvesting status won't throw any errors.
    }

    if (udp.getStatus() == Status.INACTIVE) {
      addError(
          errors, HARVESTING_STATUS, "active", "must not be active if provider status is inactive");
    }

    final var sushiConfig = harvestingConfig.map(HarvestingConfig::getSushiConfig);
    final var harvestingStart = harvestingConfig.map(HarvestingConfig::getHarvestingStart);
    requirePresent(errors, HARVESTING_START, harvestingStart);
    final var serviceType = sushiConfig.map(SushiConfig::getServiceType);
    requirePresent(errors, SERVICE_TYPE, serviceType);
    final var serviceUrl = sushiConfig.map(SushiConfig::getServiceUrl);
    requirePresent(errors, SERVICE_URL, serviceUrl);
    final var customerId =
        Optional.ofNullable(udp.getSushiCredentials()).map(SushiCredentials::getCustomerId);
    requirePresent(errors, CUSTOMER_ID, customerId);

    requireValidServiceUrl(errors, serviceUrl);
    requireValidHarvestingStart(errors, harvestingStart);

    return errors;
  }

  private static void requirePresent(
      final Errors errors, final String field, final Optional<String> maybeValue) {
    if (maybeValue.filter(not(String::isBlank)).isEmpty()) {
      final var value = maybeValue.orElse(null);
      addError(errors, field, value, "is required if harvesting status is active");
    }
  }

  private static void requireValidServiceUrl(
      final Errors errors, final Optional<String> maybeServiceUrl) {
    maybeServiceUrl
        .filter(not(String::isBlank))
        .filter(serviceUrl -> !SERVICE_URL_PATTERN.matcher(serviceUrl).find())
        .ifPresent(
            serviceUrl ->
                addError(
                    errors,
                    SERVICE_URL,
                    serviceUrl,
                    "must start with 'http://' or 'https://' followed by a non-whitespace"
                        + " character"));
  }

  private static void requireValidHarvestingStart(
      final Errors errors, final Optional<String> maybeHarvestingStart) {
    maybeHarvestingStart
        .filter(not(String::isBlank))
        .filter(harvestingStart -> !HARVESTING_START_PATTERN.matcher(harvestingStart).matches())
        .ifPresent(
            harvestingStart ->
                addError(
                    errors,
                    HARVESTING_START,
                    harvestingStart,
                    "must be a month in the format 'yyyy-MM', e.g. '2024-01'"));
  }

  private static void addError(
      final Errors errors, final String field, final String value, final String message) {
    errors
        .getErrors()
        .add(
            new Error()
                .withMessage(message)
                .withCode("-1")
                .withType(DomainModelConsts.VALIDATION_FIELD_ERROR)
                .withParameters(List.of(new Parameter().withKey(field).withValue(value))));
  }
}
