package org.folio.rest.util;

import static java.util.function.Predicate.not;

import java.util.Optional;
import java.util.regex.Pattern;
import org.folio.rest.jaxrs.model.Errors;
import org.folio.rest.jaxrs.model.HarvestingConfig;
import org.folio.rest.jaxrs.model.HarvestingConfig.HarvestingStatus;
import org.folio.rest.jaxrs.model.SushiConfig;
import org.folio.rest.jaxrs.model.SushiCredentials;
import org.folio.rest.jaxrs.model.UsageDataProvider;
import org.folio.rest.jaxrs.model.UsageDataProvider.Status;
import org.folio.rest.tools.utils.ValidationHelper;

/**
 * Validates a {@link UsageDataProvider} on write, following the rules of the ui-erm-usage form.
 * Existing records are not checked.
 */
public class UsageDataProviderValidator {

  private static final String HARVESTING_STATUS = "harvestingConfig.harvestingStatus";
  private static final String HARVESTING_START = "harvestingConfig.harvestingStart";
  private static final String SERVICE_TYPE = "harvestingConfig.sushiConfig.serviceType";
  private static final String SERVICE_URL = "harvestingConfig.sushiConfig.serviceUrl";
  private static final String CUSTOMER_ID = "sushiCredentials.customerId";

  // same pattern as the ui-erm-usage form
  private static final Pattern SERVICE_URL_PATTERN = Pattern.compile("^https?://\\S");

  private UsageDataProviderValidator() {}

  /**
   * Validates {@code udp} and returns the keys of the rejected fields. If harvesting is active, the
   * tests assert that the harvesting config has a non-empty {@code harvestingStart} and {@code
   * sushiConfig.serviceType}, a {@code sushiConfig.serviceUrl} starting with {@code http(s)://},
   * and no active {@code harvestingStatus} on an inactive UDP. {@code sushiCredentials.customerId}
   * must also be non-empty.
   */
  public static Errors validate(final UsageDataProvider udp) {
    final var errors = new Errors();
    final var harvestingConfig = Optional.ofNullable(udp.getHarvestingConfig());

    if (harvestingConfig
        .map(HarvestingConfig::getHarvestingStatus)
        .filter(HarvestingStatus.ACTIVE::equals)
        .isEmpty()) {
      return errors; // A configuration with inactive harvesting status won't throw errors.
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

    serviceUrl
        .filter(not(String::isEmpty))
        .filter(url -> !SERVICE_URL_PATTERN.matcher(url).find())
        .ifPresent(
            url ->
                addError(
                    errors,
                    SERVICE_URL,
                    url,
                    "must start with 'http://' or 'https://' followed by a non-whitespace"
                        + " character"));

    return errors;
  }

  @SuppressWarnings({"OptionalUsedAsFieldOrParameterType", "java:S3553"})
  private static void requirePresent(
      final Errors errors, final String field, final Optional<String> maybeValue) {
    if (maybeValue.filter(not(String::isEmpty)).isEmpty()) {
      addError(errors, field, "<missing or empty>", "is required if harvesting status is active");
    }
  }

  private static void addError(
      final Errors errors, final String field, final String value, final String message) {
    errors
        .getErrors()
        .addAll(ValidationHelper.createValidationErrorMessage(field, value, message).getErrors());
  }
}
