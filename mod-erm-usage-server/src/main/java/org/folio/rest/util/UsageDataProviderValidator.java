package org.folio.rest.util;

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

  private static final String MSG_INACTIVE_WITH_ACTIVE_HARVESTING =
      "must not be active if provider status is inactive";
  private static final String MSG_REQUIRED = "is required if harvesting status is active";
  private static final String MSG_INVALID_URL =
      "must start with 'http://' or 'https://' followed by a non-whitespace character";

  // same pattern as the ui-erm-usage form
  private static final Pattern SERVICE_URL_PATTERN = Pattern.compile("^https?://\\S");

  private UsageDataProviderValidator() {}

  /**
   * Validates a {@link UsageDataProvider}.
   *
   * @param udp the {@link UsageDataProvider} to validate
   * @return {@link Errors} naming the violating fields, empty if {@code udp} is valid
   */
  public static Errors validate(final UsageDataProvider udp) {
    final var errors = new Errors();
    final var harvestingConfig = Optional.ofNullable(udp.getHarvestingConfig());

    // No harvesting configuration or configured harvester not 'active'
    if (harvestingConfig
        .map(HarvestingConfig::getHarvestingStatus)
        .filter(HarvestingStatus.ACTIVE::equals)
        .isEmpty()) {
      return errors; // No errors
    }

    // Harvesting status 'active' but provider status 'inactive'
    if (udp.getStatus() == Status.INACTIVE) {
      addError(errors, HARVESTING_STATUS, "active", MSG_INACTIVE_WITH_ACTIVE_HARVESTING);
    }

    final var sushiConfig = harvestingConfig.map(HarvestingConfig::getSushiConfig);
    final var serviceUrl = sushiConfig.map(SushiConfig::getServiceUrl);

    // No harvesting start
    requirePresent(
        errors, HARVESTING_START, harvestingConfig.map(HarvestingConfig::getHarvestingStart));

    // No service type
    requirePresent(errors, SERVICE_TYPE, sushiConfig.map(SushiConfig::getServiceType));

    // No service URL
    requirePresent(errors, SERVICE_URL, serviceUrl);

    // No customer ID in SUSHI credentials
    final var customerId =
        Optional.ofNullable(udp.getSushiCredentials()).map(SushiCredentials::getCustomerId);
    requirePresent(errors, CUSTOMER_ID, customerId);

    // Service URL doesn't match pattern
    serviceUrl
        .filter(url -> !SERVICE_URL_PATTERN.matcher(url).find())
        .ifPresent(url -> addError(errors, SERVICE_URL, url, MSG_INVALID_URL));

    return errors;
  }

  private static void requirePresent(
      final Errors errors, final String field, final Optional<String> maybeValue) {
    maybeValue
        .filter(value -> !value.isEmpty())
        .ifPresentOrElse(value -> {}, () -> addError(errors, field, null, MSG_REQUIRED));
  }

  private static void addError(
      final Errors errors, final String field, final String value, final String message) {
    errors
        .getErrors()
        .addAll(ValidationHelper.createValidationErrorMessage(field, value, message).getErrors());
  }
}
