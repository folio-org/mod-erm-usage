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
      "must not be active if status is inactive";
  private static final String MSG_REQUIRED = "is required if harvesting status is active";
  private static final String MSG_INVALID_URL =
      "must start with http:// or https:// followed by a non-whitespace character";

  // same pattern as the ui-erm-usage form
  private static final Pattern SERVICE_URL_PATTERN = Pattern.compile("^https?://\\S");

  private UsageDataProviderValidator() {}

  /**
   * Validates a {@link UsageDataProvider}.
   *
   * @param udp the {@link UsageDataProvider} to validate
   * @return {@link Errors} naming the violating fields, empty if {@code udp} is valid
   */
  public static Errors validate(UsageDataProvider udp) {
    Errors errors = new Errors();
    HarvestingConfig harvestingConfig = udp.getHarvestingConfig();
    if (harvestingConfig == null
        || harvestingConfig.getHarvestingStatus() != HarvestingStatus.ACTIVE) {
      return errors;
    }

    if (udp.getStatus() == Status.INACTIVE) {
      addError(errors, HARVESTING_STATUS, "active", MSG_INACTIVE_WITH_ACTIVE_HARVESTING);
    }

    Optional<SushiConfig> sushiConfig = Optional.ofNullable(harvestingConfig.getSushiConfig());
    String serviceUrl = sushiConfig.map(SushiConfig::getServiceUrl).orElse(null);
    requirePresent(errors, HARVESTING_START, harvestingConfig.getHarvestingStart());
    requirePresent(errors, SERVICE_TYPE, sushiConfig.map(SushiConfig::getServiceType).orElse(null));
    requirePresent(errors, SERVICE_URL, serviceUrl);
    requirePresent(
        errors,
        CUSTOMER_ID,
        Optional.ofNullable(udp.getSushiCredentials())
            .map(SushiCredentials::getCustomerId)
            .orElse(null));

    if (isPresent(serviceUrl) && !SERVICE_URL_PATTERN.matcher(serviceUrl).find()) {
      addError(errors, SERVICE_URL, serviceUrl, MSG_INVALID_URL);
    }

    return errors;
  }

  // like the UI's required check, only null and empty values count as missing
  private static boolean isPresent(String value) {
    return value != null && !value.isEmpty();
  }

  private static void requirePresent(Errors errors, String field, String value) {
    if (!isPresent(value)) {
      addError(errors, field, value, MSG_REQUIRED);
    }
  }

  private static void addError(Errors errors, String field, String value, String message) {
    errors
        .getErrors()
        .addAll(ValidationHelper.createValidationErrorMessage(field, value, message).getErrors());
  }
}
