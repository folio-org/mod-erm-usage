package org.folio.rest.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.folio.rest.jaxrs.model.Error;
import org.folio.rest.jaxrs.model.HarvestingConfig;
import org.folio.rest.jaxrs.model.HarvestingConfig.HarvestingStatus;
import org.folio.rest.jaxrs.model.Parameter;
import org.folio.rest.jaxrs.model.SushiConfig;
import org.folio.rest.jaxrs.model.SushiCredentials;
import org.folio.rest.jaxrs.model.UsageDataProvider;
import org.folio.rest.jaxrs.model.UsageDataProvider.Status;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class UsageDataProviderValidatorTest {

  private static UsageDataProvider createActiveUdp() {
    return new UsageDataProvider()
        .withLabel("Provider")
        .withStatus(Status.ACTIVE)
        .withHarvestingConfig(
            new HarvestingConfig()
                .withHarvestingStatus(HarvestingStatus.ACTIVE)
                .withHarvestingStart("2024-01")
                .withSushiConfig(
                    new SushiConfig().withServiceType("cs50").withServiceUrl("https://x.org")))
        .withSushiCredentials(new SushiCredentials().withCustomerId("customer"));
  }

  private static List<String> validate(UsageDataProvider udp) {
    return UsageDataProviderValidator.validate(udp).getErrors().stream()
        .map(Error::getParameters)
        .flatMap(List::stream)
        .map(Parameter::getKey)
        .toList();
  }

  @Test
  void validActiveUdpIsAccepted() {
    assertThat(validate(createActiveUdp())).isEmpty();
  }

  @Test
  void activeUdpWithoutStatusIsAccepted() {
    assertThat(validate(createActiveUdp().withStatus(null))).isEmpty();
  }

  @Test
  void minimalUdpWithInactiveHarvestingIsAccepted() {
    UsageDataProvider udp =
        new UsageDataProvider()
            .withLabel("Provider")
            .withStatus(Status.INACTIVE)
            .withHarvestingConfig(
                new HarvestingConfig().withHarvestingStatus(HarvestingStatus.INACTIVE));
    assertThat(validate(udp)).isEmpty();
  }

  @Test
  void inactiveHarvestingSkipsAllOtherRules() {
    UsageDataProvider udp = createActiveUdp().withSushiCredentials(null);
    udp.getHarvestingConfig()
        .withHarvestingStatus(HarvestingStatus.INACTIVE)
        .withHarvestingStart(null)
        .withRequestedReports(List.of("JR1"))
        .withSushiConfig(new SushiConfig().withServiceType("unsupported").withServiceUrl("x.org"));
    assertThat(validate(udp)).isEmpty();
  }

  @Test
  void inactiveStatusWithActiveHarvestingIsRejected() {
    assertThat(validate(createActiveUdp().withStatus(Status.INACTIVE)))
        .containsExactly("harvestingConfig.harvestingStatus");
  }

  @ParameterizedTest
  @NullAndEmptySource
  void missingHarvestingStartIsRejected(String value) {
    UsageDataProvider udp = createActiveUdp();
    udp.getHarvestingConfig().setHarvestingStart(value);
    assertThat(validate(udp)).containsExactly("harvestingConfig.harvestingStart");
  }

  @ParameterizedTest
  @NullAndEmptySource
  void missingServiceTypeIsRejected(String value) {
    UsageDataProvider udp = createActiveUdp();
    udp.getHarvestingConfig().getSushiConfig().setServiceType(value);
    assertThat(validate(udp)).containsExactly("harvestingConfig.sushiConfig.serviceType");
  }

  @ParameterizedTest
  @NullAndEmptySource
  void missingServiceUrlIsRejected(String value) {
    UsageDataProvider udp = createActiveUdp();
    udp.getHarvestingConfig().getSushiConfig().setServiceUrl(value);
    assertThat(validate(udp)).containsExactly("harvestingConfig.sushiConfig.serviceUrl");
  }

  @ParameterizedTest
  @NullAndEmptySource
  void missingCustomerIdIsRejected(String value) {
    UsageDataProvider udp = createActiveUdp();
    udp.getSushiCredentials().setCustomerId(value);
    assertThat(validate(udp)).containsExactly("sushiCredentials.customerId");
  }

  @Test
  void missingSushiObjectsAreRejected() {
    UsageDataProvider udp = createActiveUdp().withSushiCredentials(null);
    udp.getHarvestingConfig().setSushiConfig(null);
    assertThat(validate(udp))
        .containsExactlyInAnyOrder(
            "harvestingConfig.sushiConfig.serviceType",
            "harvestingConfig.sushiConfig.serviceUrl",
            "sushiCredentials.customerId");
  }

  @ParameterizedTest
  @ValueSource(strings = {"http://x", "https://x.org/sushi", "https://x y"})
  void validServiceUrlIsAccepted(String serviceUrl) {
    UsageDataProvider udp = createActiveUdp();
    udp.getHarvestingConfig().getSushiConfig().setServiceUrl(serviceUrl);
    assertThat(validate(udp)).isEmpty();
  }

  @ParameterizedTest
  @ValueSource(strings = {"ftp://x.org", "x.org", "https://", "https:// x.org", "HTTPS://x.org"})
  void invalidServiceUrlIsRejected(String serviceUrl) {
    UsageDataProvider udp = createActiveUdp();
    udp.getHarvestingConfig().getSushiConfig().setServiceUrl(serviceUrl);
    assertThat(validate(udp)).containsExactly("harvestingConfig.sushiConfig.serviceUrl");
  }

  @Test
  void allViolationsAreReported() {
    UsageDataProvider udp = createActiveUdp().withStatus(Status.INACTIVE);
    udp.getHarvestingConfig().setHarvestingStart(null);
    udp.getHarvestingConfig().getSushiConfig().setServiceUrl("x.org");
    assertThat(validate(udp))
        .containsExactlyInAnyOrder(
            "harvestingConfig.harvestingStatus",
            "harvestingConfig.harvestingStart",
            "harvestingConfig.sushiConfig.serviceUrl");
  }
}
