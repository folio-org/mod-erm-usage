package org.folio.rest.util;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.util.Objects;
import java.util.stream.Collectors;
import org.folio.rest.jaxrs.model.HarvestingConfig;
import org.folio.rest.jaxrs.model.SushiCredentials;
import org.folio.rest.jaxrs.model.UsageDataProvider;

@JsonPropertyOrder({
  "providerName",
  "harvestingStatus",
  "reportRelease",
  "requestedReports",
  "customerId",
  "requestorId",
  "apiKey",
  "requestorName",
  "requestorMail"
})
public class ExportObject {
  private String providerName;
  private String harvestingStatus;
  private String reportRelease;
  private String requestedReports;
  private String customerId;
  private String requestorId;
  private String apiKey;
  private String requestorName;
  private String requestorMail;

  public String getHarvestingStatus() {
    return harvestingStatus;
  }

  public String getReportRelease() {
    return reportRelease;
  }

  public String getRequestedReports() {
    return requestedReports;
  }

  public String getProviderName() {
    return providerName;
  }

  public String getCustomerId() {
    return customerId;
  }

  public String getRequestorId() {
    return requestorId;
  }

  public String getApiKey() {
    return apiKey;
  }

  public String getRequestorName() {
    return requestorName;
  }

  @JsonProperty("requestorMail")
  public String getRequestorMail() {
    return requestorMail;
  }

  public ExportObject(UsageDataProvider provider) {
    if (provider.getHarvestingConfig() == null)
      provider.setHarvestingConfig(new HarvestingConfig());
    if (provider.getSushiCredentials() == null)
      provider.setSushiCredentials(new SushiCredentials());

    this.providerName = provider.getLabel();
    this.harvestingStatus =
        Objects.toString(provider.getHarvestingConfig().getHarvestingStatus(), "");
    this.reportRelease = Objects.toString(provider.getHarvestingConfig().getReportRelease(), "");
    this.requestedReports =
        provider.getHarvestingConfig().getRequestedReports().stream()
            .collect(Collectors.joining(", "));
    this.customerId = provider.getSushiCredentials().getCustomerId();
    this.requestorId = provider.getSushiCredentials().getRequestorId();
    this.apiKey = provider.getSushiCredentials().getApiKey();
    this.requestorName = provider.getSushiCredentials().getRequestorName();
    this.requestorMail = provider.getSushiCredentials().getRequestorMail();
  }
}
