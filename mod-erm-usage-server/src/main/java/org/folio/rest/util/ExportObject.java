package org.folio.rest.util;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.text.SimpleDateFormat;
import java.util.Objects;
import java.util.TimeZone;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
  "requestorMail",
  "createdDate",
  "updatedDate"
})
public class ExportObject {
  private static final Logger LOG = LogManager.getLogger(ExportObject.class);

  private String providerName;
  private String harvestingStatus;
  private String reportRelease;
  private String requestedReports;
  private String customerId;
  private String requestorId;
  private String apiKey;
  private String requestorName;
  private String requestorMail;
  private String createdDate;
  private String updatedDate;

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

  public String getRequestorMail() {
    return requestorMail;
  }

  public String getCreatedDate() {
    return createdDate;
  }

  public String getUpdatedDate() {
    return updatedDate;
  }

  public ExportObject(UsageDataProvider provider) {
    if (provider.getHarvestingConfig() == null)
      provider.setHarvestingConfig(new HarvestingConfig());
    if (provider.getSushiCredentials() == null)
      provider.setSushiCredentials(new SushiCredentials());

    this.providerName = provider.getLabel();
    this.harvestingStatus =
        Objects.toString(provider.getHarvestingConfig().getHarvestingStatus(), null);
    this.reportRelease = provider.getHarvestingConfig().getReportRelease();
    this.requestedReports = String.join(", ", provider.getHarvestingConfig().getRequestedReports());
    this.customerId = provider.getSushiCredentials().getCustomerId();
    this.requestorId = provider.getSushiCredentials().getRequestorId();
    this.apiKey = provider.getSushiCredentials().getApiKey();
    this.requestorName = provider.getSushiCredentials().getRequestorName();
    this.requestorMail = provider.getSushiCredentials().getRequestorMail();
    if (provider.getMetadata() != null) {
      try {
        final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        if (provider.getMetadata().getCreatedDate() != null)
          this.createdDate = dateFormat.format(provider.getMetadata().getCreatedDate());
        if (provider.getMetadata().getUpdatedDate() != null)
          this.updatedDate = dateFormat.format(provider.getMetadata().getUpdatedDate());
      } catch (Exception e) {
        LOG.error("Error getting Metadata", e);
      }
    }
  }
}
