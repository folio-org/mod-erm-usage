package org.olf.erm.usage.harvester;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.folio.rest.jaxrs.model.Aggregator;
import org.folio.rest.jaxrs.model.AggregatorSetting;
import org.folio.rest.jaxrs.model.UdProvidersDataCollection;
import org.folio.rest.jaxrs.model.UsageDataProvider;
import org.folio.rest.jaxrs.model.UsageDataProvider.HarvestingStatus;
import org.folio.rest.util.Constants;
import org.olf.erm.usage.harvester.endpoints.ServiceEndpoint;
import com.google.common.net.HttpHeaders;
import com.google.common.net.MediaType;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;

public class HarvesterVerticle extends AbstractVerticle {

  private static final Logger LOG = Logger.getLogger(HarvesterVerticle.class);
  private static final String ERR_MSG_STATUS = "Received status code %s, %s from %s";
  private static final String ERR_MSG_DECODE = "Error decoding response from %s, %s";

  private String okapiUrl;
  private String tenantsPath;
  private String reportsPath;
  private String providerPath;
  private String aggregatorPath;
  private String moduleId;

  public Future<List<String>> getTenants() {
    Future<List<String>> future = Future.future();

    final String url = okapiUrl + tenantsPath;
    WebClient client = WebClient.create(vertx);
    client.getAbs(url).send(ar -> {
      client.close();
      if (ar.succeeded()) {
        if (ar.result().statusCode() == 200) {
          JsonArray jsonArray;
          try {
            jsonArray = ar.result().bodyAsJsonArray();
            if (!jsonArray.isEmpty()) {
              List<String> tenants = jsonArray.stream()
                  .map(o -> ((JsonObject) o).getString("id"))
                  .collect(Collectors.toList());
              LOG.info("Found tenants: " + tenants);
              future.complete(tenants);
            } else {
              future.fail("No tenants found.");
            }
          } catch (Exception e) {
            future.fail(String.format(ERR_MSG_DECODE, url, e.getMessage()));
          }
        } else {
          future.fail(String.format(ERR_MSG_STATUS, ar.result().statusCode(),
              ar.result().statusMessage(), url));
        }
      } else {
        future.fail(ar.cause());
      }
    });
    return future;
  }

  public Future<Void> hasEnabledModule(String tenantId) {
    final String logprefix = "Tenant: " + tenantId + ", ";
    final String moduleUrl = okapiUrl + tenantsPath + "/" + tenantId + "/modules/" + moduleId;

    Future<Void> future = Future.future();
    WebClient client = WebClient.create(vertx);
    client.getAbs(moduleUrl).send(ar -> {
      client.close();
      if (ar.succeeded()) {
        Boolean hasUsageModule = false;
        if (ar.result().statusCode() == 200) {
          try {
            hasUsageModule = ar.result().bodyAsJsonObject().getString("id").equals(moduleId);
            future.complete();
          } catch (Exception e) {
            future.fail(logprefix + String.format(ERR_MSG_DECODE, moduleUrl, e.getMessage()));
          }
        } else {
          future.fail(logprefix + String.format(ERR_MSG_STATUS, ar.result().statusCode(),
              ar.result().statusMessage(), moduleUrl));
        }
        LOG.info(logprefix + "module enabled: " + hasUsageModule);
      } else {
        future.fail(ar.cause());
      }
    });
    return future;
  }

  // TODO: handle limits > 30
  public Future<UdProvidersDataCollection> getProviders(String tenantId) {
    final String logprefix = "Tenant: " + tenantId + ", ";
    final String url = okapiUrl + providerPath;
    LOG.info(logprefix + "getting providers");
    Future<UdProvidersDataCollection> future = Future.future();

    WebClient client = WebClient.create(vertx);
    client.requestAbs(HttpMethod.GET, url)
        .putHeader(Constants.OKAPI_HEADER_TENANT, tenantId)
        .putHeader(HttpHeaders.ACCEPT, MediaType.JSON_UTF_8.toString())
        .setQueryParam("limit", "30")
        .setQueryParam("offset", "0")
        .send(ar -> {
          client.close();
          if (ar.succeeded()) {
            if (ar.result().statusCode() == 200) {
              UdProvidersDataCollection entity;
              try {
                entity = ar.result().bodyAsJson(UdProvidersDataCollection.class);
                LOG.info(logprefix + "total providers: " + entity.getTotalRecords());
                future.complete(entity);
              } catch (Exception e) {
                future.fail(logprefix + String.format(ERR_MSG_DECODE, url, e.getMessage()));
              }
            } else {
              future.fail(logprefix + String.format(ERR_MSG_STATUS, ar.result().statusCode(),
                  ar.result().statusMessage(), url));
            }
          } else {
            future.fail(logprefix + "error: " + ar.cause().getMessage());
          }
        });
    return future;
  }

  public Future<AggregatorSetting> getAggregatorSetting(String tenantId,
      UsageDataProvider provider) {
    final String logprefix = "Tenant: " + tenantId + ", ";
    Future<AggregatorSetting> future = Future.future();

    Aggregator aggregator = provider.getAggregator();
    if (aggregator == null || aggregator.getId() == null) {
      return Future
          .failedFuture(logprefix + "no aggregator found for provider " + provider.getLabel());
    }

    final String aggrUrl = okapiUrl + aggregatorPath + "/" + aggregator.getId();
    WebClient client = WebClient.create(vertx);
    client.requestAbs(HttpMethod.GET, aggrUrl)
        .putHeader(Constants.OKAPI_HEADER_TENANT, tenantId)
        .putHeader(HttpHeaders.ACCEPT, MediaType.JSON_UTF_8.toString())
        .send(ar -> {
          client.close();
          if (ar.succeeded()) {
            if (ar.result().statusCode() == 200) {
              try {
                AggregatorSetting setting = ar.result().bodyAsJson(AggregatorSetting.class);
                LOG.info(logprefix + "got AggregatorSetting for id: " + aggregator.getId());
                future.complete(setting);
              } catch (Exception e) {
                future.fail(logprefix + String.format(ERR_MSG_DECODE, aggrUrl, e.getMessage()));
              }
            } else {
              future.fail(logprefix + String.format(ERR_MSG_STATUS, ar.result().statusCode(),
                  ar.result().statusMessage(), aggrUrl));
            }
          } else {
            future.fail(logprefix + "failed getting AggregatorSetting for id: " + aggregator.getId()
                + ", " + ar.cause().getMessage());
          }
        });
    return future;
  }

  public JsonObject createReportJsonObject(String reportData, String reportName,
      UsageDataProvider provider, String begin, String end) {
    JsonObject cr = new JsonObject();
    cr.put("beginDate", begin);
    cr.put("reportName", reportName);
    cr.put("platformId", provider.getPlatformId());
    cr.put("customerId", provider.getCustomerId());
    cr.put("release", provider.getReportRelease());
    cr.put("format", "???"); // FIXME
    cr.put("downloadTime", LocalDateTime.now().toString()); // FIXME
    cr.put("creationTime", LocalDateTime.now().toString()); // FIXME
    cr.put("endDate", end);
    cr.put("vendorId", provider.getVendorId());
    cr.put("report", reportData);
    cr.put("id", UUID.randomUUID().toString());
    return cr;
  }

  public Future<ServiceEndpoint> getServiceEndpoint(String tenantId, UsageDataProvider provider) {
    Future<AggregatorSetting> aggrFuture = Future.future();
    Future<ServiceEndpoint> sepFuture = Future.future();

    Aggregator aggregator = provider.getAggregator();
    // Complete aggrFuture if aggregator is not set.. aka skip it
    if (aggregator != null) {
      aggrFuture = getAggregatorSetting(tenantId, provider);
    } else {
      aggrFuture.complete(null);
    }

    aggrFuture.compose(as -> {
      ServiceEndpoint sep = ServiceEndpoint.create(vertx, provider, as);
      sepFuture.complete(sep);
    }, sepFuture);

    return sepFuture;
  }

  public Future<List<FetchItem>> getFetchList(String tenantId, UsageDataProvider provider) {
    final String logprefix = "Tenant: " + tenantId + ", ";

    // check if harvesting status is 'active'
    if (!provider.getHarvestingStatus().equals(HarvestingStatus.ACTIVE)) {
      LOG.info(logprefix + "skipping " + provider.getLabel() + " as harvesting status is "
          + provider.getHarvestingStatus());
      return Future.failedFuture("Harvesting not active");
    }

    List<FetchItem> list = new ArrayList<>();
    provider.getRequestedReports().forEach(r ->
    // TODO: implement logic to determine the dates to be processed here
    list.add(new FetchItem(r, "2016-03-01", "2016-03-31")));

    return Future.succeededFuture(list);
  }

  public void fetchAndPostReports(String tenantId, UsageDataProvider provider) {
    final String logprefix = "Tenant: " + tenantId + ", ";
    LOG.info(logprefix + "processing provider: " + provider.getLabel());

    getServiceEndpoint(tenantId, provider).map(sep -> {
      if (sep != null) {
        getFetchList(tenantId, provider).compose(list -> {
          list.forEach(li -> sep.fetchSingleReport(li.reportType, li.begin, li.end).compose(rep -> {
            JsonObject crJson =
                createReportJsonObject(rep, li.reportType, provider, li.begin, li.end);
            return postReport(tenantId, crJson);
          }));
          return Future.succeededFuture();
        });
      }
      return Future.failedFuture("No ServiceEndpoint");
    });
  }

  public Future<HttpResponse<Buffer>> postReport(String tenantId, JsonObject reportContent) {
    final String logprefix = "Tenant: " + tenantId + ", ";
    final String url = okapiUrl + reportsPath;
    final Future<HttpResponse<Buffer>> future = Future.future();

    LOG.info(logprefix + "posting report with data " + reportContent);

    WebClient client = WebClient.create(vertx);
    client.requestAbs(HttpMethod.POST, url)
        .putHeader(Constants.OKAPI_HEADER_TENANT, tenantId)
        .putHeader("accept", "application/json")
        .sendJsonObject(reportContent, ar -> {
          if (ar.succeeded()) {
            // TODO: check for 201 created and 204 no content
            LOG.info(logprefix + String.format(ERR_MSG_STATUS, ar.result().statusCode(),
                ar.result().statusMessage(), url));
            future.complete(ar.result());
          } else {
            LOG.error(ar.cause());
            future.fail(ar.cause());
          }
        });

    return future;
  }

  public void run() {
    getTenants()
        .compose(tenants -> tenants.forEach(t -> hasEnabledModule(t).compose(
            f -> getProviders(t).compose(providers -> providers.getUsageDataProviders()
                .forEach(p -> fetchAndPostReports(t, p)), handleErrorFuture()),
            handleErrorFuture())), handleErrorFuture());
  }

  private Future<Object> handleErrorFuture() {
    return Future.future().setHandler(ar -> LOG.error(ar.cause().getMessage()));
  }

  public String getOkapiUrl() {
    return okapiUrl;
  }

  public void setOkapiUrl(String okapiUrl) {
    this.okapiUrl = okapiUrl;
  }

  public String getTenantsPath() {
    return tenantsPath;
  }

  public void setTenantsPath(String tenantsPath) {
    this.tenantsPath = tenantsPath;
  }

  public String getReportsPath() {
    return reportsPath;
  }

  public void setReportsPath(String reportsPath) {
    this.reportsPath = reportsPath;
  }

  public String getProviderPath() {
    return providerPath;
  }

  public void setProviderPath(String providerPath) {
    this.providerPath = providerPath;
  }

  public String getModuleId() {
    return moduleId;
  }

  public void setModuleId(String moduleId) {
    this.moduleId = moduleId;
  }

  public String getAggregatorPath() {
    return aggregatorPath;
  }

  public void setAggregatorPath(String aggregatorPath) {
    this.aggregatorPath = aggregatorPath;
  }

  @Override
  public void start(Future<Void> startFuture) throws Exception {
    // read configuration and setup class variables
    okapiUrl = config().getString("okapiUrl");
    tenantsPath = config().getString("tenantsPath");
    reportsPath = config().getString("reportsPath");
    providerPath = config().getString("providerPath");
    aggregatorPath = config().getString("aggregatorPath");
    moduleId = config().getString("moduleId");

    if (StringUtils.isAnyBlank(okapiUrl, tenantsPath, reportsPath, providerPath, aggregatorPath,
        moduleId)) {
      startFuture.fail("No or incomplete configuration found. Use -conf argument");
    } else {
      startFuture.complete();

      // only start processing if not in test
      if (!config().getBoolean("testing", false)) {
        run();
      }
    }
  }
}
