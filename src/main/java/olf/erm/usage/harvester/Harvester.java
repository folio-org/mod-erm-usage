package olf.erm.usage.harvester;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.log4j.Logger;
import org.folio.rest.jaxrs.model.Aggregator;
import org.folio.rest.jaxrs.model.AggregatorSetting;
import org.folio.rest.jaxrs.model.UdProvidersDataCollection;
import org.folio.rest.jaxrs.model.UsageDataProvider;
import org.folio.rest.jaxrs.model.UsageDataProvider.HarvestingStatus;
import org.folio.rest.util.Constants;
import com.google.common.net.HttpHeaders;
import com.google.common.net.MediaType;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import olf.erm.usage.harvester.endpoints.ServiceEndpoint;

public class Harvester {

  private static final Logger LOG = Logger.getLogger(Harvester.class);
  private static final String ERR_MSG_STATUS = "Received status code %s, %s from %s";
  private static final String ERR_MSG_DECODE = "Error decoding response from %s, %s";
  private Vertx vertx = Vertx.vertx();

  public Future<List<String>> getTenants(String url) {
    Future<List<String>> future = Future.future();
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

  public Future<Void> hasEnabledModule(String url, String tenantId, String moduleId) {
    final String logprefix = "Tenant: " + tenantId + ", ";
    final String moduleUrl = url + "/" + tenantId + "/modules/" + moduleId;

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
  public Future<UdProvidersDataCollection> getProviders(String url, String tenantId) {
    final String logprefix = "Tenant: " + tenantId + ", ";
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

  public Future<AggregatorSetting> getAggregatorSetting(String url, String tenantId,
      UsageDataProvider provider) {
    final String logprefix = "Tenant: " + tenantId + ", ";
    Future<AggregatorSetting> future = Future.future();

    Aggregator aggregator = provider.getAggregator();
    if (aggregator == null || aggregator.getId() == null) {
      return Future
          .failedFuture(logprefix + "no aggregator found for provider " + provider.getLabel());
    }

    final String aggrUrl = url + "/" + aggregator.getId();
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

  public Future<JsonObject> fetchSingleReport(ServiceEndpoint sep, String report, String begin,
      String end, String tenantId) {
    final String logprefix = "Tenant: " + tenantId + ", ";
    final String url = sep.buildURL(report, begin, end);
    LOG.info(logprefix + "fetching report from URL: " + url);

    WebClient client = WebClient.create(vertx);
    Future<JsonObject> future = Future.future();
    client.getAbs(url).send(ar -> {
      client.close();
      if (ar.succeeded()) {
        if (ar.result().statusCode() == 200) {
          JsonObject cr = new JsonObject();
          cr.put("beginDate", begin);
          cr.put("reportName", report);
          cr.put("platformId", sep.getProvider().getPlatformId());
          cr.put("customerId", sep.getProvider().getCustomerId());
          cr.put("release", sep.getProvider().getReportRelease());
          cr.put("format", "???"); // FIXME
          cr.put("downloadTime", ar.result().getHeader(HttpHeaders.DATE));
          cr.put("creationTime", LocalDateTime.now().toString()); // FIXME
          cr.put("endDate", end);
          cr.put("vendorId", sep.getProvider().getVendorId());
          cr.put("report", ar.result().bodyAsString());
          cr.put("id", UUID.randomUUID().toString());
          future.complete(cr);
        } else {
          future.fail(logprefix + String.format(ERR_MSG_STATUS, ar.result().statusCode(),
              ar.result().statusMessage(), url));
        }
      } else {
        future.fail(logprefix + ar.cause());
      }
    });
    return future;
  }

  public void fetchReports(String url, String tenantId, UsageDataProvider provider) {
    final String logprefix = "Tenant: " + tenantId + ", ";
    LOG.info(logprefix + "processing provider: " + provider.getLabel());

    // check if harvesting status is 'active'
    if (!provider.getHarvestingStatus().equals(HarvestingStatus.ACTIVE)) {
      LOG.info(logprefix + "skipping " + provider.getLabel() + " as harvesting status is "
          + provider.getHarvestingStatus());
      return;
    }

    Aggregator aggregator = provider.getAggregator();
    // Complete aggrFuture if aggregator is not set.. aka skip it
    Future<AggregatorSetting> aggrFuture = Future.future();
    if (aggregator != null) {
      aggrFuture = getAggregatorSetting(url + "/aggregator-settings", tenantId, provider);
    } else {
      aggrFuture.complete(null);
    }

    aggrFuture.compose(as -> {
      ServiceEndpoint sep = ServiceEndpoint.create(provider, as);
      if (sep != null) {
        provider.getRequestedReports()
            .forEach(r -> fetchSingleReport(sep, r, "2018-03-01", "2018-03-31", tenantId)
                .compose(report -> postReport(url + "/counter-reports", tenantId, report)));
      }
      return Future.succeededFuture();
    });
  }

  public Future<Void> postReport(String url, String tenantId, JsonObject reportContent) {
    final String logprefix = "Tenant: " + tenantId + ", ";
    final Future<Void> future = Future.future();

    WebClient client = WebClient.create(vertx);
    client.requestAbs(HttpMethod.POST, url)
        .putHeader(Constants.OKAPI_HEADER_TENANT, tenantId)
        .putHeader("accept", "application/json")
        .sendJsonObject(reportContent, ar -> {
          if (ar.succeeded()) {
            LOG.info(String.format(ERR_MSG_STATUS, ar.result().statusCode(),
                ar.result().statusMessage(), url));
            future.complete();
          } else {
            LOG.error(ar.cause());
            future.fail(ar.cause());
          }
        });

    LOG.info(logprefix + "posting report with data " + reportContent);
    return future;
  }

  public void run() {
    String okapiUrl = "http://192.168.56.103:9130";
    String tenantsUrl = okapiUrl + "/_/proxy/tenants";
    String moduleId = "mod-erm-usage-0.0.1";
    String providerPath = "/usage-data-providers";
    getTenants(tenantsUrl).compose(tenants -> tenants.forEach(t -> {
      hasEnabledModule(tenantsUrl, t, moduleId).compose(f -> {
        getProviders(okapiUrl + providerPath, t)
            .compose(providers -> providers.getUsageDataProviders()
                .forEach(p -> fetchReports(okapiUrl, t, p)), handleErrorFuture());
      }, handleErrorFuture());
    }), handleErrorFuture());
  }

  private Future<Object> handleErrorFuture() {
    return Future.future().setHandler(ar -> LOG.error(ar.cause().getMessage()));
  }

  public static void main(String[] args) {
    new Harvester().run();
  }
}
