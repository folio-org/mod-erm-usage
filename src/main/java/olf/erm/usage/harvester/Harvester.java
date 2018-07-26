package olf.erm.usage.harvester;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.log4j.Logger;
import org.folio.rest.impl.AggregatorSettingsAPI;
import org.folio.rest.impl.UsageDataProvidersAPI;
import org.folio.rest.jaxrs.model.Aggregator;
import org.folio.rest.jaxrs.model.AggregatorSetting;
import org.folio.rest.jaxrs.model.UdProvidersDataCollection;
import org.folio.rest.jaxrs.model.UsageDataProvider;
import org.folio.rest.jaxrs.model.UsageDataProvider.HarvestingStatus;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.rest.util.Constants;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import olf.erm.usage.harvester.endpoints.ServiceEndpoint;

public class Harvester {

  private static final Logger LOG = Logger.getLogger(Harvester.class);
  private String okapiUrl = "http://192.168.56.103:9130";
  private String tenantsPath = "/_/proxy/tenants";
  private String moduleId = "mod-erm-usage-0.0.1";

  private Vertx vertx = Vertx.vertx();

  private Map<String, String> createOkapiHeaders(String tenantId) {
    Map<String, String> okapiHeaders = new HashMap<>();
    okapiHeaders.put(Constants.OKAPI_HEADER_TENANT, TenantTool.calculateTenantId(tenantId));
    okapiHeaders.put("accept", "application/json");
    return okapiHeaders;
  }

  public Future<List<String>> getTenants(String url) {
    Future<List<String>> future = Future.future();
    WebClient client = WebClient.create(vertx);
    client.getAbs(url).send(ar -> {
      if (ar.succeeded()) {
        if (200 == ar.result().statusCode()) {
          JsonArray jsonarray;
          try {
            jsonarray = ar.result().bodyAsJsonArray();
          } catch (Exception e) {
            future.fail("Did not receive a JsonArray from " + url + ". " + e.getMessage());
            client.close();
            return;
          }
          if (!jsonarray.isEmpty()) {
            List<String> tenants = jsonarray.stream()
                .map(o -> ((JsonObject) o).getString("id"))
                .collect(Collectors.toList());
            LOG.info("Found tenants: " + tenants);
            future.complete(tenants);
          } else {
            future.fail("No tenants found.");
          }
        } else {
          future.fail("Received status code " + ar.result().statusCode() + " from " + url);
        }
      } else {
        future.fail(ar.cause());
      }
      client.close();
    });
    return future;
  }

  private Future<Void> hasUsageModule(String tenantId) {
    final String logprefix = "Tenant: " + tenantId + ", ";
    Future<Void> future = Future.future();
    WebClient client = WebClient.create(vertx);
    client.getAbs(okapiUrl + tenantsPath + "/" + tenantId + "/modules/" + moduleId).send(ar -> {
      if (ar.succeeded()) {
        Boolean hasUsageModule = false;
        if (ar.result().statusCode() == 200
            && ar.result().bodyAsJsonObject().getString("id").equals(moduleId)) {
          hasUsageModule = true;
          future.complete();
        } else {
          future.fail(logprefix + "received status code " + ar.result().statusCode() + " - "
              + ar.result().statusMessage());
        }
        LOG.info(logprefix + "module enabled: " + hasUsageModule);
      } else {
        future.fail(ar.cause());
      }
      client.close();
    });
    return future;
  }

  private Future<UdProvidersDataCollection> getProviders(String tenantId) {
    final String logprefix = "Tenant: " + tenantId + ", ";
    LOG.info(logprefix + "getting providers");
    Future<UdProvidersDataCollection> future = Future.future();

    // call UsageDataProviders API
    UsageDataProvidersAPI api = new UsageDataProvidersAPI(vertx, tenantId);
    try {
      api.getUsageDataProviders(null, null, null, 0, 30, null, createOkapiHeaders(tenantId), ar -> {
        if (ar.succeeded()) {
          UdProvidersDataCollection entity = (UdProvidersDataCollection) ar.result().getEntity();
          LOG.info(logprefix + "total providers: " + entity.getTotalRecords());
          future.complete(entity);
        } else {
          future.fail(logprefix + "error: " + ar.cause().getMessage());
        }
      }, vertx.getOrCreateContext());
    } catch (Exception e) {
      future.fail(logprefix + "error: " + e.getMessage());
    }
    return future;
  }

  private Future<AggregatorSetting> getAggregatorSetting(String tenantId,
      UsageDataProvider provider) {
    Future<AggregatorSetting> future = Future.future();
    Aggregator aggregator = provider.getAggregator();
    AggregatorSettingsAPI api = new AggregatorSettingsAPI(vertx, tenantId);
    try {
      api.getAggregatorSettingsById(aggregator.getId(), null, createOkapiHeaders(tenantId), ar -> {
        if (ar.succeeded()) {
          AggregatorSetting setting = (AggregatorSetting) ar.result().getEntity();
          LOG.info("Tenant: " + tenantId + ", got AggregatorSetting for id: " + aggregator.getId());
          future.complete(setting);
        } else {
          future.fail("Tenant: " + tenantId + ", failed getting AggregatorSetting for id: "
              + aggregator.getId() + ", " + ar.cause().getMessage());
        }
      }, vertx.getOrCreateContext());
    } catch (Exception e) {
      future.fail("Tenant: " + tenantId + ", failed getting AggregatorSetting for id: "
          + aggregator.getId() + ", " + e.getMessage());
    }
    return future;
  }

  private Future<String> fetchSingleReport(String tenantId, String url) {
    final String logprefix = "Tenant: " + tenantId + ", ";
    LOG.info(logprefix + "fetching report from URL: " + url);

    // WebClient client = WebClient.create(vertx);
    // Future<String> future = Future.future();
    // client.getAbs(url).send(ar -> {
    // if (ar.succeeded()) {
    // future.complete(ar.result().bodyAsString());
    // } else {
    // future.fail(ar.cause());
    // }
    // client.close();
    // });
    // return future;

    return Future.succeededFuture("RANDOM REPORT DATA " + RandomStringUtils.randomAlphanumeric(12));
  }

  private void fetchReports(String tenantId, UsageDataProvider provider) {
    final String logprefix = "Tenant: " + tenantId + ", ";
    LOG.info(logprefix + "processing provider: " + provider.getLabel());

    Aggregator aggregator = provider.getAggregator();

    // check if harvesting status is 'active'
    if (!provider.getHarvestingStatus().equals(HarvestingStatus.ACTIVE)) {
      LOG.info(logprefix + "skipping " + provider.getLabel() + " as harvesting status is "
          + provider.getHarvestingStatus());
      return;
    }

    // Complete aggrFuture if aggregator is not set.. aka skip it
    Future<AggregatorSetting> aggrFuture = Future.future();
    if (aggregator != null) {
      aggrFuture = getAggregatorSetting(tenantId, provider);
    } else {
      aggrFuture.complete(null);
    }

    aggrFuture.compose(as -> {
      ServiceEndpoint sep = ServiceEndpoint.create(provider, as);
      if (sep != null) {
        provider.getRequestedReports()
            .forEach(r -> fetchSingleReport(tenantId, sep.buildURL(r, "2018-03-01", "2018-03-31"))
                .compose(report -> postReport(tenantId, report)));
      }
      return Future.succeededFuture();
    });
  }

  private Future<Void> postReport(String tenantId, String reportContent) {
    final String logprefix = "Tenant: " + tenantId + ", ";

    // CounterReportAPI api = new CounterReportAPI(vertx, tenantId);
    // CounterReport cr = new CounterReport();
    // cr.setReport(reportContent);
    // Future<Void> future = Future.future();
    //
    // Handler<AsyncResult<Response>> asyncResultHandler = (ar) -> {
    // LOG.info("result successful: " + ar.succeeded());
    // if (ar.succeeded()) {
    // if (ar.result().getStatus() == 201) {
    // future.complete();
    // } else {
    // future.fail(
    // "Got status code " + ar.result().getStatus() + " " + ar.result().getStatusInfo());
    // }
    // } else {
    // future.fail(ar.cause());
    // }
    // };
    //
    // try {
    // api.postCounterReports(null, cr, createOkapiHeaders("diku"), asyncResultHandler,
    // vertx.getOrCreateContext());
    // } catch (Exception e) {
    // future.fail(e);
    // e.printStackTrace();
    // }
    //
    // return future;

    LOG.info(logprefix + "posting report with data " + reportContent);
    return Future.succeededFuture();
  }

  public Harvester() {}

  public Harvester(String okapiUrl, String tenantsPath, String moduleId) {
    this.okapiUrl = okapiUrl;
    this.tenantsPath = tenantsPath;
    this.moduleId = moduleId;
  }

  public void run() {
    getTenants(
        okapiUrl + tenantsPath)
            .compose(
                tenants -> tenants
                    .forEach(t -> hasUsageModule(t).compose(f -> getProviders(t).compose(
                        providers -> providers.getUsageDataProviders()
                            .forEach(p -> fetchReports(t, p)),
                        Future.future().setHandler(ar -> LOG.error(ar.cause().getMessage()))))),
                Future.future().setHandler(ar -> LOG.error(ar.cause().getMessage())));
  }

  public static void main(String[] args) {
    new Harvester().run();
  }
}
