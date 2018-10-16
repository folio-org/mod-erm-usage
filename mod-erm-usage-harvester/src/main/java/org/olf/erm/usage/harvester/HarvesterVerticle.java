package org.olf.erm.usage.harvester;

import java.sql.Date;
import java.time.Instant;
import java.time.LocalDate;
import java.time.YearMonth;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.rest.jaxrs.model.Aggregator;
import org.folio.rest.jaxrs.model.AggregatorSetting;
import org.folio.rest.jaxrs.model.CounterReport;
import org.folio.rest.jaxrs.model.CounterReports;
import org.folio.rest.jaxrs.model.UsageDataProvider;
import org.folio.rest.jaxrs.model.UsageDataProvider.HarvestingStatus;
import org.folio.rest.jaxrs.model.UsageDataProviders;
import org.olf.erm.usage.harvester.endpoints.ServiceEndpoint;
import com.google.common.base.Strings;
import com.google.common.net.HttpHeaders;
import com.google.common.net.MediaType;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
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
  private String loginPath = "/bl-users/login";
  private String requiredPerm = "ermusage.all";

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
            List<String> tenants = jsonArray.stream()
                .map(o -> ((JsonObject) o).getString("id"))
                .collect(Collectors.toList());
            LOG.info("Found tenants: " + tenants);
            future.complete(tenants);
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

  public Future<Boolean> hasEnabledUsageModule(String tenantId) {
    final String logprefix = "Tenant: " + tenantId + ", ";
    final String moduleUrl = okapiUrl + tenantsPath + "/" + tenantId + "/modules/" + moduleId;

    Future<Boolean> future = Future.future();
    WebClient client = WebClient.create(vertx);
    client.getAbs(moduleUrl).send(ar -> {
      client.close();
      if (ar.succeeded()) {
        if (ar.result().statusCode() == 200) {
          try {
            future.complete(ar.result().bodyAsJsonObject().getString("id").equals(moduleId));
          } catch (Exception e) {
            future.fail(logprefix + String.format(ERR_MSG_DECODE, moduleUrl, e.getMessage()));
          }
        } else if (ar.result().statusCode() == 404) {
          future.complete(false);
        } else {
          future.fail(logprefix + String.format(ERR_MSG_STATUS, ar.result().statusCode(),
              ar.result().statusMessage(), moduleUrl));
        }
        // LOG.info(logprefix + "module enabled: " + hasUsageModule);
      } else {
        // LOG.error(ar.cause());
        future.fail(ar.cause());
      }
    });
    return future;
  }

  // TODO: handle limits > 30
  public Future<UsageDataProviders> getActiveProviders(String tenantId) {
    final String logprefix = "Tenant: " + tenantId + ", ";
    final String url = okapiUrl + providerPath;
    final String queryStr = String.format("(harvestingStatus=%s)", HarvestingStatus.ACTIVE);
    LOG.info(logprefix + "getting providers");

    Future<UsageDataProviders> future = Future.future();

    WebClient client = WebClient.create(vertx);
    client.requestAbs(HttpMethod.GET, url)
        .putHeader("x-okapi-tenant", tenantId)
        .putHeader("accept", "application/json,text/plain")
        .setQueryParam("limit", "30")
        .setQueryParam("offset", "0")
        .setQueryParam("query", queryStr)
        .send(ar -> {
          client.close();
          if (ar.succeeded()) {
            if (ar.result().statusCode() == 200) {
              UsageDataProviders entity;
              try {
                entity = ar.result().bodyAsJson(UsageDataProviders.class);
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
        .putHeader("x-okapi-tenant", tenantId)
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

  public CounterReport createCounterReport(String reportData, String reportName,
      UsageDataProvider provider, YearMonth yearMonth) {
    CounterReport cr = new CounterReport();
    cr.setId(UUID.randomUUID().toString());
    cr.setYearMonth(yearMonth.toString());
    cr.setReportName(reportName);
    cr.setPlatformId(provider.getPlatformId());
    cr.setCustomerId(provider.getCustomerId());
    cr.setRelease(provider.getReportRelease().toString()); // TODO: update release to be a integer
    cr.setDownloadTime(Date.from(Instant.now())); // FIXME
    cr.setCreationTime(Date.from(Instant.now())); // FIXME
    cr.setVendorId(provider.getVendorId());
    if (reportData != null) {
      cr.setFormat("???"); // FIXME
      cr.setReport(reportData);
    } else {
      cr.setFailedAttempts(1);
    }
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

  public Future<List<YearMonth>> getValidMonths(String tenantId, String vendorId, String reportName,
      YearMonth start, YearMonth end) {
    Future<List<YearMonth>> future = Future.future();
    WebClient client = WebClient.create(vertx);

    // TODO: report="" or NOT failedAttempts=""
    String queryStr = String.format(
        "(vendorId=%s AND report=\"\" AND reportName=%s AND yearMonth>=%s AND yearMonth<=%s)",
        vendorId, reportName, start.toString(), end.toString());
    client.getAbs(okapiUrl + reportsPath)
        .putHeader("x-okapi-tenant", tenantId)
        .putHeader("accept", "application/json,text/plain")
        .setQueryParam("query", queryStr)
        .setQueryParam("tiny", "true")
        .send(ar -> {
          if (ar.succeeded()) {
            // TODO: catch decode exception
            CounterReports result = ar.result().bodyAsJson(CounterReports.class);
            List<YearMonth> availableMonths = new ArrayList<>();
            result.getCounterReports().forEach(r -> {
              // TODO: catch parse exception
              // TODO: check r.getDownloadTime() and value of r.getFailedAttempts()
              if (r.getFailedAttempts() == null)
                availableMonths.add(YearMonth.parse(r.getYearMonth()));
            });
            future.complete(availableMonths);
          } else {
            future.fail(ar.cause());
          }
        });

    return future;
  }

  public Future<List<FetchItem>> getFetchList(String tenantId, UsageDataProvider provider) {
    final String logprefix = "Tenant: " + tenantId + ", ";

    // check if harvesting status is 'active'
    if (!provider.getHarvestingStatus().equals(HarvestingStatus.ACTIVE)) {
      LOG.info(logprefix + "skipping " + provider.getLabel() + " as harvesting status is "
          + provider.getHarvestingStatus());
      return Future.failedFuture("Harvesting not active");
    }

    Future<List<FetchItem>> future = Future.future();

    YearMonth startMonth = DateUtil.getStartMonth(provider.getHarvestingStart());
    YearMonth endMonth = DateUtil.getEndMonth(provider.getHarvestingEnd());

    List<FetchItem> fetchList = new ArrayList<>();

    @SuppressWarnings("rawtypes")
    List<Future> futures = new ArrayList<>();
    provider.getRequestedReports().forEach(reportName -> {
      futures.add(getValidMonths(tenantId, provider.getVendorId(), reportName, startMonth, endMonth)
          .map(list -> {
            List<YearMonth> arrayList =
                DateUtil.getYearMonths(provider.getHarvestingStart(), provider.getHarvestingEnd());
            arrayList.removeAll(list);
            arrayList.forEach(li -> fetchList.add(
                new FetchItem(reportName, li.atDay(1).toString(), li.atEndOfMonth().toString())));
            return Future.succeededFuture();
          }));
    });

    CompositeFuture.all(futures).setHandler(ar -> {
      if (ar.succeeded()) {
        future.complete(fetchList);
      } else {
        future.fail(ar.cause());
      }
    });

    return future;
  }

  public void fetchAndPostReports(String tenantId, UsageDataProvider provider) {
    final String logprefix = "Tenant: " + tenantId + ", ";
    LOG.info(logprefix + "processing provider: " + provider.getLabel());

    getServiceEndpoint(tenantId, provider).map(sep -> {
      if (sep != null) {
        getFetchList(tenantId, provider).compose(list -> {
          list.forEach(
              li -> sep.fetchSingleReport(li.reportType, li.begin, li.end).compose(reportData -> {
                // fetchSingleReport completes successful, we got a valid report
                // FIXME: Fix me
                LocalDate parse = LocalDate.parse(li.begin);
                YearMonth month = YearMonth.of(parse.getYear(), parse.getMonth());

                CounterReport report =
                    createCounterReport(reportData, li.reportType, provider, month);
                postReport(tenantId, report);
              }, handleErrorFuture("Tenant: " + tenantId + ", Provider: " + provider.getLabel()
                  + ", " + li.toString() + ", ")));
          return Future.succeededFuture();
        });
      }
      return Future.failedFuture("No ServiceEndpoint");
    });
  }

  // TODO: handle failed POST/PUT
  public Future<HttpResponse<Buffer>> postReport(String tenantId, CounterReport report) {
    return getReport(tenantId, report.getVendorId(), report.getReportName(), report.getYearMonth(),
        true).compose(existing -> {
          if (existing == null) { // no report found
            // POST the report
            return sendReportRequest(HttpMethod.POST, tenantId, report);
          } else {
            if (report.getFailedAttempts() != null) {
              // FIXME: check null
              report.setFailedAttempts(existing.getFailedAttempts() + 1);
            }
            report.setId(existing.getId());
            return sendReportRequest(HttpMethod.PUT, tenantId, report);
          }
        });

  }

  public Future<HttpResponse<Buffer>> sendReportRequest(HttpMethod method, String tenantId,
      CounterReport report) {
    final String logprefix = "Tenant: " + tenantId + ", ";
    String urlTmp = okapiUrl + reportsPath;
    if (!method.equals(HttpMethod.POST) && !method.equals(HttpMethod.PUT)) {
      return Future.failedFuture("HttpMethod not supported");
    } else if (method.equals(HttpMethod.PUT)) {
      urlTmp += "/" + report.getId();
    }
    final String url = urlTmp;

    final Future<HttpResponse<Buffer>> future = Future.future();

    LOG.info(logprefix + "posting report with id " + report.getId());

    WebClient client = WebClient.create(vertx);
    client.requestAbs(method, url)
        .putHeader("x-okapi-tenant", tenantId)
        .putHeader("accept", "application/json,text/plain")
        .sendJsonObject(JsonObject.mapFrom(report), ar -> {
          if (ar.succeeded()) {
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

  /**
   * completes with the found report or null if none is found fails otherwise
   */
  public Future<CounterReport> getReport(String tenantId, String vendorId, String reportName,
      String month, boolean tiny) {
    WebClient client = WebClient.create(vertx);
    Future<CounterReport> future = Future.future();
    String queryStr = String.format("(vendorId=%s AND yearMonth=%s AND reportName=%s)", vendorId,
        month, reportName);
    client.getAbs(okapiUrl + reportsPath)
        .putHeader("x-okapi-tenant", tenantId)
        .putHeader("accept", "application/json")
        .setQueryParam("query", queryStr)
        .setQueryParam("tiny", String.valueOf(tiny))
        .send(handler -> {
          if (handler.succeeded()) {
            if (handler.result().statusCode() == 200) {
              CounterReports collection = handler.result().bodyAsJson(CounterReports.class);
              if (collection.getCounterReports().size() == 1)
                future.complete(collection.getCounterReports().get(0));
              else
                future.complete(null);
            } else {
              future.complete(null);
            }
          } else {
            future.fail(handler.cause());
          }
        });
    return future;
  }

  public void run() {

    this.getTenants().setHandler(ar -> {
      if (ar.succeeded()) {
        List<String> tenantList = ar.result();
        tenantList.forEach(t -> {
          this.hasEnabledUsageModule(t).map(en -> {
            if (en) {
              this.getActiveProviders(t).map(providers -> {
                providers.getUsageDataProviders().forEach(p -> this.fetchAndPostReports(t, p));
                return null;
              }).setHandler(h -> {
                if (h.failed())
                  LOG.error("Tenant " + t + ": failed to get providers: " + h.cause());
              });
            } else {
              LOG.info("Tenant " + t + ": not enabled");
            }
            return Future.succeededFuture();
          }).setHandler(h -> {
            if (h.failed())
              LOG.error(h.cause());
          });
        });
      } else {
        LOG.error("Failed to get tenants: " + ar.cause());
      }
    });

  }

  private Future<Object> handleErrorFuture(String logPrefix) {
    return Future.future().setHandler(ar -> LOG.error(logPrefix + ar.cause().getMessage()));
  }

  public Future<String> getAuthToken(String tenantId, String username, String password) {
    JsonObject userCred = new JsonObject().put("username", username).put("password", password);
    WebClient client = WebClient.create(vertx);
    Future<String> future = Future.future();
    client.postAbs(okapiUrl + loginPath)
        .addQueryParam("expandPermissions", "false")
        .addQueryParam("fullPermissions", "false")
        .putHeader(XOkapiHeaders.TENANT, tenantId)
        .putHeader(HttpHeaders.CONTENT_TYPE, MediaType.JSON_UTF_8.toString())
        .putHeader(HttpHeaders.ACCEPT, MediaType.JSON_UTF_8.toString())
        .sendJson(userCred, h -> {
          if (h.succeeded()) {
            String token = h.result().getHeader(XOkapiHeaders.TOKEN);
            if (Strings.isNullOrEmpty(token))
              future.fail("No token received: " + h.result().statusCode() + " "
                  + h.result().statusMessage());
            else if (h.result()
                .bodyAsJsonObject()
                .getJsonArray("permissions.permissions", new JsonArray())
                .contains(requiredPerm))
              future.fail("Required permission not found");
            else
              future.complete(token);
          } else {
            future.fail(h.cause());
          }
        });
    return future;
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
      System.out.println("Starting with config:\n" + config().encodePrettily());
      startFuture.complete();

      // only start processing if not in test
      if (!config().getBoolean("testing", false)) {
        run();
      }
    }
  }
}
