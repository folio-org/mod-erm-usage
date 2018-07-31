package olf.erm.usage.harvester;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import com.github.tomakehurst.wiremock.http.Fault;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class HarvesterTest {

  @Rule
  public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().dynamicPort());
  @Rule
  public Timeout timeoutRule = Timeout.seconds(5);

  private static final Harvester harvester = new Harvester();
  private static final String tenantId = "diku";
  private static final String tenantPath = "/_/proxy/tenants";
  private static final String moduleId = "mod-erm-usage-0.0.1";

  @Test
  public void getTenantsBodyValid(TestContext context) {
    final String path = "/getTenantsBodyValid";
    stubFor(get(urlEqualTo(path)).willReturn(aResponse().withBodyFile("TenantsResponse200.json")));

    Async async = context.async();
    harvester.getTenants(wireMockRule.url(path)).setHandler(ar -> {
      context.assertTrue(ar.succeeded());
      context.assertEquals(2, ar.result().size());
      context.assertEquals(tenantId, ar.result().get(0));
      async.complete();
    });
  }

  @Test
  public void getTenantsBodyInvalid(TestContext context) {
    final String path = "/getTenantsBodyInvalid";
    stubFor(get(urlEqualTo(path)).willReturn(aResponse().withBody("{ }")));

    Async async = context.async();
    harvester.getTenants(wireMockRule.url(path)).setHandler(ar -> {
      context.assertTrue(ar.failed());
      context.assertTrue(ar.cause().getMessage().contains("Error decoding"));
      async.complete();
    });
  }

  @Test
  public void getTenantsBodyEmpty(TestContext context) {
    final String path = "/getTenantsBodyEmpty";
    stubFor(get(urlEqualTo(path)).willReturn(aResponse().withBody("[ ]")));

    Async async = context.async();
    harvester.getTenants(wireMockRule.url(path)).setHandler(ar -> {
      context.assertTrue(ar.failed());
      context.assertTrue(ar.cause().getMessage().equals("No tenants found."));
      async.complete();
    });
  }

  @Test
  public void getTenantsResponseInvalid(TestContext context) {
    final String path = "/getTenantsResponseInvalid";
    stubFor(get(urlEqualTo(path)).willReturn(aResponse().withStatus(404)));

    Async async = context.async();
    harvester.getTenants(wireMockRule.url(path)).setHandler(ar -> {
      context.assertTrue(ar.failed());
      context.assertTrue(ar.cause().getMessage().contains("404"));
      async.complete();
    });
  }

  @Test
  public void getTenantsNoService(TestContext context) {
    final String wireMockUrl = wireMockRule.url("/getTenantsNoService");
    wireMockRule.stop();

    Async async = context.async();
    harvester.getTenants(wireMockUrl).setHandler(ar -> {
      context.assertTrue(ar.failed());
      async.complete();
    });
  }

  @Test
  public void getTenantsWithFault(TestContext context) {
    final String path = "/getTenantsWithFault";
    stubFor(
        get(urlEqualTo(path)).willReturn(aResponse().withFault(Fault.CONNECTION_RESET_BY_PEER)));

    Async async = context.async();
    harvester.getTenants(wireMockRule.url(path)).setHandler(ar -> {
      context.assertTrue(ar.failed());
      async.complete();
    });
  }

  @Test
  public void hasEnabledModuleYes(TestContext context) {
    final String path = tenantPath + "/" + tenantId + "/modules/" + moduleId;
    stubFor(get(urlEqualTo(path))
        .willReturn(aResponse().withBody("{ \"id\" : \"" + moduleId + "\" }")));

    Async async = context.async();
    harvester.hasEnabledModule(wireMockRule.url(tenantPath), tenantId, moduleId).setHandler(ar -> {
      context.assertTrue(ar.succeeded(), "Future failed.");
      async.complete();
    });
  }

  @Test
  public void hasEnabledModuleNo(TestContext context) {
    final String path = tenantPath + "/" + tenantId + "/modules/" + moduleId;
    stubFor(get(urlEqualTo(path)).willReturn(aResponse().withBody(moduleId)));

    Async async = context.async();
    harvester.hasEnabledModule(wireMockRule.url(tenantPath), tenantId, moduleId).setHandler(ar -> {
      context.assertTrue(ar.failed());
      context.assertTrue(ar.cause().getMessage().contains("Error decoding"));
      async.complete();
    });
  }

  @Test
  public void hasEnabledModuleResponseInvalid(TestContext context) {
    final String path = tenantPath + "/" + tenantId + "/modules/" + moduleId;
    stubFor(get(urlEqualTo(path)).willReturn(aResponse().withStatus(404)));

    Async async = context.async();
    harvester.hasEnabledModule(wireMockRule.url(tenantPath), tenantId, moduleId).setHandler(ar -> {
      context.assertTrue(ar.failed());
      context.assertTrue(ar.cause().getMessage().contains("404"));
      async.complete();
    });
  }

  @Test
  public void hasEnabledModuleNoService(TestContext context) {
    final String wireMockUrl = wireMockRule.url(tenantPath);
    wireMockRule.stop();

    Async async = context.async();
    harvester.hasEnabledModule(wireMockUrl, tenantId, moduleId).setHandler(ar -> {
      context.assertTrue(ar.failed());
      async.complete();
    });
  }

  @Test
  public void getProviders(TestContext context) {
    final String path = "/usage-data-providers";
    stubFor(get(urlPathMatching(path))
        .willReturn(aResponse().withBodyFile("usage-data-providers.json")));

    Async async = context.async();
    harvester.getProviders(wireMockRule.url(path), tenantId).setHandler(ar -> {
      context.assertTrue(ar.succeeded());
      context.assertEquals(3, ar.result().getTotalRecords());
      async.complete();
    });
  }
}
