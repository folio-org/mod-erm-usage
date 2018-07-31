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

  @Test
  public void getTenantsBodyValid(TestContext context) {
    final String path = "/getTenantsBodyValid";
    stubFor(get(urlEqualTo(path)).willReturn(aResponse().withBodyFile("TenantsResponse200.json")));

    Async async = context.async();
    new Harvester().getTenants(wireMockRule.url(path)).setHandler(ar -> {
      context.assertTrue(ar.succeeded());
      context.assertEquals(2, ar.result().size());
      context.assertEquals("diku", ar.result().get(0));
      async.complete();
    });
  }

  @Test
  public void getTenantsBodyInvalid(TestContext context) {
    final String path = "/getTenantsBodyInvalid";
    stubFor(get(urlEqualTo(path)).willReturn(aResponse().withBody("{ }")));

    Async async = context.async();
    new Harvester().getTenants(wireMockRule.url(path)).setHandler(ar -> {
      context.assertTrue(ar.failed());
      context.assertTrue(ar.cause().getMessage().contains("Did not receive a JsonArray"));
      async.complete();
    });
  }

  @Test
  public void getTenantsBodyEmpty(TestContext context) {
    final String path = "/getTenantsBodyEmpty";
    stubFor(get(urlEqualTo(path)).willReturn(aResponse().withBody("[ ]")));

    Async async = context.async();
    new Harvester().getTenants(wireMockRule.url(path)).setHandler(ar -> {
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
    new Harvester().getTenants(wireMockRule.url(path)).setHandler(ar -> {
      context.assertTrue(ar.failed());
      context.assertTrue(ar.cause().getMessage().contains("Received status code"));
      context.assertTrue(ar.cause().getMessage().contains("404"));
      async.complete();
    });
  }

  @Test
  public void getTenantsNoService(TestContext context) {
    final String path = "/getTenantsNoService";
    int port = wireMockRule.port();
    wireMockRule.stop();

    Async async = context.async();
    new Harvester().getTenants("http://localhost:" + port + path).setHandler(ar -> {
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
    new Harvester().getTenants(wireMockRule.url(path)).setHandler(ar -> {
      context.assertTrue(ar.failed());
      async.complete();
    });
  }

  @Test
  public void hasUsageModuleYes(TestContext context) {
    final String path = "/_/proxy/tenants/diku/modules/mod-erm-usage-0.0.1";
    stubFor(get(urlEqualTo(path))
        .willReturn(aResponse().withBody("{ \"id\" : \"mod-erm-usage-0.0.1\" }")));

    Async async = context.async();
    Harvester h = new Harvester(wireMockRule.url(""), "_/proxy/tenants", "mod-erm-usage-0.0.1");
    h.hasUsageModule("diku").setHandler(ar -> {
      context.assertTrue(ar.succeeded(), "Future failed.");
      async.complete();
    });
  }

  @Test
  public void hasUsageModuleNo(TestContext context) {
    final String path = "/_/proxy/tenants/diku/modules/mod-erm-usage-0.0.1";
    stubFor(get(urlEqualTo(path)).willReturn(aResponse().withBody("mod-erm-usage-0.0.1")));

    Async async = context.async();
    Harvester h = new Harvester(wireMockRule.url(""), "_/proxy/tenants", "mod-erm-usage-0.0.1");
    h.hasUsageModule("diku").setHandler(ar -> {
      context.assertTrue(ar.failed());
      context.assertTrue(ar.cause().getMessage().contains("JsonObject"));
      async.complete();
    });
  }

  @Test
  public void hasUsageModuleResponseInvalid(TestContext context) {
    final String path = "/_/proxy/tenants/diku/modules/mod-erm-usage-0.0.1";
    stubFor(get(urlEqualTo(path)).willReturn(aResponse().withStatus(404)));

    Async async = context.async();
    Harvester h = new Harvester(wireMockRule.url(""), "_/proxy/tenants", "mod-erm-usage-0.0.1");
    h.hasUsageModule("diku").setHandler(ar -> {
      context.assertTrue(ar.failed());
      context.assertTrue(ar.cause().getMessage().contains("status code 404"));
      async.complete();
    });
  }

  @Test
  public void hasUsageModuleNoService(TestContext context) {
    int port = wireMockRule.port();
    wireMockRule.stop();

    Async async = context.async();
    Harvester h =
        new Harvester("http://localhost:" + port + "/", "_/proxy/tenants", "mod-erm-usage-0.0.1");
    h.hasUsageModule("diku").setHandler(ar -> {
      context.assertTrue(ar.failed());
      async.complete();
    });
  }
  
  @Test
  public void getProviders(TestContext context) {
    final String path = "/usage-data-providers";
    stubFor(get(urlPathMatching(path)).willReturn(aResponse().withBodyFile("usage-data-providers.json")));

    Async async = context.async();
    new Harvester().getProviders(wireMockRule.url(path), "diku").setHandler(ar -> {
      context.assertTrue(ar.succeeded());
      context.assertEquals(3, ar.result().getTotalRecords());
      async.complete();
    });
  }
}
