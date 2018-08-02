package olf.erm.usage.harvester;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import java.io.IOException;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.folio.rest.jaxrs.model.UsageDataProvider;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.http.Fault;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.google.gson.JsonSyntaxException;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class HarvesterTest {

  private static final Logger LOG = Logger.getLogger(HarvesterTest.class);

  @Rule
  public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().dynamicPort());
  @Rule
  public Timeout timeoutRule = Timeout.seconds(5);

  private static final Harvester harvester = new Harvester("", "/_/proxy/tenants",
      "/counter-reports", "/usage-data-providers", "/aggregator-settings");
  private static final String tenantId = "diku";

  @Before
  public void setup() {
    harvester.setOkapiUrl(StringUtils.removeEnd(wireMockRule.url(""), "/"));
  }

  @Test
  public void getTenantsBodyValid(TestContext context) {
    stubFor(get(urlEqualTo(harvester.getTenantsPath()))
        .willReturn(aResponse().withBodyFile("TenantsResponse200.json")));

    Async async = context.async();
    harvester.getTenants().setHandler(ar -> {
      context.assertTrue(ar.succeeded());
      context.assertEquals(2, ar.result().size());
      context.assertEquals(tenantId, ar.result().get(0));
      async.complete();
    });
  }

  @Test
  public void getTenantsBodyInvalid(TestContext context) {
    stubFor(get(urlEqualTo(harvester.getTenantsPath())).willReturn(aResponse().withBody("{ }")));

    Async async = context.async();
    harvester.getTenants().setHandler(ar -> {
      context.assertTrue(ar.failed());
      context.assertTrue(ar.cause().getMessage().contains("Error decoding"));
      async.complete();
    });
  }

  @Test
  public void getTenantsBodyEmpty(TestContext context) {
    stubFor(get(urlEqualTo(harvester.getTenantsPath())).willReturn(aResponse().withBody("[ ]")));

    Async async = context.async();
    harvester.getTenants().setHandler(ar -> {
      context.assertTrue(ar.failed());
      context.assertTrue(ar.cause().getMessage().equals("No tenants found."));
      async.complete();
    });
  }

  @Test
  public void getTenantsResponseInvalid(TestContext context) {
    stubFor(get(urlEqualTo(harvester.getTenantsPath())).willReturn(aResponse().withStatus(404)));

    Async async = context.async();
    harvester.getTenants().setHandler(ar -> {
      context.assertTrue(ar.failed());
      context.assertTrue(ar.cause().getMessage().contains("404"));
      async.complete();
    });
  }

  @Test
  public void getTenantsNoService(TestContext context) {
    wireMockRule.stop();

    Async async = context.async();
    harvester.getTenants().setHandler(ar -> {
      context.assertTrue(ar.failed());
      LOG.error(ar.cause());
      async.complete();
    });
  }

  @Test
  public void getTenantsWithFault(TestContext context) {
    stubFor(get(urlEqualTo(harvester.getTenantsPath()))
        .willReturn(aResponse().withFault(Fault.CONNECTION_RESET_BY_PEER)));

    Async async = context.async();
    harvester.getTenants().setHandler(ar -> {
      context.assertTrue(ar.failed());
      async.complete();
    });
  }

  @Test
  public void hasEnabledModuleYes(TestContext context) {
    stubFor(get(urlEqualTo(
        harvester.getTenantsPath() + "/" + tenantId + "/modules/" + harvester.getModuleId()))
            .willReturn(aResponse().withBody("{ \"id\" : \"" + harvester.getModuleId() + "\" }")));

    Async async = context.async();
    harvester.hasEnabledModule(tenantId).setHandler(ar -> {
      context.assertTrue(ar.succeeded());
      async.complete();
    });
  }

  @Test
  public void hasEnabledModuleNo(TestContext context) {
    stubFor(get(urlEqualTo(
        harvester.getTenantsPath() + "/" + tenantId + "/modules/" + harvester.getModuleId()))
            .willReturn(aResponse().withBody(harvester.getModuleId())));

    Async async = context.async();
    harvester.hasEnabledModule(tenantId).setHandler(ar -> {
      context.assertTrue(ar.failed());
      context.assertTrue(ar.cause().getMessage().contains("Error decoding"));
      async.complete();
    });
  }

  @Test
  public void hasEnabledModuleResponseInvalid(TestContext context) {
    stubFor(get(urlEqualTo(
        harvester.getTenantsPath() + "/" + tenantId + "/modules/" + harvester.getModuleId()))
            .willReturn(aResponse().withStatus(404)));

    Async async = context.async();
    harvester.hasEnabledModule(tenantId).setHandler(ar -> {
      context.assertTrue(ar.failed());
      context.assertTrue(ar.cause().getMessage().contains("404"));
      async.complete();
    });
  }

  @Test
  public void hasEnabledModuleNoService(TestContext context) {
    wireMockRule.stop();

    Async async = context.async();
    harvester.hasEnabledModule(tenantId).setHandler(ar -> {
      context.assertTrue(ar.failed());
      async.complete();
    });
  }

  @Test
  public void getProvidersBodyValid(TestContext context) {
    stubFor(get(urlPathMatching(harvester.getProviderPath()))
        .willReturn(aResponse().withBodyFile("usage-data-providers.json")));

    Async async = context.async();
    harvester.getProviders(tenantId).setHandler(ar -> {
      context.assertTrue(ar.succeeded());
      context.assertEquals(3, ar.result().getTotalRecords());
      async.complete();
    });
  }

  @Test
  public void getProvidersBodyInvalid(TestContext context) {
    stubFor(get(urlPathMatching(harvester.getProviderPath())).willReturn(aResponse().withBody("")));

    Async async = context.async();
    harvester.getProviders(tenantId).setHandler(ar -> {
      context.assertTrue(ar.failed());
      context.assertTrue(ar.cause().getMessage().contains("Error decoding"));
      async.complete();
    });
  }

  @Test
  public void getProvidersResponseInvalid(TestContext context) {
    stubFor(
        get(urlPathMatching(harvester.getProviderPath())).willReturn(aResponse().withStatus(404)));

    Async async = context.async();
    harvester.getProviders(tenantId).setHandler(ar -> {
      context.assertTrue(ar.failed());
      context.assertTrue(ar.cause().getMessage().contains("404"));
      async.complete();
    });
  }

  @Test
  public void getProvidersNoService(TestContext context) {
    wireMockRule.stop();

    Async async = context.async();
    harvester.getProviders(tenantId).setHandler(ar -> {
      context.assertTrue(ar.failed());
      LOG.error(ar.cause());
      async.complete();
    });
  }

  @Test
  public void getAggregatorSettingsBodyValid(TestContext context)
      throws JsonSyntaxException, IOException {
    final UsageDataProvider provider = new ObjectMapper().readValue(Resources
        .toString(Resources.getResource("__files/usage-data-provider.json"), Charsets.UTF_8),
        UsageDataProvider.class);
    stubFor(get(urlEqualTo(harvester.getAggregatorPath() + "/" + provider.getAggregator().getId()))
        .willReturn(aResponse().withBodyFile("aggregator-setting.json")));

    Async async = context.async();
    harvester.getAggregatorSetting(tenantId, provider).setHandler(ar -> {
      context.assertTrue(ar.succeeded());
      context.assertTrue("Nationaler Statistikserver".equals(ar.result().getLabel()));
      async.complete();
    });
  }

  @Test
  public void getAggregatorSettingsBodyValidNoAggregator(TestContext context)
      throws JsonSyntaxException, IOException {
    final UsageDataProvider provider1 = new ObjectMapper().readValue(Resources
        .toString(Resources.getResource("__files/usage-data-provider.json"), Charsets.UTF_8),
        UsageDataProvider.class);
    final UsageDataProvider provider2 = new ObjectMapper().readValue(Resources
        .toString(Resources.getResource("__files/usage-data-provider.json"), Charsets.UTF_8),
        UsageDataProvider.class);

    provider1.setAggregator(null);
    Async async = context.async();
    harvester.getAggregatorSetting(tenantId, provider1).setHandler(ar -> {
      context.assertTrue(ar.failed());
      context.assertTrue(ar.result() == null);
      context.assertTrue(ar.cause().getMessage().contains("no aggregator found"));
      async.complete();
    });

    provider2.getAggregator().setId(null);
    Async async2 = context.async();
    harvester.getAggregatorSetting(tenantId, provider2).setHandler(ar -> {
      context.assertTrue(ar.failed());
      context.assertTrue(ar.result() == null);
      context.assertTrue(ar.cause().getMessage().contains("no aggregator found"));
      async2.complete();
    });
  }

  @Test
  public void getAggregatorSettingsBodyInvalid(TestContext context)
      throws JsonSyntaxException, IOException {
    final UsageDataProvider provider = new ObjectMapper().readValue(Resources
        .toString(Resources.getResource("__files/usage-data-provider.json"), Charsets.UTF_8),
        UsageDataProvider.class);
    stubFor(get(urlEqualTo(harvester.getAggregatorPath() + "/" + provider.getAggregator().getId()))
        .willReturn(aResponse().withBody("garbage")));

    Async async = context.async();
    harvester.getAggregatorSetting(tenantId, provider).setHandler(ar -> {
      context.assertTrue(ar.failed());
      context.assertTrue(ar.result() == null);
      context.assertTrue(ar.cause().getMessage().contains("Error decoding"));
      async.complete();
    });
  }

  @Test
  public void getAggregatorSettingsResponseInvalid(TestContext context)
      throws JsonSyntaxException, IOException {
    final UsageDataProvider provider = new ObjectMapper().readValue(Resources
        .toString(Resources.getResource("__files/usage-data-provider.json"), Charsets.UTF_8),
        UsageDataProvider.class);
    stubFor(get(urlEqualTo(harvester.getAggregatorPath() + "/" + provider.getAggregator().getId()))
        .willReturn(
            aResponse().withBody("Aggregator settingObject does not exist").withStatus(404)));

    Async async = context.async();
    harvester.getAggregatorSetting(tenantId, provider).setHandler(ar -> {
      context.assertTrue(ar.failed());
      context.assertTrue(ar.result() == null);
      context.assertTrue(ar.cause().getMessage().contains("404"));
      async.complete();
    });
  }

  @Test
  public void getAggregatorSettingsNoService(TestContext context)
      throws JsonSyntaxException, IOException {
    final UsageDataProvider provider = new ObjectMapper().readValue(Resources
        .toString(Resources.getResource("__files/usage-data-provider.json"), Charsets.UTF_8),
        UsageDataProvider.class);
    wireMockRule.stop();

    Async async = context.async();
    harvester.getAggregatorSetting(tenantId, provider).setHandler(ar -> {
      context.assertTrue(ar.failed());
      async.complete();
    });
  }
}
