package olf.erm.usage.harvester;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
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
    stubFor(get(urlEqualTo(path))
        .willReturn(aResponse().withBody("[ {\"id\": \"diku\"}, {\"id\": \"supermock\"}]")));

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
    stubFor(get(urlEqualTo(path))
        .willReturn(aResponse().withFault(Fault.CONNECTION_RESET_BY_PEER)));
    
    Async async = context.async();
    new Harvester().getTenants(wireMockRule.url(path)).setHandler(ar -> {
      context.assertTrue(ar.failed());
      async.complete();
    });
  }
}
