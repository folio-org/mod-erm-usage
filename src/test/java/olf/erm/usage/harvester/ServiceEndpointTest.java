package olf.erm.usage.harvester;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import org.folio.rest.jaxrs.model.AggregatorSetting;
import org.folio.rest.jaxrs.model.UsageDataProvider;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.google.common.io.Resources;
import com.google.common.net.HttpHeaders;
import io.vertx.core.Future;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import olf.erm.usage.harvester.endpoints.ServiceEndpoint;

@RunWith(VertxUnitRunner.class)
public class ServiceEndpointTest {

  @Rule
  public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().dynamicPort());
  @Rule
  public Timeout timeoutRule = Timeout.seconds(5);

  @Test
  public void fetchSingleReportWithAggregator(TestContext context)
      throws JsonParseException, JsonMappingException, IOException {
    final UsageDataProvider provider = new ObjectMapper().readValue(
        new File(Resources.getResource("__files/usage-data-provider.json").getFile()),
        UsageDataProvider.class);
    final AggregatorSetting aggregator =
        new ObjectMapper()
            .readValue(new File(Resources.getResource("__files/aggregator-setting.json").getFile()),
                AggregatorSetting.class)
            .withServiceUrl(wireMockRule.url(""));
    final String endDate = "2016-03-31";
    final String beginDate = "2016-03-01";
    final ServiceEndpoint sep = ServiceEndpoint.create(provider, aggregator);
    final String url = sep.buildURL("JR1", beginDate, endDate);

    System.out.println(url);
    stubFor(get(urlEqualTo(url.replaceAll(wireMockRule.url(""), "/")))
        .willReturn(aResponse().withBodyFile("nss-report-2016-03.xml")
            .withHeader(HttpHeaders.DATE, LocalDateTime.now().toString())));

    Async async = context.async();
    Future<String> fetchSingleReport =
        ServiceEndpoint.create(provider, aggregator).fetchSingleReport("JR1", beginDate, endDate);
    fetchSingleReport.setHandler(ar -> {
      if (ar.succeeded()) {
        context.assertTrue(ar.succeeded());
        context.assertTrue(ar.result().startsWith("<!-- wiremock -->"));
        System.out.println(ar.result());
        async.complete();
      } else {
        System.out.println(ar.cause());
        context.fail();
      }
    });
  }
}
