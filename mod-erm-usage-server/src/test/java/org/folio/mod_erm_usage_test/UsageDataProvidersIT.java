package org.folio.mod_erm_usage_test;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.givenThat;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static com.jayway.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.equalTo;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.jayway.restassured.RestAssured;
import com.jayway.restassured.http.ContentType;
import com.jayway.restassured.parsing.Parser;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.folio.rest.RestVerticle;
import org.folio.rest.client.TenantClient;
import org.folio.rest.jaxrs.model.AggregatorSetting;
import org.folio.rest.jaxrs.model.SushiCredentials;
import org.folio.rest.jaxrs.model.UsageDataProvider;
import org.folio.rest.jaxrs.model.UsageDataProviders;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.NetworkUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class UsageDataProvidersIT {

  public static final String APPLICATION_JSON = "application/json";
  public static final String BASE_URI = "/usage-data-providers";
  private static final String VENDOR_PATH = "/vendor";
  private static final String TENANT = "diku";
  private static final String AGGREGATOR_SETTINGS_PATH = "/aggregator-settings/";
  private static Vertx vertx;
  private static UsageDataProvider udprovider;
  private static UsageDataProvider udprovider2;
  private static UsageDataProvider udproviderChanged;
  private static UsageDataProvider udproviderInvalid;
  private static AggregatorSetting aggregator;

  @Rule
  public Timeout timeout = Timeout.seconds(10);

  @Rule
  public WireMockRule wireMockRule = new WireMockRule(options().dynamicPort());

  @BeforeClass
  public static void setUp(TestContext context) {
    vertx = Vertx.vertx();

    // setup sample data
    try {
      String udprovider2Str =
        new String(Files.readAllBytes(Paths.get("../ramls/examples/udproviders2.sample")));
      udprovider2 = Json.decodeValue(udprovider2Str, UsageDataProvider.class);
      SushiCredentials sushiCredentials = udprovider2.getSushiCredentials();

      String aggregatorStr =
        new String(Files.readAllBytes(Paths.get("../ramls/examples/aggregatorsettings.sample")));
      aggregator = Json.decodeValue(aggregatorStr, AggregatorSetting.class);
      String udproviderStr =
        new String(Files.readAllBytes(Paths.get("../ramls/examples/udproviders.sample")));
      udprovider = Json.decodeValue(udproviderStr, UsageDataProvider.class);
      udproviderChanged = Json.decodeValue(udproviderStr, UsageDataProvider.class)
        .withLabel("CHANGED")
        .withSushiCredentials(sushiCredentials.withRequestorMail("CHANGED@ub.uni-leipzig.de"));
      udproviderInvalid = Json.decodeValue(udproviderStr, UsageDataProvider.class)
        .withLabel(null);
    } catch (IOException ex) {
      context.fail(ex);
    }

    try {
      PostgresClient.setIsEmbedded(true);
      PostgresClient instance = PostgresClient.getInstance(vertx);
      instance.startEmbeddedPostgres();
    } catch (Exception e) {
      e.printStackTrace();
      context.fail(e);
      return;
    }
    Async async = context.async();
    int port = NetworkUtils.nextFreePort();

    RestAssured.reset();
    RestAssured.baseURI = "http://localhost";
    RestAssured.port = port;
    RestAssured.defaultParser = Parser.JSON;

    TenantClient tenantClient = new TenantClient("http://localhost:" + port, TENANT, TENANT);
    DeploymentOptions options =
      new DeploymentOptions().setConfig(new JsonObject().put("http.port", port)).setWorker(true);

    vertx.deployVerticle(RestVerticle.class.getName(), options, res -> {
      try {
        tenantClient.postTenant(null, res2 -> {
          async.complete();
        });
      } catch (Exception e) {
        context.fail(e);
      }
    });
  }

  @AfterClass
  public static void teardown(TestContext context) {
    RestAssured.reset();
    Async async = context.async();
    vertx.close(context.asyncAssertSuccess(res -> {
      PostgresClient.stopEmbeddedPostgres();
      async.complete();
    }));
  }

  @Test
  public void checkThatWeCanAddAProviderWithAggregatorSettings() {
    mockVendorFound();
    String mockedOkapiUrl = "http://localhost:" + wireMockRule.port();

    // POST provider with aggregator, should fail
    mockAggregatorNotFound();
    given().body(Json.encode(udprovider2))
      .header("X-Okapi-Tenant", TENANT)
      .header("x-okapi-url", mockedOkapiUrl)
      .header("content-type", APPLICATION_JSON)
      .header("accept", APPLICATION_JSON)
      .post(BASE_URI)
      .then()
      .statusCode(500);

    // POST provider then with mocked aggregator found
    mockAggregatorFound();
    given().body(Json.encode(udprovider2))
      .header("X-Okapi-Tenant", TENANT)
      .header("x-okapi-url", mockedOkapiUrl)
      .header("content-type", APPLICATION_JSON)
      .header("accept", APPLICATION_JSON)
      .post(BASE_URI)
      .then()
      .statusCode(201);

    // GET provider
    given().header("X-Okapi-Tenant", TENANT)
      .header("content-type", APPLICATION_JSON)
      .header("accept", APPLICATION_JSON)
      .get(BASE_URI + "/" + udprovider2.getId())
      .then()
      .statusCode(200)
      .body("id", equalTo(udprovider2.getId()))
      .body("label", equalTo(udprovider2.getLabel()))
      .body("harvestingConfig.aggregator.name", equalTo(aggregator.getLabel()));

    // DELETE provider
    given().header("X-Okapi-Tenant", TENANT)
      .header("content-type", APPLICATION_JSON)
      .header("accept", "text/plain")
      .delete(BASE_URI + "/" + udprovider2.getId())
      .then()
      .statusCode(204);
  }

  @Test
  public void checkThatWeCanAddGetPutAndDeleteUsageDataProviders() {

    mockVendorFound();
    String mockedOkapiUrl = "http://localhost:" + wireMockRule.port();

    // POST provider without aggregator
    given().body(Json.encode(udprovider))
      .header("X-Okapi-Tenant", TENANT)
      .header("x-okapi-url", mockedOkapiUrl)
      .header("content-type", APPLICATION_JSON)
      .header("accept", APPLICATION_JSON)
      .request()
      .post(BASE_URI)
      .then()
      .statusCode(201)
      .body("id", equalTo(udprovider.getId()))
      .body("label", equalTo(udprovider.getLabel()));

    // GET
    given().header("X-Okapi-Tenant", TENANT)
      .header("content-type", APPLICATION_JSON)
      .header("accept", APPLICATION_JSON)
      .when()
      .get(BASE_URI + "/" + udprovider.getId())
      .then()
      .contentType(ContentType.JSON)
      .statusCode(200)
      .body("id", equalTo(udprovider.getId()))
      .body("label", equalTo(udprovider.getLabel()));

    // PUT
    given().body(Json.encode(udproviderChanged))
      .header("X-Okapi-Tenant", TENANT)
      .header("x-okapi-url", mockedOkapiUrl)
      .header("content-type", APPLICATION_JSON)
      .header("accept", "text/plain")
      .request()
      .put(BASE_URI + "/" + udproviderChanged.getId())
      .then()
      .statusCode(204);

    // GET again
    given().header("X-Okapi-Tenant", TENANT)
      .header("content-type", APPLICATION_JSON)
      .header("accept", APPLICATION_JSON)
      .request()
      .get(BASE_URI + "/" + udproviderChanged.getId())
      .then()
      .statusCode(200)
      .body("id", equalTo(udproviderChanged.getId()))
      .body("label", equalTo("CHANGED"))
      .body("sushiCredentials.requestorMail", equalTo("CHANGED@ub.uni-leipzig.de"));

    // DELETE
    given().header("X-Okapi-Tenant", TENANT)
      .header("content-type", APPLICATION_JSON)
      .header("accept", "text/plain")
      .when()
      .delete(BASE_URI + "/" + udproviderChanged.getId())
      .then()
      .statusCode(204);

    // GET again
    given().header("X-Okapi-Tenant", TENANT)
      .header("content-type", APPLICATION_JSON)
      .header("accept", APPLICATION_JSON)
      .when()
      .get(BASE_URI + "/" + udproviderChanged.getId())
      .then()
      .statusCode(404);
  }

  @Test
  public void checkThatWeCanSearchByCQL() {

    mockVendorFound();
    String mockedOkapiUrl = "http://localhost:" + wireMockRule.port();

    // POST provider without aggregator
    UsageDataProvider udp = given().body(Json.encode(udprovider))
      .header("X-Okapi-Tenant", TENANT)
      .header("X-Okapi-Url", mockedOkapiUrl)
      .header("content-type", APPLICATION_JSON)
      .header("accept", APPLICATION_JSON)
      .request()
      .post(BASE_URI)
      .thenReturn()
      .as(UsageDataProvider.class);
    assertThat(udp.getLabel()).isEqualTo(udprovider.getLabel());
    assertThat(udp.getId()).isNotEmpty();

    String cqlLabel = "?query=(label=\"" + udprovider.getLabel() + "\")";
    UsageDataProviders queryResult = given().header("X-Okapi-Tenant", TENANT)
      .header("content-type", APPLICATION_JSON)
      .header("accept", APPLICATION_JSON)
      .when()
      .get(BASE_URI + cqlLabel)
      .thenReturn()
      .as(UsageDataProviders.class);
    assertThat(queryResult.getUsageDataProviders().size()).isEqualTo(1);
    assertThat(queryResult.getUsageDataProviders().get(0).getLabel())
      .isEqualTo(udp.getLabel());
    assertThat(queryResult.getUsageDataProviders().get(0).getId())
      .isEqualTo(udp.getId());

    // DELETE
    given().header("X-Okapi-Tenant", TENANT)
      .header("content-type", APPLICATION_JSON)
      .header("accept", "text/plain")
      .when()
      .delete(BASE_URI + "/" + udprovider.getId())
      .then()
      .statusCode(204);
  }

  @Test
  public void checkThatInvalidUsageDataProviderIsNotPosted() {
    given()
      .body(udproviderInvalid)
      .header("X-Okapi-Tenant", TENANT)
      .header("content-type", APPLICATION_JSON)
      .header("accept", APPLICATION_JSON)
      .request()
      .post(BASE_URI)
      .then()
      .statusCode(422);
  }

  private void mockVendorFound() {
    String vendorId = udprovider.getVendor().getId();
    String vendorUrl = VENDOR_PATH + "/" + vendorId;
    givenThat(get(urlPathEqualTo(vendorUrl))
      .willReturn(aResponse()
        .withHeader("Content-type", "application/json")
        .withBodyFile("vendor.json")
        .withStatus(200)));
  }

  private void mockAggregatorFound() {
    String aggregatorId = aggregator.getId();
    String aggregatorUrl = AGGREGATOR_SETTINGS_PATH + aggregatorId;
    givenThat(get(urlPathEqualTo(aggregatorUrl))
      .willReturn(aResponse()
        .withHeader("Content-type", "application/json")
        .withBody(Json.encode(aggregator))
        .withStatus(200)));
  }

  private void mockAggregatorNotFound() {
    String aggregatorId = aggregator.getId();
    String aggregatorUrl = AGGREGATOR_SETTINGS_PATH + aggregatorId;
    givenThat(get(urlPathEqualTo(aggregatorUrl))
      .willReturn(aResponse()
        .withStatus(404)));
  }

}
