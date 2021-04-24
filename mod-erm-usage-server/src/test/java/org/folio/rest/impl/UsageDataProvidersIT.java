package org.folio.rest.impl;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.equalTo;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import io.restassured.parsing.Parser;
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
import org.folio.postgres.testing.PostgresTesterContainer;
import org.folio.rest.RestVerticle;
import org.folio.rest.client.TenantClient;
import org.folio.rest.jaxrs.model.AggregatorSetting;
import org.folio.rest.jaxrs.model.SushiCredentials;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.jaxrs.model.UsageDataProvider;
import org.folio.rest.jaxrs.model.UsageDataProvider.HasFailedReport;
import org.folio.rest.jaxrs.model.UsageDataProviders;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.NetworkUtils;
import org.folio.rest.util.ModuleVersion;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class UsageDataProvidersIT {

  private static final String APPLICATION_JSON = "application/json";
  private static final String BASE_URI = "/usage-data-providers";
  private static final String AGGREGATOR_PATH = "/aggregator-settings";
  private static final String TENANT = "diku";
  private static Vertx vertx;
  private static UsageDataProvider udprovider;
  private static UsageDataProvider udprovider2;
  private static UsageDataProvider udproviderChanged;
  private static UsageDataProvider udproviderInvalid;
  private static AggregatorSetting aggregator;

  @Rule public Timeout timeout = Timeout.seconds(10);

  @Rule public WireMockRule wireMockRule = new WireMockRule(options().dynamicPort());

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
      udproviderChanged =
          Json.decodeValue(udproviderStr, UsageDataProvider.class)
              .withLabel("CHANGED")
              .withSushiCredentials(
                  sushiCredentials.withRequestorMail("CHANGED@ub.uni-leipzig.de"));
      udproviderInvalid = Json.decodeValue(udproviderStr, UsageDataProvider.class).withLabel(null);
    } catch (IOException ex) {
      context.fail(ex);
    }

    PostgresClient.setPostgresTester(new PostgresTesterContainer());
    Async async = context.async();
    int port = NetworkUtils.nextFreePort();

    RestAssured.reset();
    RestAssured.baseURI = "http://localhost";
    RestAssured.port = port;
    RestAssured.defaultParser = Parser.JSON;

    TenantClient tenantClient = new TenantClient("http://localhost:" + port, TENANT, TENANT);
    DeploymentOptions options =
        new DeploymentOptions().setConfig(new JsonObject().put("http.port", port));

    vertx.deployVerticle(
        RestVerticle.class.getName(),
        options,
        res -> {
          try {
            tenantClient.postTenant(
                new TenantAttributes().withModuleTo(ModuleVersion.getModuleVersion()),
                res2 -> async.complete());
          } catch (Exception e) {
            context.fail(e);
          }
        });
  }

  @AfterClass
  public static void teardown(TestContext context) {
    RestAssured.reset();
    Async async = context.async();
    vertx.close(
        context.asyncAssertSuccess(
            res -> {
              PostgresClient.stopPostgresTester();
              async.complete();
            }));
  }

  @Test
  public void checkThatWeCanAddAProviderWithAggregatorSettings() {
    // POST aggregator
    given()
        .body(Json.encode(aggregator))
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", APPLICATION_JSON)
        .post(AGGREGATOR_PATH)
        .then()
        .statusCode(201);

    // POST provider
    given()
        .body(Json.encode(udprovider2))
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", APPLICATION_JSON)
        .post(BASE_URI)
        .then()
        .statusCode(201);

    // GET provider && check if aggregator name got resolved
    given()
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", APPLICATION_JSON)
        .get(BASE_URI + "/" + udprovider2.getId())
        .then()
        .statusCode(200)
        .body("id", equalTo(udprovider2.getId()))
        .body("label", equalTo(udprovider2.getLabel()))
        .body("harvestingConfig.aggregator.name", equalTo(aggregator.getLabel()));

    // DELETE provider
    given()
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", "text/plain")
        .delete(BASE_URI + "/" + udprovider2.getId())
        .then()
        .statusCode(204);

    // DELETE aggregator
    given()
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", "text/plain")
        .delete(AGGREGATOR_PATH + "/" + aggregator.getId())
        .then()
        .statusCode(204);
  }

  @Test
  public void checkThatWeCanAddGetPutAndDeleteUsageDataProviders() {
    String mockedOkapiUrl = "http://localhost:" + wireMockRule.port();

    // POST provider without aggregator
    given()
        .body(Json.encode(udprovider))
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
    UsageDataProvider udproviderResult =
        given()
            .header("X-Okapi-Tenant", TENANT)
            .header("content-type", APPLICATION_JSON)
            .header("accept", APPLICATION_JSON)
            .when()
            .get(BASE_URI + "/" + udprovider.getId())
            .then()
            .contentType(ContentType.JSON)
            .statusCode(200)
            .extract()
            .as(UsageDataProvider.class);
    assertThat(udproviderResult)
        .usingRecursiveComparison()
        .ignoringFields("metadata")
        .isEqualTo(udprovider);

    // PUT
    given()
        .body(Json.encode(udproviderChanged))
        .header("X-Okapi-Tenant", TENANT)
        .header("x-okapi-url", mockedOkapiUrl)
        .header("content-type", APPLICATION_JSON)
        .header("accept", "text/plain")
        .request()
        .put(BASE_URI + "/" + udproviderChanged.getId())
        .then()
        .statusCode(204);

    // GET again
    UsageDataProvider udproviderChangedResult =
        given()
            .header("X-Okapi-Tenant", TENANT)
            .header("content-type", APPLICATION_JSON)
            .header("accept", APPLICATION_JSON)
            .request()
            .get(BASE_URI + "/" + udproviderChanged.getId())
            .then()
            .statusCode(200)
            .extract()
            .as(UsageDataProvider.class);
    assertThat(udproviderChangedResult)
        .usingRecursiveComparison()
        .ignoringFields("metadata")
        .isEqualTo(udproviderChanged);

    // DELETE
    given()
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", "text/plain")
        .when()
        .delete(BASE_URI + "/" + udproviderChanged.getId())
        .then()
        .statusCode(204);

    // GET again
    given()
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", APPLICATION_JSON)
        .when()
        .get(BASE_URI + "/" + udproviderChanged.getId())
        .then()
        .statusCode(404);
  }

  @Test
  public void checkThatWeCanSearchByCQL() {
    String mockedOkapiUrl = "http://localhost:" + wireMockRule.port();

    // POST provider without aggregator
    UsageDataProvider udp =
        given()
            .body(Json.encode(udprovider))
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
    UsageDataProviders queryResult =
        given()
            .header("X-Okapi-Tenant", TENANT)
            .header("content-type", APPLICATION_JSON)
            .header("accept", APPLICATION_JSON)
            .when()
            .get(BASE_URI + cqlLabel)
            .thenReturn()
            .as(UsageDataProviders.class);
    assertThat(queryResult.getUsageDataProviders().size()).isEqualTo(1);
    assertThat(queryResult.getUsageDataProviders().get(0).getLabel()).isEqualTo(udp.getLabel());
    assertThat(queryResult.getUsageDataProviders().get(0).getId()).isEqualTo(udp.getId());

    // DELETE
    given()
        .header("X-Okapi-Tenant", TENANT)
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

  @Test
  public void checkThatDefaultValueForHasFailedReportsIsNo() {
    UsageDataProvider udp =
        given()
            .body(udprovider)
            .header("X-Okapi-Tenant", TENANT)
            .header("content-type", APPLICATION_JSON)
            .header("accept", APPLICATION_JSON)
            .request()
            .post(BASE_URI)
            .then()
            .statusCode(201)
            .extract()
            .as(UsageDataProvider.class);

    assertThat(udp.getHasFailedReport()).isEqualTo(HasFailedReport.NO);

    given()
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", "text/plain")
        .when()
        .delete(BASE_URI + "/" + udprovider.getId())
        .then()
        .statusCode(204);
  }
}
