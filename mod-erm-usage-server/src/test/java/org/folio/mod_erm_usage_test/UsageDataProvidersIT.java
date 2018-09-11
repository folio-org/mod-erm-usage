package org.folio.mod_erm_usage_test;

import static com.jayway.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.equalTo;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.folio.rest.RestVerticle;
import org.folio.rest.client.TenantClient;
import org.folio.rest.jaxrs.model.AggregatorSetting;
import org.folio.rest.jaxrs.model.UdProvidersDataCollection;
import org.folio.rest.jaxrs.model.UsageDataProvider;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.NetworkUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
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

@RunWith(VertxUnitRunner.class)
public class UsageDataProvidersIT {

  public static final String APPLICATION_JSON = "application/json";
  public static final String BASE_URI = "/usage-data-providers";
  private static final String TENANT = "diku";
  private static Vertx vertx;
  private static int port;
  private static UsageDataProvider udprovider;
  private static UsageDataProvider udproviderChanged;
  private static UsageDataProvider udprovider2;
  private static AggregatorSetting aggregator;

  @Rule
  public Timeout timeout = Timeout.seconds(10);

  @BeforeClass
  public static void setUp(TestContext context) {
    vertx = Vertx.vertx();

    // setup sample data
    try {
      String udproviderStr =
          new String(Files.readAllBytes(Paths.get("../ramls/examples/udproviders2.sample")));
      udprovider = Json.decodeValue(udproviderStr, UsageDataProvider.class);
      udproviderChanged = Json.decodeValue(udproviderStr, UsageDataProvider.class)
          .withLabel("CHANGED")
          .withRequestorMail("CHANGED@ub.uni-leipzig.de");
      String aggregatorStr =
          new String(Files.readAllBytes(Paths.get("../ramls/examples/aggregatorsettings.sample")));
      aggregator = Json.decodeValue(aggregatorStr, AggregatorSetting.class);
      String udprovider2Str =
          new String(Files.readAllBytes(Paths.get("../ramls/examples/udproviders.sample")));
      udprovider2 = Json.decodeValue(udprovider2Str, UsageDataProvider.class);
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
    port = NetworkUtils.nextFreePort();

    RestAssured.baseURI = "http://localhost";
    RestAssured.port = port;
    RestAssured.defaultParser = Parser.JSON;

    TenantClient tenantClient = new TenantClient("localhost", port, "diku", "diku");
    DeploymentOptions options =
        new DeploymentOptions().setConfig(new JsonObject().put("http.port", port)).setWorker(true);

    vertx.deployVerticle(RestVerticle.class.getName(), options, res -> {
      try {
        tenantClient.post(null, res2 -> {
          async.complete();
        });
      } catch (Exception e) {
        context.fail(e);
      }
    });
  }

  @AfterClass
  public static void teardown(TestContext context) {
    Async async = context.async();
    vertx.close(context.asyncAssertSuccess(res -> {
      PostgresClient.stopEmbeddedPostgres();
      async.complete();
    }));
  }

  @Test
  public void checkThatWeCanAddAProviderWithAggregatorSettings() {
    // POST provider with aggregator, should fail
    given().body(Json.encode(udprovider2))
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", APPLICATION_JSON)
        .post(BASE_URI)
        .then()
        .statusCode(500);

    // POST aggregator first
    given().body(Json.encode(aggregator))
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", APPLICATION_JSON)
        .post("/aggregator-settings")
        .then()
        .statusCode(201);

    // POST provider then
    given().body(Json.encode(udprovider2))
        .header("X-Okapi-Tenant", TENANT)
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
        .body("label", equalTo(udprovider2.getLabel()));

    // DELETE both
    given().header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", "text/plain")
        .delete(BASE_URI + "/" + udprovider2.getId())
        .then()
        .statusCode(204);
    given().header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", "text/plain")
        .delete("/aggregator-settings/" + aggregator.getId())
        .then()
        .statusCode(204);
  }

  @Test
  public void checkThatWeCanAddGetPutAndDeleteUsageDataProviders() {
    // POST provider without aggregator
    given().body(Json.encode(udprovider))
        .header("X-Okapi-Tenant", TENANT)
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
        .body("requestorMail", equalTo("CHANGED@ub.uni-leipzig.de"));

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
  public void checkThatWeCanSearchByCQL() throws UnsupportedEncodingException {
    UsageDataProvider usageDataProvider = given()
        .body("{\n" + " \"id\": \"b4a196da-434c-475f-9c65-63f9951d1909\",\n"
            + " \"label\": \"Test Usage Data Provider\",\n" + " \"vendorId\": \"uuid-123456789\",\n"
            + " \"platformId\": \"uuid-123456789\",\n" + "\"harvestingStart\": \"2018-01\","
            + " \"harvestingStatus\": \"active\",\n" + " \"serviceType\": \"Sushi Lite\",\n"
            + " \"serviceUrl\": \"http://example.com\",\n" + " \"reportRelease\": 4,\n"
            + " \"requestedReports\": [\n" + "  \"JR1\"\n" + " ],"
            + " \"customerId\": \"12345def\",\n" + " \"requestorId\": \"1234abcd\",\n"
            + " \"apiKey\": \"678iuoi\",\n" + " \"requestorName\": \"Karla Kolumna\",\n"
            + " \"requestorMail\": \"kolumna@ub.uni-leipzig.de\"\n" + "}")
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", APPLICATION_JSON)
        .request()
        .post(BASE_URI)
        .thenReturn()
        .as(UsageDataProvider.class);
    assertThat(usageDataProvider.getLabel()).isEqualTo("Test Usage Data Provider");
    assertThat(usageDataProvider.getId()).isNotEmpty();

    String cqlLabel = "?query=(label=\"Test Usage*\")";
    UdProvidersDataCollection queryResult = given().header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", APPLICATION_JSON)
        .when()
        .get(BASE_URI + cqlLabel)
        .thenReturn()
        .as(UdProvidersDataCollection.class);
    assertThat(queryResult.getUsageDataProviders().size()).isEqualTo(1);
    assertThat(queryResult.getUsageDataProviders().get(0).getLabel())
        .isEqualTo(usageDataProvider.getLabel());
    assertThat(queryResult.getUsageDataProviders().get(0).getId())
        .isEqualTo(usageDataProvider.getId());
  }

  @Test
  public void checkThatInvalidUsageDataProviderIsNotPosted() {
    given()
        .body("{\n" + " \"id\": \"fc20602a-8ade-4b66-90d0-2853c99affa5\",\n"
            + " \"vendorId\": \"uuid-123456789\",\n" + " \"platformId\": \"uuid-123456789\",\n"
            + " \"harvestingStatus\": \"active\",\n" + " \"serviceType\": \"Sushi Lite\",\n"
            + " \"serviceUrl\": \"http://example.com\",\n" + " \"reportRelease\": 4,\n"
            + " \"requestedReports\": [\n" + "  \"JR1\"\n" + " ],"
            + " \"customerId\": \"12345def\",\n" + " \"requestorId\": \"1234abcd\",\n"
            + " \"apiKey\": \"678iuoi\",\n" + " \"requestorName\": \"Karla Kolumna\",\n"
            + " \"requestorMail\": \"kolumna@ub.uni-leipzig.de\"\n" + "}")
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", APPLICATION_JSON)
        .request()
        .post(BASE_URI)
        .then()
        .statusCode(422);
  }

}
