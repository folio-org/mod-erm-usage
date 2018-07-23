package org.folio.mod_erm_usage_test;

import static com.jayway.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.equalTo;

import com.jayway.restassured.RestAssured;
import com.jayway.restassured.http.ContentType;
import com.jayway.restassured.parsing.Parser;
import io.vertx.core.Context;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.io.UnsupportedEncodingException;
import org.folio.rest.RestVerticle;
import org.folio.rest.client.TenantClient;
import org.folio.rest.jaxrs.model.UdProvidersDataCollection;
import org.folio.rest.jaxrs.model.UsageDataProvider;
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
  private static final String TENANT = "diku";
  private static Vertx vertx;
  private static Context vertxContext;
  private static int port;
  @Rule
  public Timeout timeout = Timeout.seconds(10);

  @BeforeClass
  public static void setUp(TestContext context) {
    vertx = Vertx.vertx();
    vertxContext = vertx.getOrCreateContext();
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
    DeploymentOptions options = new DeploymentOptions()
        .setConfig(new JsonObject().put("http.port", port))
        .setWorker(true);

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
  public void checkThatWeCanAddGetPutAndDeleteUsageDataProviders() {
    UsageDataProvider usageDataProvider = given()
        .body("{\n"
            + " \"id\": \"fc20602a-8ade-4b66-90d0-2853c99affa5\",\n"
            + " \"label\": \"Nature Sushi\",\n"
            + " \"vendorId\": \"uuid-123456789\",\n"
            + " \"platformId\": \"uuid-123456789\",\n"
            + " \"harvestingStatus\": \"active\",\n"
            + " \"serviceType\": \"Sushi Lite\",\n"
            + " \"serviceUrl\": \"http://example.com\",\n"
            + " \"reportRelease\": 4,\n"
            + " \"requestedReports\": [\n"
            + "  \"JR1\"\n"
            + " ],"
            + " \"customerId\": \"12345def\",\n"
            + " \"requestorId\": \"1234abcd\",\n"
            + " \"apiKey\": \"678iuoi\",\n"
            + " \"requestorName\": \"Karla Kolumna\",\n"
            + " \"requestorMail\": \"kolumna@ub.uni-leipzig.de\"\n"
            + "}")
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", APPLICATION_JSON)
        .request()
        .post(BASE_URI)
        .thenReturn()
        .as(UsageDataProvider.class);
    assertThat(usageDataProvider.getLabel()).isEqualTo("Nature Sushi");
    assertThat(usageDataProvider.getId()).isNotEmpty();

    given()
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", APPLICATION_JSON)
        .when()
        .get(BASE_URI + "/" + usageDataProvider.getId())
        .then()
        .contentType(ContentType.JSON)
        .statusCode(200)
        .body("id", equalTo(usageDataProvider.getId()))
        .body("label", equalTo(usageDataProvider.getLabel()));

    given()
        .body("{\n"
            + " \"id\": \"fc20602a-8ade-4b66-90d0-2853c99affa5\",\n"
            + " \"label\": \"Nature CHANGED\",\n"
            + " \"vendorId\": \"uuid-123456789\",\n"
            + " \"platformId\": \"uuid-123456789\",\n"
            + " \"harvestingStatus\": \"active\",\n"
            + " \"serviceType\": \"Sushi Lite\",\n"
            + " \"serviceUrl\": \"http://example.com\",\n"
            + " \"reportRelease\": 4,\n"
            + " \"requestedReports\": [\n"
            + "  \"JR1\"\n"
            + " ],"
            + " \"customerId\": \"12345def\",\n"
            + " \"requestorId\": \"1234abcd\",\n"
            + " \"apiKey\": \"678iuoi\",\n"
            + " \"requestorName\": \"Karla Kolumna\",\n"
            + " \"requestorMail\": \"CHANGED@ub.uni-leipzig.de\"\n"
            + "}")
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", "text/plain")
        .request()
        .put(BASE_URI + "/" + usageDataProvider.getId())
        .then()
        .statusCode(204);

    UsageDataProvider changed = given()
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", APPLICATION_JSON)
        .request()
        .get(BASE_URI + "/" + usageDataProvider.getId())
        .thenReturn()
        .as(UsageDataProvider.class);
    assertThat(changed.getId()).isEqualTo(usageDataProvider.getId());
    assertThat(changed.getLabel()).isEqualTo("Nature CHANGED");
    assertThat(changed.getRequestorMail()).isEqualTo("CHANGED@ub.uni-leipzig.de");

    given()
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", "text/plain")
        .when()
        .delete(BASE_URI + "/" + usageDataProvider.getId())
        .then()
        .statusCode(204);

    given()
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", APPLICATION_JSON)
        .when()
        .get(BASE_URI + "/" + usageDataProvider.getId())
        .then()
        .statusCode(404);
  }

  @Test
  public void checkThatWeCanSearchByCQL() throws UnsupportedEncodingException {
    UsageDataProvider usageDataProvider = given()
        .body("{\n"
            + " \"id\": \"b4a196da-434c-475f-9c65-63f9951d1909\",\n"
            + " \"label\": \"Test Usage Data Provider\",\n"
            + " \"vendorId\": \"uuid-123456789\",\n"
            + " \"platformId\": \"uuid-123456789\",\n"
            + " \"harvestingStatus\": \"active\",\n"
            + " \"serviceType\": \"Sushi Lite\",\n"
            + " \"serviceUrl\": \"http://example.com\",\n"
            + " \"reportRelease\": 4,\n"
            + " \"requestedReports\": [\n"
            + "  \"JR1\"\n"
            + " ],"
            + " \"customerId\": \"12345def\",\n"
            + " \"requestorId\": \"1234abcd\",\n"
            + " \"apiKey\": \"678iuoi\",\n"
            + " \"requestorName\": \"Karla Kolumna\",\n"
            + " \"requestorMail\": \"kolumna@ub.uni-leipzig.de\"\n"
            + "}")
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
    UdProvidersDataCollection queryResult = given()
        .header("X-Okapi-Tenant", TENANT)
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
        .body("{\n"
            + " \"id\": \"fc20602a-8ade-4b66-90d0-2853c99affa5\",\n"
            + " \"vendorId\": \"uuid-123456789\",\n"
            + " \"platformId\": \"uuid-123456789\",\n"
            + " \"harvestingStatus\": \"active\",\n"
            + " \"serviceType\": \"Sushi Lite\",\n"
            + " \"serviceUrl\": \"http://example.com\",\n"
            + " \"reportRelease\": 4,\n"
            + " \"requestedReports\": [\n"
            + "  \"JR1\"\n"
            + " ],"
            + " \"customerId\": \"12345def\",\n"
            + " \"requestorId\": \"1234abcd\",\n"
            + " \"apiKey\": \"678iuoi\",\n"
            + " \"requestorName\": \"Karla Kolumna\",\n"
            + " \"requestorMail\": \"kolumna@ub.uni-leipzig.de\"\n"
            + "}")
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", APPLICATION_JSON)
        .request()
        .post(BASE_URI)
        .then()
        .statusCode(422);
  }

}
