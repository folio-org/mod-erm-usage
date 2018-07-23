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
import org.folio.rest.jaxrs.model.AggregatorSetting;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.NetworkUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class AggregatorSettingsIT {

  public static final String APPLICATION_JSON = "application/json";
  public static final String BASE_URI = "/aggregator-settings";
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
  public void checkThatWeCanAddGetPutAndDeleteAggregatorSettings() {

    AggregatorSetting aggSetting = given()
        .body("{\n"
            + "  \"id\": \"debb8412-3cd9-4dc6-8390-5e71b017c24e\",\n"
            + "  \"label\": \"Nationaler Statistikserver\",\n"
            + "\t\"username\": \"TestUser\",\n"
            + "  \"password\": \"TestPassword\",\n"
            + "  \"apiKey\": \"132Test456ApiKey\",\n"
            + "  \"serviceUrl\": \"https://sushi.redi-bw.de\",\n"
            + "  \"accountConfig\": {\n"
            + "    \"configType\": \"Manual\",\n"
            + "    \"configMail\": \"ab@counter-stats.com\",\n"
            + "    \"displayContact\": [\n"
            + "      \"Counter Aggregator Contact\",\n"
            + "      \"Tel: +49 1234 - 9876\"\n"
            + "    ]\n"
            + "  }\n"
            + "}")
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", APPLICATION_JSON)
        .request()
        .post(BASE_URI)
        .thenReturn()
        .as(AggregatorSetting.class);
    assertThat(aggSetting.getLabel()).isEqualTo("Nationaler Statistikserver");
    assertThat(aggSetting.getId()).isNotEmpty();

    given()
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", APPLICATION_JSON)
        .when()
        .get(BASE_URI + "/" + aggSetting.getId())
        .then()
        .contentType(ContentType.JSON)
        .statusCode(200)
        .body("id", equalTo(aggSetting.getId()))
        .body("label", equalTo(aggSetting.getLabel()));

    given()
        .body("{\n"
            + "  \"id\": \"debb8412-3cd9-4dc6-8390-5e71b017c24e\",\n"
            + "  \"label\": \"Nationaler Statistikserver CHANGED\",\n"
            + "\t\"username\": \"TestUser\",\n"
            + "  \"password\": \"TestPassword\",\n"
            + "  \"apiKey\": \"132Test456ApiKey\",\n"
            + "  \"serviceUrl\": \"https://sushi.redi-bw.CHANGED\",\n"
            + "  \"accountConfig\": {\n"
            + "    \"configType\": \"Manual\",\n"
            + "    \"configMail\": \"ab@counter-stats.com\",\n"
            + "    \"displayContact\": [\n"
            + "      \"Counter Aggregator Contact\",\n"
            + "      \"Tel: +49 1234 - 9876\"\n"
            + "    ]\n"
            + "  }\n"
            + "}")
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", "text/plain")
        .request()
        .put(BASE_URI + "/" + aggSetting.getId())
        .then()
        .statusCode(204);

    AggregatorSetting changed = given()
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", APPLICATION_JSON)
        .request()
        .get(BASE_URI + "/" + aggSetting.getId())
        .thenReturn()
        .as(AggregatorSetting.class);
    assertThat(changed.getId()).isEqualTo(aggSetting.getId());
    assertThat(changed.getLabel()).isEqualTo("Nationaler Statistikserver CHANGED");
    assertThat(changed.getServiceUrl()).isEqualTo("https://sushi.redi-bw.CHANGED");

    given()
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", "text/plain")
        .when()
        .delete(BASE_URI + "/" + aggSetting.getId())
        .then()
        .statusCode(204);

    given()
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", APPLICATION_JSON)
        .when()
        .get(BASE_URI + "/" + aggSetting.getId())
        .then()
        .statusCode(404);
  }

  @Test
  public void checkThatWeCanSearchByCQL() throws UnsupportedEncodingException {
    AggregatorSetting aggSetting = given()
        .body("{\n"
            + "  \"id\": \"decd9dd8-ffdf-489a-bebd-38e0cb3c4948\",\n"
            + "  \"label\": \"Test Aggregator\",\n"
            + "\t\"username\": \"TestUser\",\n"
            + "  \"password\": \"TestPassword\",\n"
            + "  \"apiKey\": \"132Test456ApiKey\",\n"
            + "  \"serviceUrl\": \"https://sushi.redi-bw.de\",\n"
            + "  \"accountConfig\": {\n"
            + "    \"configType\": \"Manual\",\n"
            + "    \"configMail\": \"ab@counter-stats.com\",\n"
            + "    \"displayContact\": [\n"
            + "      \"Counter Aggregator Contact\",\n"
            + "      \"Tel: +49 1234 - 9876\"\n"
            + "    ]\n"
            + "  }\n"
            + "}")
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", APPLICATION_JSON)
        .request()
        .post(BASE_URI)
        .thenReturn()
        .as(AggregatorSetting.class);
    assertThat(aggSetting.getLabel()).isEqualTo("Test Aggregator");
    assertThat(aggSetting.getId()).isNotEmpty();

    String cqlLabel = "?query=(label=\"Test Aggr*\")";
    given()
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", APPLICATION_JSON)
        .when()
        .get(BASE_URI + cqlLabel)
        .then()
        .contentType(ContentType.JSON)
        .statusCode(200)
        .body("aggregatorSettings.size()", equalTo(1))
        .body("aggregatorSettings[0].id", equalTo(aggSetting.getId()))
        .body("aggregatorSettings[0].label", equalTo(aggSetting.getLabel()));

    String cqlConfigType = "?query=(accountConfig.configType=\"Manual\")";
    given()
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", APPLICATION_JSON)
        .when()
        .get(BASE_URI + cqlConfigType)
        .then()
        .contentType(ContentType.JSON)
        .statusCode(200)
        .body("aggregatorSettings.size()", equalTo(1))
        .body("aggregatorSettings[0].id", equalTo(aggSetting.getId()))
        .body("aggregatorSettings[0].label", equalTo(aggSetting.getLabel()));
  }

  @Test
  public void checkThatInvalidAggregatorSettingsIsNotPosted() {
    given()
        .body("{\n"
            + "  \"id\": \"fb7f9f78-6fff-4492-8312-455f2e043175\",\n"
            + "  \"username\": \"TestUser\",\n"
            + "  \"password\": \"TestPassword\",\n"
            + "  \"apiKey\": \"132Test456ApiKey\",\n"
            + "  \"serviceUrl\": \"https://sushi.redi-bw.de\",\n"
            + "  \"accountConfig\": {\n"
            + "    \"configType\": \"Manual\",\n"
            + "    \"configMail\": \"ab@counter-stats.com\",\n"
            + "    \"displayContact\": [\n"
            + "      \"Counter Aggregator Contact\",\n"
            + "      \"Tel: +49 1234 - 9876\"\n"
            + "    ]\n"
            + "  }\n"
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
