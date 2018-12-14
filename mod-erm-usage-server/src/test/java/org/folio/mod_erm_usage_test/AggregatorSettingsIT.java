package org.folio.mod_erm_usage_test;

import static com.jayway.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.UUID;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.rest.RestVerticle;
import org.folio.rest.client.TenantClient;
import org.folio.rest.jaxrs.model.AggregatorSetting;
import org.folio.rest.jaxrs.model.AggregatorSettings;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.NetworkUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import com.google.common.net.HttpHeaders;
import com.google.common.net.MediaType;
import com.jayway.restassured.RestAssured;
import com.jayway.restassured.builder.RequestSpecBuilder;
import com.jayway.restassured.filter.log.RequestLoggingFilter;
import com.jayway.restassured.filter.log.ResponseLoggingFilter;
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
public class AggregatorSettingsIT {

  private static final String BASE_URI = "/aggregator-settings";
  private static final String TENANT = "diku";
  private static final String QUERY_PARAM = "query";
  private static Vertx vertx;

  private static String aggregatorSettingSampleString;
  private static AggregatorSetting aggregatorSettingSample;
  private static AggregatorSetting aggregatorSettingSampleModified;

  @Rule
  public Timeout timeout = Timeout.seconds(10);

  @BeforeClass
  public static void setUp(TestContext context) {
    try {
      aggregatorSettingSampleString =
          new String(Files.readAllBytes(Paths.get("../ramls/examples/aggregatorsettings.sample")));
      aggregatorSettingSample =
          Json.decodeValue(aggregatorSettingSampleString, AggregatorSetting.class);
      aggregatorSettingSampleModified =
          Json.decodeValue(aggregatorSettingSampleString, AggregatorSetting.class)
              .withLabel("changed label");
    } catch (Exception ex) {
      context.fail(ex);
    }

    vertx = Vertx.vertx();
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
    RestAssured.basePath = BASE_URI;
    RestAssured.port = port;
    RestAssured.defaultParser = Parser.JSON;
    RestAssured.filters(new ResponseLoggingFilter(), new RequestLoggingFilter());
    RestAssured.requestSpecification =
        new RequestSpecBuilder().addHeader(XOkapiHeaders.TENANT, TENANT)
            .addHeader(HttpHeaders.CONTENT_TYPE, MediaType.JSON_UTF_8.toString())
            .build();

    TenantClient tenantClient = new TenantClient("localhost", port, TENANT, TENANT);
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
  public void checkThatWeCanPostGetPutAndDelete() {
    // POST & GET
    AggregatorSetting postResponse = given().body(aggregatorSettingSample)
        .post()
        .then()
        .statusCode(201)
        .extract()
        .as(AggregatorSetting.class);
    assertThat(postResponse).isEqualToComparingFieldByFieldRecursively(aggregatorSettingSample);

    AggregatorSettings ass;
    ass = given().get().then().statusCode(200).extract().as(AggregatorSettings.class);
    assertThat(ass.getTotalRecords()).isEqualTo(1);
    assertThat(ass.getAggregatorSettings().get(0))
        .isEqualToComparingFieldByFieldRecursively(aggregatorSettingSample);

    // PUT & GET
    given().body(aggregatorSettingSampleModified)
        .put(aggregatorSettingSample.getId())
        .then()
        .statusCode(204);

    ass = given().get().then().statusCode(200).extract().as(AggregatorSettings.class);
    assertThat(ass.getTotalRecords()).isEqualTo(1);
    assertThat(ass.getAggregatorSettings().get(0))
        .isEqualToComparingFieldByFieldRecursively(aggregatorSettingSampleModified);

    // DELETE & GET
    given().delete(aggregatorSettingSample.getId()).then().statusCode(204);

    ass = given().get().then().statusCode(200).extract().as(AggregatorSettings.class);
    assertThat(ass.getTotalRecords()).isEqualTo(0);
    assertThat(ass.getAggregatorSettings()).isEmpty();
  }

  @Test
  public void checkThatWeCanSearchByCQL() {
    String location = given().body(aggregatorSettingSample)
        .post()
        .then()
        .statusCode(201)
        .extract()
        .header(HttpHeaders.LOCATION);

    AggregatorSettings as = given().param(QUERY_PARAM, "(label=*Digital*)")
        .get()
        .then()
        .statusCode(200)
        .extract()
        .as(AggregatorSettings.class);
    assertThat(as.getTotalRecords()).isEqualTo(1);
    assertThat(as.getAggregatorSettings().get(0))
        .isEqualToComparingFieldByFieldRecursively(aggregatorSettingSample);

    as = given().param(QUERY_PARAM, "(accountConfig.configType=Manual)")
        .get()
        .then()
        .statusCode(200)
        .extract()
        .as(AggregatorSettings.class);
    assertThat(as.getTotalRecords()).isEqualTo(1);
    assertThat(as.getAggregatorSettings().get(0))
        .isEqualToComparingFieldByFieldRecursively(aggregatorSettingSample);

    as = given().param(QUERY_PARAM, "(label=somelabelthatsnotpresent)")
        .get()
        .then()
        .statusCode(200)
        .extract()
        .as(AggregatorSettings.class);
    assertThat(as.getTotalRecords()).isEqualTo(0);
    assertThat(as.getAggregatorSettings()).isEmpty();

    given().basePath("").delete(location).then().statusCode(204);
  }

  @Test
  public void checkThatInvalidAggregatorSettingIsNotPosted() {
    AggregatorSetting withoutLabel =
        Json.decodeValue(aggregatorSettingSampleString, AggregatorSetting.class).withLabel(null);

    given().body(withoutLabel).post().then().statusCode(422);
    AggregatorSettings as = given().get().then().extract().as(AggregatorSettings.class);
    assertThat(as.getTotalRecords()).isEqualTo(0);
  }

  @Test
  public void checkThatIdIsGeneratedOnPost() {
    AggregatorSetting withoutId =
        Json.decodeValue(aggregatorSettingSampleString, AggregatorSetting.class).withId(null);
    String location = given().body(withoutId)
        .post()
        .then()
        .statusCode(201)
        .extract()
        .header(HttpHeaders.LOCATION);
    given().basePath("").delete(location).then().statusCode(204);
  }

  @Test
  public void checkThatWeCanGetByIdFromReturnedLocationAfterPost() {
    String location = given().body(aggregatorSettingSample)
        .post()
        .then()
        .statusCode(201)
        .extract()
        .header(HttpHeaders.LOCATION);
    AggregatorSetting as = given().basePath("")
        .get(location)
        .then()
        .statusCode(200)
        .extract()
        .as(AggregatorSetting.class);
    assertThat(as).isEqualToComparingFieldByFieldRecursively(aggregatorSettingSample);
    given().basePath("").delete(location).then().statusCode(204);
  }

  @Test
  public void checkThatMetaDataIsCreatedOnPost() {
    // no user header
    String location = given().body(aggregatorSettingSample)
        .post()
        .then()
        .statusCode(201)
        .extract()
        .header("location");
    AggregatorSetting as =
        given().basePath("").get(location).then().extract().as(AggregatorSetting.class);
    assertThat(as.getMetadata()).isNull();
    given().basePath("").delete(location).then().statusCode(204);

    // with user header
    location = given().body(aggregatorSettingSample)
        .header(XOkapiHeaders.USER_ID, UUID.randomUUID())
        .post()
        .then()
        .statusCode(201)
        .extract()
        .header("location");
    as = given().basePath("").get(location).then().extract().as(AggregatorSetting.class);
    assertThat(as.getMetadata()).isNotNull();
    given().basePath("").delete(location).then().statusCode(204);
  }
}
