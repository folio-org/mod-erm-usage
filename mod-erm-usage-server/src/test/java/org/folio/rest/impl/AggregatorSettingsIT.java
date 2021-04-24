package org.folio.rest.impl;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.net.HttpHeaders;
import com.google.common.net.MediaType;
import io.restassured.RestAssured;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.parsing.Parser;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.UUID;
import org.assertj.core.api.recursive.comparison.RecursiveComparisonConfiguration;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.postgres.testing.PostgresTesterContainer;
import org.folio.rest.RestVerticle;
import org.folio.rest.client.TenantClient;
import org.folio.rest.jaxrs.model.AggregatorSetting;
import org.folio.rest.jaxrs.model.AggregatorSettings;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.NetworkUtils;
import org.folio.rest.util.ModuleVersion;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class AggregatorSettingsIT {

  private static final String BASE_URI = "/aggregator-settings";
  private static final String TENANT = "diku";
  private static final String QUERY_PARAM = "query";

  private static Vertx vertx;

  private static String aggregatorSettingSampleString;
  private static AggregatorSetting aggregatorSettingSample;
  private static AggregatorSetting aggregatorSettingSampleModified;
  private static RecursiveComparisonConfiguration comparisonConfiguration;
  @Rule public Timeout timeout = Timeout.seconds(10);

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
    PostgresClient.setPostgresTester(new PostgresTesterContainer());
    Async async = context.async();
    int port = NetworkUtils.nextFreePort();

    RestAssured.reset();
    RestAssured.baseURI = "http://localhost";
    RestAssured.basePath = BASE_URI;
    RestAssured.port = port;
    RestAssured.defaultParser = Parser.JSON;
    // RestAssured.filters(new ResponseLoggingFilter(), new RequestLoggingFilter());
    RestAssured.requestSpecification =
        new RequestSpecBuilder()
            .addHeader(XOkapiHeaders.TENANT, TENANT)
            .addHeader(HttpHeaders.CONTENT_TYPE, MediaType.JSON_UTF_8.toString())
            .build();

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

    comparisonConfiguration = new RecursiveComparisonConfiguration();
    comparisonConfiguration.ignoreFields("metadata");
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
  public void checkThatWeCanPostGetPutAndDelete() {
    // POST & GET
    AggregatorSetting postResponse =
        given()
            .body(aggregatorSettingSample)
            .post()
            .then()
            .statusCode(201)
            .extract()
            .as(AggregatorSetting.class);
    assertThat(postResponse)
        .usingRecursiveComparison(comparisonConfiguration)
        .isEqualTo(aggregatorSettingSample);

    AggregatorSettings ass;
    ass = given().get().then().statusCode(200).extract().as(AggregatorSettings.class);
    assertThat(ass.getTotalRecords()).isEqualTo(1);
    assertThat(ass.getAggregatorSettings().get(0))
        .usingRecursiveComparison(comparisonConfiguration)
        .isEqualTo(aggregatorSettingSample);

    // PUT & GET
    given()
        .body(aggregatorSettingSampleModified)
        .put(aggregatorSettingSample.getId())
        .then()
        .statusCode(204);

    ass = given().get().then().statusCode(200).extract().as(AggregatorSettings.class);
    assertThat(ass.getTotalRecords()).isEqualTo(1);
    assertThat(ass.getAggregatorSettings().get(0))
        .usingRecursiveComparison(comparisonConfiguration)
        .isEqualTo(aggregatorSettingSampleModified);

    // DELETE & GET
    given().delete(aggregatorSettingSample.getId()).then().statusCode(204);

    ass = given().get().then().statusCode(200).extract().as(AggregatorSettings.class);
    assertThat(ass.getTotalRecords()).isZero();
    assertThat(ass.getAggregatorSettings()).isEmpty();
  }

  @Test
  public void checkThatWeCanSearchByCQL() {
    String location =
        given()
            .body(aggregatorSettingSample)
            .post()
            .then()
            .statusCode(201)
            .extract()
            .header(HttpHeaders.LOCATION);

    AggregatorSettings as =
        given()
            .param(QUERY_PARAM, "(label==*Digital*)")
            .get()
            .then()
            .statusCode(200)
            .extract()
            .as(AggregatorSettings.class);
    assertThat(as.getTotalRecords()).isEqualTo(1);
    assertThat(as.getAggregatorSettings().get(0))
        .usingRecursiveComparison(comparisonConfiguration)
        .isEqualTo(aggregatorSettingSample);

    as =
        given()
            .param(QUERY_PARAM, "(accountConfig.configType=Manual)")
            .get()
            .then()
            .statusCode(200)
            .extract()
            .as(AggregatorSettings.class);
    assertThat(as.getTotalRecords()).isEqualTo(1);
    assertThat(as.getAggregatorSettings().get(0))
        .usingRecursiveComparison(comparisonConfiguration)
        .isEqualTo(aggregatorSettingSample);

    as =
        given()
            .param(QUERY_PARAM, "(label=somelabelthatsnotpresent)")
            .get()
            .then()
            .statusCode(200)
            .extract()
            .as(AggregatorSettings.class);
    assertThat(as.getTotalRecords()).isZero();
    assertThat(as.getAggregatorSettings()).isEmpty();

    given().delete(location).then().statusCode(204);
  }

  @Test
  public void checkThatInvalidAggregatorSettingIsNotPosted() {
    AggregatorSetting withoutLabel =
        Json.decodeValue(aggregatorSettingSampleString, AggregatorSetting.class).withLabel(null);

    given().body(withoutLabel).post().then().statusCode(422);
    AggregatorSettings as = given().get().then().extract().as(AggregatorSettings.class);
    assertThat(as.getTotalRecords()).isZero();
  }

  @Test
  public void checkThatIdIsGeneratedOnPost() {
    AggregatorSetting withoutId =
        Json.decodeValue(aggregatorSettingSampleString, AggregatorSetting.class).withId(null);
    String location =
        given()
            .body(withoutId)
            .post()
            .then()
            .statusCode(201)
            .extract()
            .header(HttpHeaders.LOCATION);
    given().delete(location).then().statusCode(204);
  }

  @Test
  public void checkThatWeCanGetByIdFromReturnedLocationAfterPost() {
    String location =
        given()
            .body(aggregatorSettingSample)
            .post()
            .then()
            .statusCode(201)
            .extract()
            .header(HttpHeaders.LOCATION);
    AggregatorSetting as =
        given().get(location).then().statusCode(200).extract().as(AggregatorSetting.class);
    assertThat(as)
        .usingRecursiveComparison(comparisonConfiguration)
        .isEqualTo(aggregatorSettingSample);
    given().delete(location).then().statusCode(204);
  }

  @Test
  public void checkThatMetaDataIsCreatedOnPost() {
    // METADATA IS CREATED IF USER_ID IS NULL
    // no user header
    String location =
        given()
            .body(aggregatorSettingSample)
            .post()
            .then()
            .statusCode(201)
            .extract()
            .header("Location");
    AggregatorSetting as = given().get(location).then().extract().as(AggregatorSetting.class);
    assertThat(as.getMetadata()).isNotNull();
    given().delete(location).then().statusCode(204);

    // with user header
    location =
        given()
            .body(aggregatorSettingSample)
            .header(XOkapiHeaders.USER_ID, UUID.randomUUID())
            .post()
            .then()
            .statusCode(201)
            .extract()
            .header("Location");
    as = given().get(location).then().extract().as(AggregatorSetting.class);
    assertThat(as.getMetadata()).isNotNull();
    given().delete(location).then().statusCode(204);
  }
}
