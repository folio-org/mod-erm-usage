package org.folio.rest.impl;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.io.Resources;
import com.google.common.net.HttpHeaders;
import com.google.common.net.MediaType;
import io.restassured.RestAssured;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.filter.log.RequestLoggingFilter;
import io.restassured.filter.log.ResponseLoggingFilter;
import io.restassured.parsing.Parser;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.UUID;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.rest.RestVerticle;
import org.folio.rest.client.TenantClient;
import org.folio.rest.jaxrs.model.AggregatorSetting;
import org.folio.rest.jaxrs.model.AggregatorSettings;
import org.folio.rest.jaxrs.model.UsageDataProviders;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.NetworkUtils;
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
        new RequestSpecBuilder()
            .addHeader(XOkapiHeaders.TENANT, TENANT)
            .addHeader(HttpHeaders.CONTENT_TYPE, MediaType.JSON_UTF_8.toString())
            .build();

    TenantClient tenantClient = new TenantClient("http://localhost:" + port, TENANT, TENANT);
    DeploymentOptions options =
        new DeploymentOptions().setConfig(new JsonObject().put("http.port", port)).setWorker(true);

    vertx.deployVerticle(
        RestVerticle.class.getName(),
        options,
        res -> {
          try {
            tenantClient.postTenant(
                null,
                res2 -> {
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
    vertx.close(
        context.asyncAssertSuccess(
            res -> {
              PostgresClient.stopEmbeddedPostgres();
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
    assertThat(postResponse).isEqualToComparingFieldByFieldRecursively(aggregatorSettingSample);

    AggregatorSettings ass;
    ass = given().get().then().statusCode(200).extract().as(AggregatorSettings.class);
    assertThat(ass.getTotalRecords()).isEqualTo(1);
    assertThat(ass.getAggregatorSettings().get(0))
        .isEqualToComparingFieldByFieldRecursively(aggregatorSettingSample);

    // PUT & GET
    given()
        .body(aggregatorSettingSampleModified)
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
            .param(QUERY_PARAM, "(label=*Digital*)")
            .get()
            .then()
            .statusCode(200)
            .extract()
            .as(AggregatorSettings.class);
    assertThat(as.getTotalRecords()).isEqualTo(1);
    assertThat(as.getAggregatorSettings().get(0))
        .isEqualToComparingFieldByFieldRecursively(aggregatorSettingSample);

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
        .isEqualToComparingFieldByFieldRecursively(aggregatorSettingSample);

    as =
        given()
            .param(QUERY_PARAM, "(label=somelabelthatsnotpresent)")
            .get()
            .then()
            .statusCode(200)
            .extract()
            .as(AggregatorSettings.class);
    assertThat(as.getTotalRecords()).isEqualTo(0);
    assertThat(as.getAggregatorSettings()).isEmpty();

    given().delete(location).then().statusCode(204);
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
    assertThat(as).isEqualToComparingFieldByFieldRecursively(aggregatorSettingSample);
    given().delete(location).then().statusCode(204);
  }

  @Test
  public void checkThatMetaDataIsCreatedOnPost() {
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
    assertThat(as.getMetadata()).isNull();
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

  private void setupTestData(TestContext ctx) throws IOException {
    AggregatorSettings aggregatorSettings =
        Json.decodeValue(
            Resources.toString(
                Resources.getResource("exportcredentials/aggregators.json"),
                StandardCharsets.UTF_8),
            AggregatorSettings.class);
    AggregatorSetting aggregator1 = aggregatorSettings.getAggregatorSettings().get(0);

    JsonArray providers =
        Json.decodeValue(
                Resources.toString(
                    Resources.getResource("exportcredentials/providers.json"),
                    StandardCharsets.UTF_8),
                UsageDataProviders.class)
            .getUsageDataProviders().stream()
            .map(Json::encode)
            .collect(JsonArray::new, JsonArray::add, JsonArray::addAll);

    Async async = ctx.async(2);
    PostgresClient.getInstance(vertx, TENANT)
        .save(
            "aggregator_settings",
            aggregator1.getId(),
            aggregator1,
            ar -> {
              assertThat(ar.succeeded()).isTrue();
              async.countDown();
            });
    PostgresClient.getInstance(vertx, TENANT)
        .saveBatch(
            "usage_data_providers",
            providers,
            ar -> {
              assertThat(ar.succeeded()).isTrue();
              async.countDown();
            });

    async.await(5000);

    AggregatorSettings as = given().get().then().extract().as(AggregatorSettings.class);
    assertThat(as.getAggregatorSettings().size()).isEqualTo(1);
    assertThat(as.getAggregatorSettings().get(0).getId())
        .isEqualTo("0adec15b-8230-48fe-b4df-87106c5dc36e");
    UsageDataProviders providersResult =
        given()
            .basePath("/usage-data-providers")
            .get()
            .then()
            .extract()
            .as(UsageDataProviders.class);
    assertThat(providersResult.getUsageDataProviders().size()).isEqualTo(4);
  }

  private void clearTestData(TestContext ctx) {
    Async async = ctx.async(2);
    PostgresClient.getInstance(vertx, TENANT)
        .delete(
            "usage_data_providers",
            new Criterion(),
            ar -> {
              assertThat(ar.succeeded()).isTrue();
              async.countDown();
            });
    PostgresClient.getInstance(vertx, TENANT)
        .delete(
            "aggregator_settings",
            new Criterion(),
            ar -> {
              assertThat(ar.succeeded()).isTrue();
              async.countDown();
            });
    async.await(5000);

    AggregatorSettings aggregators =
        given()
            .basePath("/aggregator-settings")
            .get()
            .then()
            .extract()
            .as(AggregatorSettings.class);
    assertThat(aggregators.getAggregatorSettings().size()).isEqualTo(0);
    UsageDataProviders providers =
        given()
            .basePath("/usage-data-providers")
            .get()
            .then()
            .extract()
            .as(UsageDataProviders.class);
    assertThat(aggregators.getAggregatorSettings().size()).isEqualTo(0);
  }

  @Test
  public void testExportSushiCredentialsForAggregator(TestContext ctx) throws IOException {
    setupTestData(ctx);

    // three providers for id
    String result =
        given()
            .pathParam("id", "0adec15b-8230-48fe-b4df-87106c5dc36e")
            .get("/{id}/exportcredentials")
            .then()
            .statusCode(200)
            .contentType(MediaType.CSV_UTF_8.type())
            .extract()
            .asString();
    String expectedResult =
        "providerName,harvestingStatus,reportRelease,requestedReports,customerId,requestorId,apiKey,requestorName,requestorMail\n"
            + "Provider1,inactive,4,\"JR1, JR4\",CustomerId1,RequestorId1,ApiKey1,RequestorName1,RequestorMail1\n"
            + "Provider2,active,4,\"JR1, JR4\",CustomerId2,RequestorId2,ApiKey2,\"RequestorName2,WithComma\",RequestorMail2\n"
            + "Provider3,active,4,\"JR1, JR4\",CustomerId3,RequestorId3,ApiKey3,RequestorName3,RequestorMail3\n";
    assertThat(result).isEqualTo(expectedResult);

    // no providers for id
    String result2 =
        given()
            .pathParam("id", "c53dfc71-5086-4bbe-b592-26b2c977bc1f")
            .get("/{id}/exportcredentials")
            .then()
            .statusCode(200)
            .contentType(MediaType.CSV_UTF_8.type())
            .extract()
            .asString();
    String expectedResult2 =
        "providerName,harvestingStatus,reportRelease,requestedReports,customerId,requestorId,apiKey,requestorName,requestorMail\n";
    assertThat(result2).isEqualTo(expectedResult2);

    clearTestData(ctx);
  }
}
