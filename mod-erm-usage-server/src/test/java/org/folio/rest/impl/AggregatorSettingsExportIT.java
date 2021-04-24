package org.folio.rest.impl;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.equalTo;

import com.google.common.io.Resources;
import com.google.common.net.HttpHeaders;
import com.google.common.net.MediaType;
import io.restassured.RestAssured;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.parsing.Parser;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.rest.RestVerticle;
import org.folio.rest.jaxrs.model.AggregatorSetting;
import org.folio.rest.jaxrs.model.AggregatorSettings;
import org.folio.rest.jaxrs.model.UsageDataProviders;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.NetworkUtils;
import org.folio.rest.util.Constants;
import org.folio.rest.util.PostgresContainerRule;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.olf.erm.usage.counter.common.ExcelUtil;

@RunWith(VertxUnitRunner.class)
public class AggregatorSettingsExportIT {

  private static final String BASE_URI = "/aggregator-settings";
  private static final String TENANT = "diku";
  private static final Vertx vertx = Vertx.vertx();

  private static final String EXPECTED_CSV_RESULT_EMPTY =
      "providerName,harvestingStatus,reportRelease,requestedReports,customerId,requestorId,apiKey,requestorName,requestorMail,createdDate,updatedDate\n";
  private static final String EXPECTED_CSV_RESULT =
      "providerName,harvestingStatus,reportRelease,requestedReports,customerId,requestorId,apiKey,requestorName,requestorMail,createdDate,updatedDate\n"
          + "Provider1,inactive,4,\"JR1, JR4\",CustomerId1,RequestorId1,ApiKey1,RequestorName1,RequestorMail1,,\n"
          + "Provider2,active,4,\"JR1, JR4\",CustomerId2,RequestorId2,ApiKey2,\"RequestorName2,WithComma\",RequestorMail2,,\n"
          + "Provider3,active,4,\"JR1, JR4\",CustomerId3,RequestorId3,ApiKey3,RequestorName3,RequestorMail3,,\n";

  @ClassRule public static PostgresContainerRule pgRule = new PostgresContainerRule(vertx, TENANT);

  @BeforeClass
  public static void beforeClass(TestContext context) {
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

    Async async = context.async();
    DeploymentOptions options =
        new DeploymentOptions().setConfig(new JsonObject().put("http.port", port));
    vertx.deployVerticle(
        RestVerticle.class.getName(),
        options,
        ar -> {
          if (ar.succeeded()) {
            async.complete();
          } else {
            context.fail(ar.cause());
          }
        });

    async.await();
    try {
      setupTestData(context);
    } catch (IOException e) {
      context.fail(e);
    }
  }

  @AfterClass
  public static void afterClass() {
    RestAssured.reset();
  }

  private static void setupTestData(TestContext ctx) throws IOException {
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
            .getUsageDataProviders()
            .stream()
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
            Constants.TABLE_NAME_UDP,
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

  @Test
  public void testExportSushiCredentialsForAggregatorAsCsv() {
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
    assertThat(result).isEqualTo(EXPECTED_CSV_RESULT);

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
    assertThat(result2).isEqualTo(EXPECTED_CSV_RESULT_EMPTY);
  }

  @Test
  public void testExportSushiCredentialsForAggregatorAsXlsx() throws IOException {
    // three providers for id
    InputStream result =
        given()
            .pathParam("id", "0adec15b-8230-48fe-b4df-87106c5dc36e")
            .queryParam("format", "xlsx")
            .get("/{id}/exportcredentials")
            .then()
            .statusCode(200)
            .contentType(MediaType.OOXML_SHEET.type())
            .extract()
            .asInputStream();
    assertThat(ExcelUtil.toCSV(result)).isEqualToNormalizingNewlines(EXPECTED_CSV_RESULT);

    // no providers for id
    InputStream result2 =
        given()
            .pathParam("id", "c53dfc71-5086-4bbe-b592-26b2c977bc1f")
            .queryParam("format", "xlsx")
            .get("/{id}/exportcredentials")
            .then()
            .statusCode(200)
            .contentType(MediaType.OOXML_SHEET.type())
            .extract()
            .asInputStream();
    assertThat(ExcelUtil.toCSV(result2)).isEqualToNormalizingNewlines(EXPECTED_CSV_RESULT_EMPTY);
  }

  @Test
  public void testExportSushiCredentialsForAggregatorAsUnsupportedFormat() {
    given()
        .pathParam("id", "0adec15b-8230-48fe-b4df-87106c5dc36e")
        .queryParam("format", "jpg")
        .get("/{id}/exportcredentials")
        .then()
        .statusCode(400)
        .contentType(MediaType.PLAIN_TEXT_UTF_8.type())
        .body(equalTo("Requested format \"jpg\" is not supported."));
  }
}
