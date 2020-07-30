package org.folio.rest.impl;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;

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
import java.util.UUID;
import org.folio.rest.RestVerticle;
import org.folio.rest.client.TenantClient;
import org.folio.rest.jaxrs.model.CustomReport;
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
public class CustomReportIT {

  private static final String APPLICATION_JSON = "application/json";
  private static final String BASE_URI = "/custom-reports";
  private static final String TENANT = "diku";
  private static Vertx vertx;
  private static CustomReport reportFirst;
  private static CustomReport reportSecond;
  private static CustomReport reportChanged;

  @Rule
  public Timeout timeout = Timeout.seconds(10);

  @BeforeClass
  public static void setUp(TestContext context) {
    vertx = Vertx.vertx();

    reportFirst = new CustomReport()
        .withId(UUID.randomUUID().toString())
        .withFileId(UUID.randomUUID().toString())
        .withFileName("filename.txt")
        .withFileSize(new Double(1024))
        .withProviderId(UUID.randomUUID().toString())
        .withYear(2019);

    reportSecond = new CustomReport()
        .withId(UUID.randomUUID().toString())
        .withFileId(UUID.randomUUID().toString())
        .withFileName("filename2.txt")
        .withFileSize(new Double(2024))
        .withProviderId(UUID.randomUUID().toString())
        .withYear(2020);

    reportChanged = reportFirst.withFileName("newFileName.txt");

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
              PostgresClient.stopEmbeddedPostgres();
              async.complete();
            }));
  }

  @Test
  public void checkThatWeCanAddGetPutAndDeleteCustomReports() {
    // POST
    given()
        .body(Json.encode(reportFirst))
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", APPLICATION_JSON)
        .post(BASE_URI)
        .then()
        .statusCode(201)
        .body("fileName", equalTo(reportFirst.getFileName()))
        .body("id", equalTo(reportFirst.getId()));

    // GET
    given()
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", APPLICATION_JSON)
        .get(BASE_URI + "/" + reportFirst.getId())
        .then()
        .contentType(ContentType.JSON)
        .statusCode(200)
        .body("id", equalTo(reportFirst.getId()));

    // PUT
    given()
        .body(Json.encode(reportChanged))
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", "text/plain")
        .put(BASE_URI + "/" + reportFirst.getId())
        .then()
        .statusCode(204);

    // GET again
    given()
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", APPLICATION_JSON)
        .get(BASE_URI + "/" + reportFirst.getId())
        .then()
        .statusCode(200)
        .body("id", equalTo(reportChanged.getId()))
        .body("fileName", equalTo(reportChanged.getFileName()));

    // POST second report
    given()
        .body(Json.encode(reportSecond))
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", APPLICATION_JSON)
        .post(BASE_URI)
        .then()
        .statusCode(201)
        .body("fileName", equalTo(reportSecond.getFileName()))
        .body("id", equalTo(reportSecond.getId()));

    // GET all
    given()
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", APPLICATION_JSON)
        .get(BASE_URI)
        .then()
        .statusCode(200)
        .body("customReports.size()", equalTo(2));

    // DELETE
    given()
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", "text/plain")
        .delete(BASE_URI + "/" + reportFirst.getId())
        .then()
        .statusCode(204);

    // DELETE
    given()
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", "text/plain")
        .delete(BASE_URI + "/" + reportSecond.getId())
        .then()
        .statusCode(204);

    // GET again
    given()
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", APPLICATION_JSON)
        .get(BASE_URI + "/" + reportFirst.getId())
        .then()
        .statusCode(404);
  }

}
