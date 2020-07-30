package org.folio.rest.impl;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;

import io.restassured.RestAssured;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.filter.log.RequestLoggingFilter;
import io.restassured.filter.log.ResponseLoggingFilter;
import io.restassured.http.ContentType;
import io.restassured.parsing.Parser;
import io.restassured.response.Response;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.rest.RestVerticle;
import org.folio.rest.client.TenantClient;
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
public class ErmUsageFilesIT {

  @Rule
  public Timeout timeout = Timeout.seconds(10);

  private static final String TENANT = "diku";
  private static final String ERM_USAGE_FILES_ENDPOINT = "/erm-usage/files";
  private static final String TEST_CONTENT = "This is the test content!!!!";
  private static Vertx vertx;

  @BeforeClass
  public static void setUp(TestContext context) {
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

    int port = NetworkUtils.nextFreePort();

    RestAssured.reset();
    RestAssured.baseURI = "http://localhost";
    RestAssured.basePath = "";
    RestAssured.port = port;
    RestAssured.defaultParser = Parser.JSON;
    RestAssured.filters(new ResponseLoggingFilter(), new RequestLoggingFilter());
    RestAssured.requestSpecification =
        new RequestSpecBuilder().addHeader(XOkapiHeaders.TENANT, TENANT).build();

    TenantClient tenantClient = new TenantClient("http://localhost:" + port, TENANT, TENANT);
    DeploymentOptions options =
        new DeploymentOptions().setConfig(new JsonObject().put("http.port", port)).setWorker(true);

    Async async = context.async();
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
    async.await();
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
  public void checkThatWeCanAddGetAndDeleteFiles() {
    // POST File
    Response postResponse =
        given()
            .body(TEST_CONTENT.getBytes())
            .header("X-Okapi-Tenant", TENANT)
            .header("content-type", ContentType.BINARY)
            .post(ERM_USAGE_FILES_ENDPOINT)
            .then()
            .statusCode(200)
            .extract()
            .response();
    String response = postResponse.getBody().print();
    JsonObject jsonResponse = new JsonObject(response);
    String id = jsonResponse.getString("id");

    // GET
    given()
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", ContentType.BINARY)
        .get(ERM_USAGE_FILES_ENDPOINT + "/" + id)
        .then()
        .contentType(ContentType.BINARY.toString())
        .statusCode(200)
        .body(equalTo(TEST_CONTENT));

    // DELETE
    given()
        .header("X-Okapi-Tenant", TENANT)
        .delete(ERM_USAGE_FILES_ENDPOINT + "/" + id)
        .then()
        .statusCode(204);

    // GET
    given()
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", ContentType.BINARY)
        .get(ERM_USAGE_FILES_ENDPOINT + "/" + id)
        .then()
        .contentType(ContentType.TEXT)
        .statusCode(404);
  }

}
