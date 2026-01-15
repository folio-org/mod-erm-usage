package org.folio.rest.impl2;

import static com.google.common.net.HttpHeaders.ACCEPT;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static io.restassured.RestAssured.given;
import static io.restassured.http.ContentType.BINARY;
import static io.restassured.http.ContentType.TEXT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.equalTo;

import io.restassured.RestAssured;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.parsing.Parser;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.RandomUtils;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.postgres.testing.PostgresTesterContainer;
import org.folio.rest.RestVerticle;
import org.folio.rest.impl.TenantAPI;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.ModuleName;
import org.folio.rest.tools.utils.NetworkUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class ErmUsageFilesIT {

  private static final String TENANT = "diku";
  private static final String ERM_USAGE_FILES_ENDPOINT = "/erm-usage/files";
  private static final String TEST_CONTENT = "This is the test content!!!!";
  private static Vertx vertx;
  private static WebClient webClient;

  @BeforeClass
  public static void setUp(TestContext context) {
    vertx = Vertx.vertx();
    webClient = WebClient.create(vertx);

    PostgresClient.setPostgresTester(new PostgresTesterContainer());
    PostgresClient.getInstance(vertx);

    int port = NetworkUtils.nextFreePort();

    RestAssured.reset();
    RestAssured.baseURI = "http://localhost";
    RestAssured.basePath = "";
    RestAssured.port = port;
    RestAssured.defaultParser = Parser.JSON;
    RestAssured.requestSpecification =
        new RequestSpecBuilder().addHeader(XOkapiHeaders.TENANT, TENANT).build();

    DeploymentOptions options =
        new DeploymentOptions().setConfig(new JsonObject().put("http.port", port));

    String moduleId = ModuleName.getModuleName() + "-" + ModuleName.getModuleVersion();
    vertx
        .deployVerticle(RestVerticle.class.getName(), options)
        .compose(
            s ->
                new TenantAPI()
                    .postTenantSync(
                        new TenantAttributes().withModuleTo(moduleId),
                        Map.of(XOkapiHeaders.TENANT, TENANT),
                        vertx.getOrCreateContext()))
        .onComplete(
            context.asyncAssertSuccess(resp -> assertThat(resp.getStatus()).isEqualTo(204)));
  }

  @AfterClass
  public static void teardown(TestContext context) {
    RestAssured.reset();
    vertx
        .close()
        .onComplete(context.asyncAssertSuccess(res -> PostgresClient.stopPostgresTester()));
  }

  private Future<HttpResponse<Buffer>> upload(byte[] bytes) {
    return webClient
        .post(RestAssured.port, "localhost", ERM_USAGE_FILES_ENDPOINT)
        .putHeader(XOkapiHeaders.TENANT, TENANT)
        .putHeader(CONTENT_TYPE, BINARY.toString())
        .sendBuffer(Buffer.buffer(bytes));
  }

  private Future<HttpResponse<Buffer>> get(String id) {
    return webClient
        .get(RestAssured.port, "localhost", ERM_USAGE_FILES_ENDPOINT + "/" + id)
        .putHeader(XOkapiHeaders.TENANT, TENANT)
        .putHeader(ACCEPT, BINARY.toString())
        .send();
  }

  @Test
  public void testConcurrentUpload(TestContext context) {
    List<Integer> byteSizes = List.of(25000, 50000, 75000, 125000, 250000);
    List<byte[]> bytesList = byteSizes.stream().map(RandomUtils::nextBytes).toList();
    List<Future<HttpResponse<Buffer>>> uploadFutures =
        bytesList.stream().map(this::upload).toList();

    Future.all(uploadFutures)
        .compose(
            compositeFuture -> {
              List<HttpResponse<Buffer>> uploadResponses =
                  uploadFutures.stream().map(Future::result).toList();

              // Test for correct size attribute of POST responses
              List<Float> sizes =
                  uploadResponses.stream()
                      .map(resp -> resp.bodyAsJsonObject().getString("size"))
                      .map(Float::valueOf)
                      .toList();
              assertThat(sizes)
                  .containsExactlyInAnyOrder(
                      byteSizes.stream().map(i -> i / 1000f).toArray(Float[]::new));

              // Get all uploaded files
              List<Future<HttpResponse<Buffer>>> getFutures =
                  uploadResponses.stream()
                      .map(resp -> resp.bodyAsJsonObject().getString("id"))
                      .map(this::get)
                      .toList();
              return Future.all(getFutures)
                  .map(cf2 -> getFutures.stream().map(Future::result).toList());
            })
        .onComplete(
            context.asyncAssertSuccess(
                getResponses -> {
                  // Test for correct body of GET responses
                  List<byte[]> downloadedBytes =
                      getResponses.stream().map(resp -> resp.body().getBytes()).toList();
                  assertThat(downloadedBytes)
                      .containsExactlyInAnyOrder(bytesList.toArray(byte[][]::new));
                }));
  }

  @Test
  public void checkThatWeCanAddGetAndDeleteFiles() {
    // POST File
    String id =
        given()
            .body(TEST_CONTENT.getBytes())
            .header(CONTENT_TYPE, BINARY)
            .post(ERM_USAGE_FILES_ENDPOINT)
            .then()
            .statusCode(200)
            .extract()
            .path("id");

    // GET
    given()
        .header(CONTENT_TYPE, BINARY)
        .get(ERM_USAGE_FILES_ENDPOINT + "/" + id)
        .then()
        .contentType(BINARY)
        .statusCode(200)
        .body(equalTo(TEST_CONTENT));

    // DELETE
    given().delete(ERM_USAGE_FILES_ENDPOINT + "/" + id).then().statusCode(204);

    // GET
    given()
        .header(CONTENT_TYPE, BINARY)
        .get(ERM_USAGE_FILES_ENDPOINT + "/" + id)
        .then()
        .contentType(TEXT)
        .statusCode(404);
  }

  @Test
  public void testUploadLargeFile() {
    byte[] content = new byte[30 * 1024 * 1024]; // 30 MB

    // POST file
    String id =
        given()
            .body(content)
            .header(CONTENT_TYPE, BINARY)
            .post(ERM_USAGE_FILES_ENDPOINT)
            .then()
            .statusCode(200)
            .extract()
            .path("id");

    // GET uploaded file
    byte[] getResult =
        given()
            .header(CONTENT_TYPE, BINARY)
            .get(ERM_USAGE_FILES_ENDPOINT + "/" + id)
            .then()
            .contentType(BINARY)
            .statusCode(200)
            .extract()
            .asByteArray();

    assertThat(getResult).isEqualTo(content);
  }
}
