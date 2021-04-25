package org.folio.rest.impl2;

import static com.google.common.net.HttpHeaders.ACCEPT;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static io.restassured.RestAssured.given;
import static io.restassured.http.ContentType.BINARY;
import static io.restassured.http.ContentType.TEXT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.equalTo;

import io.reactivex.Single;
import io.reactivex.schedulers.Schedulers;
import io.restassured.RestAssured;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.parsing.Parser;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.ext.web.client.HttpResponse;
import io.vertx.reactivex.ext.web.client.WebClient;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.RandomUtils;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.postgres.testing.PostgresTesterContainer;
import org.folio.rest.RestVerticle;
import org.folio.rest.impl.TenantAPI;
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

  private static final String TENANT = "diku";
  private static final String ERM_USAGE_FILES_ENDPOINT = "/erm-usage/files";
  private static final String TEST_CONTENT = "This is the test content!!!!";
  private static Vertx vertx;
  private static WebClient webClient;
  @Rule public Timeout timeout = Timeout.seconds(10);

  @BeforeClass
  public static void setUp(TestContext context) {
    vertx = Vertx.vertx();
    io.vertx.reactivex.core.Vertx vertxRx = io.vertx.reactivex.core.Vertx.newInstance(vertx);
    webClient = WebClient.create(vertxRx);

    PostgresClient.setPostgresTester(new PostgresTesterContainer());

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

    Async async = context.async();
    vertx.deployVerticle(
        RestVerticle.class.getName(),
        options,
        res -> {
          try {
            new TenantAPI()
                .postTenantSync(
                    new TenantAttributes().withModuleTo(ModuleVersion.getModuleVersion()),
                    Map.of(XOkapiHeaders.TENANT.toLowerCase(), TENANT),
                    res2 -> {
                      context.verify(v -> assertThat(res2.result().getStatus()).isEqualTo(204));
                      async.complete();
                    },
                    vertx.getOrCreateContext());
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
              PostgresClient.stopPostgresTester();
              async.complete();
            }));
  }

  private Single<HttpResponse<Buffer>> upload(byte[] bytes) {
    return webClient
        .post(RestAssured.port, "localhost", ERM_USAGE_FILES_ENDPOINT)
        .putHeader(XOkapiHeaders.TENANT, TENANT)
        .putHeader(CONTENT_TYPE, BINARY.toString())
        .rxSendBuffer(Buffer.buffer(bytes));
  }

  private Single<HttpResponse<Buffer>> get(String id) {
    return webClient
        .get(RestAssured.port, "localhost", ERM_USAGE_FILES_ENDPOINT + "/" + id)
        .putHeader(XOkapiHeaders.TENANT, TENANT)
        .putHeader(ACCEPT, BINARY.toString())
        .rxSend();
  }

  @Test
  public void testConcurrentUpload(TestContext context) {
    Async async = context.async();

    List<Integer> byteSizes = List.of(25000, 50000, 75000, 125000, 250000);
    List<byte[]> bytesList =
        byteSizes.stream().map(RandomUtils::nextBytes).collect(Collectors.toList());
    List<Single<HttpResponse<Buffer>>> uploadSingles =
        bytesList.stream().map(this::upload).collect(Collectors.toList());

    Single.merge(uploadSingles)
        .subscribeOn(Schedulers.io())
        .toList()
        .flattenAsFlowable(
            respList -> {
              Stream<Float> sizes =
                  respList.stream()
                      .map(resp -> resp.bodyAsJsonObject().getString("size"))
                      .map(Float::valueOf);
              // test for correct size attribute of POST responses
              assertThat(sizes)
                  .containsExactlyInAnyOrder(
                      byteSizes.stream().map(i -> i / 1000f).toArray(Float[]::new));
              return respList;
            })
        .map(resp -> resp.bodyAsJsonObject().getString("id"))
        .flatMapSingle(this::get)
        .toList()
        .subscribe(
            list -> {
              Stream<byte[]> bytes = list.stream().map(resp -> resp.body().getBytes());
              // test for correct body of GET responses
              assertThat(bytes).containsExactlyInAnyOrder(bytesList.toArray(byte[][]::new));
              async.complete();
            },
            context::fail);
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
}
