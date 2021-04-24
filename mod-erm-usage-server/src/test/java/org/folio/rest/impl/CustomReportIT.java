package org.folio.rest.impl;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

import io.restassured.RestAssured;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.http.ContentType;
import io.restassured.parsing.Parser;
import io.restassured.specification.RequestSpecification;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.assertj.core.api.recursive.comparison.RecursiveComparisonConfiguration;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.postgres.testing.PostgresTesterContainer;
import org.folio.rest.RestVerticle;
import org.folio.rest.jaxrs.model.CustomReport;
import org.folio.rest.jaxrs.model.CustomReports;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.NetworkUtils;
import org.folio.rest.util.Constants;
import org.folio.rest.util.ModuleVersion;
import org.junit.AfterClass;
import org.junit.Before;
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

  private final RequestSpecification defaultHeaders =
      new RequestSpecBuilder()
          .addHeader("X-Okapi-Tenant", TENANT)
          .addHeader("content-type", APPLICATION_JSON)
          .addHeader("accept", APPLICATION_JSON)
          .addHeader("accept", "text/plain")
          .build();

  private final RecursiveComparisonConfiguration ignoreMetadata =
      new RecursiveComparisonConfiguration() {
        {
          this.ignoreFields("metadata");
        }
      };

  @Rule public Timeout timeout = Timeout.seconds(10);

  @BeforeClass
  public static void beforeClass(TestContext context) {
    vertx = Vertx.vertx();

    reportFirst =
        new CustomReport()
            .withId(UUID.randomUUID().toString())
            .withFileId(UUID.randomUUID().toString())
            .withFileName("filename.txt")
            .withFileSize(1024d)
            .withProviderId(UUID.randomUUID().toString())
            .withYear(2019)
            .withLinkUrl("http://localhost/link1");

    reportSecond =
        new CustomReport()
            .withId(UUID.randomUUID().toString())
            .withFileId(UUID.randomUUID().toString())
            .withFileName("filename2.txt")
            .withFileSize(2024d)
            .withProviderId(UUID.randomUUID().toString())
            .withYear(2020)
            .withLinkUrl("http://localhost/link2");

    reportChanged =
        reportFirst.withFileName("newFileName.txt").withLinkUrl("http://localhost/changed");

    PostgresClient.setPostgresTester(new PostgresTesterContainer());
    Async async = context.async();
    int port = NetworkUtils.nextFreePort();

    RestAssured.reset();
    RestAssured.baseURI = "http://localhost";
    RestAssured.port = port;
    RestAssured.defaultParser = Parser.JSON;

    DeploymentOptions options =
        new DeploymentOptions().setConfig(new JsonObject().put("http.port", port));
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
  }

  @AfterClass
  public static void afterClass(TestContext context) {
    RestAssured.reset();
    Async async = context.async();
    vertx.close(
        context.asyncAssertSuccess(
            res -> {
              PostgresClient.stopPostgresTester();
              async.complete();
            }));
  }

  @Before
  public void before(TestContext context) {
    // clear table
    PostgresClient.getInstance(vertx, TENANT)
        .delete(Constants.TABLE_NAME_CUSTOM_REPORTS, new Criterion(), context.asyncAssertSuccess());
  }

  @Test
  public void checkThatWeCanAddGetPutAndDeleteCustomReports() {
    // POST
    assertThat(
            given(defaultHeaders)
                .body(reportFirst)
                .post(BASE_URI)
                .then()
                .statusCode(201)
                .extract()
                .as(CustomReport.class))
        .usingRecursiveComparison(ignoreMetadata)
        .isEqualTo(reportFirst);

    // GET
    assertThat(
            given(defaultHeaders)
                .get(BASE_URI + "/" + reportFirst.getId())
                .then()
                .contentType(ContentType.JSON)
                .statusCode(200)
                .extract()
                .as(CustomReport.class))
        .usingRecursiveComparison(ignoreMetadata)
        .isEqualTo(reportFirst);

    // PUT
    given(defaultHeaders)
        .body(reportChanged)
        .put(BASE_URI + "/" + reportFirst.getId())
        .then()
        .statusCode(204);

    // GET again
    assertThat(
            given(defaultHeaders)
                .get(BASE_URI + "/" + reportFirst.getId())
                .then()
                .statusCode(200)
                .extract()
                .as(CustomReport.class))
        .usingRecursiveComparison(ignoreMetadata)
        .isEqualTo(reportChanged);

    // POST second report
    assertThat(
            given(defaultHeaders)
                .body(reportSecond)
                .post(BASE_URI)
                .then()
                .statusCode(201)
                .extract()
                .as(CustomReport.class))
        .usingRecursiveComparison(ignoreMetadata)
        .isEqualTo(reportSecond);

    // GET all
    assertThat(
            given(defaultHeaders)
                .get(BASE_URI)
                .then()
                .statusCode(200)
                .extract()
                .as(CustomReports.class)
                .getCustomReports())
        .hasSize(2)
        .usingRecursiveComparison(ignoreMetadata)
        .ignoringCollectionOrder()
        .isEqualTo(List.of(reportChanged, reportSecond));

    // DELETE
    given(defaultHeaders).delete(BASE_URI + "/" + reportFirst.getId()).then().statusCode(204);

    // DELETE
    given(defaultHeaders).delete(BASE_URI + "/" + reportSecond.getId()).then().statusCode(204);

    // GET again
    given(defaultHeaders).get(BASE_URI + "/" + reportFirst.getId()).then().statusCode(404);

    // GET all again
    assertThat(
            given(defaultHeaders)
                .get(BASE_URI)
                .then()
                .extract()
                .as(CustomReports.class)
                .getCustomReports())
        .isEmpty();
  }

  @Test
  public void checkThatLinkUrlOrFileIdIsRequiredForPutAndPost() {
    CustomReport report =
        new CustomReport().withProviderId("b575e5c6-3858-44a4-838a-c7da97f0c975").withYear(2020);

    // POST with fileId succeeds
    given(defaultHeaders)
        .body(report.withFileId("377429b4-d45c-49ec-81c1-18c53c37ffb7").withLinkUrl(null))
        .post(BASE_URI)
        .then()
        .statusCode(201);

    // POST with linkUrl succeeds
    String id =
        given(defaultHeaders)
            .body(report.withFileId(null).withLinkUrl("http://localhost/link"))
            .post(BASE_URI)
            .then()
            .statusCode(201)
            .extract()
            .as(CustomReport.class)
            .getId();

    // POST without fails
    given(defaultHeaders)
        .body(report.withFileId(null).withLinkUrl(null))
        .post(BASE_URI)
        .then()
        .statusCode(422);

    // PUT with fileId succeeds
    given(defaultHeaders)
        .body(report.withFileId("a49fbdbe-1553-46cb-ad1a-40cdd200a300").withLinkUrl(null))
        .put(BASE_URI + "/" + id)
        .then()
        .statusCode(204);

    // PUT with linkUrl succeeds
    given(defaultHeaders)
        .body(report.withFileId(null).withLinkUrl("http://localhost/link2"))
        .put(BASE_URI + "/" + id)
        .then()
        .statusCode(204);

    // PUT without fails
    given(defaultHeaders)
        .body(report.withFileId(null).withLinkUrl(null))
        .put(BASE_URI + "/" + id)
        .then()
        .statusCode(422);
  }
}
