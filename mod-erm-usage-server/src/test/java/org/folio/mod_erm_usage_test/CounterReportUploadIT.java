package org.folio.mod_erm_usage_test;

import static io.restassured.RestAssured.get;
import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsString;
import java.io.File;
import java.nio.charset.StandardCharsets;
import javax.xml.bind.JAXB;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.rest.RestVerticle;
import org.folio.rest.client.TenantClient;
import org.folio.rest.jaxrs.model.CounterReport;
import org.folio.rest.jaxrs.model.CounterReports;
import org.folio.rest.jaxrs.model.UsageDataProvider;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.tools.utils.NetworkUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.niso.schemas.counter.Report;
import org.niso.schemas.sushi.counter.CounterReportResponse;
import org.olf.erm.usage.counter41.Counter4Utils;
import com.google.common.io.Resources;
import com.google.common.net.HttpHeaders;
import io.restassured.RestAssured;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.filter.log.RequestLoggingFilter;
import io.restassured.filter.log.ResponseLoggingFilter;
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

@RunWith(VertxUnitRunner.class)
public class CounterReportUploadIT {

  private static final String TENANT = "diku";
  private static Vertx vertx;

  private static final String PROVIDER_ID = "4b659cb9-e4bb-493d-ae30-5f5690c54802";
  private static final String PROVIDER_ID2 = "4b659cb9-e4bb-493d-ae30-5f5690c54803";

  private static final File FILE_REPORT_OK =
      new File(Resources.getResource("fileupload/reportJSTOR.xml").getFile());
  private static final File FILE_REPORT_NSS_OK =
      new File(Resources.getResource("fileupload/reportNSS.xml").getFile());
  private static final File FILE_NO_REPORT =
      new File(Resources.getResource("fileupload/noreport.txt").getFile());
  private static final File FILE_REPORT_MULTI =
      new File(Resources.getResource("fileupload/reportJSTORMultiMonth.xml").getFile());

  @Rule public Timeout timeout = Timeout.seconds(10);

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

    Async async = context.async();
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
    vertx.deployVerticle(
        RestVerticle.class.getName(),
        options,
        res -> {
          try {
            tenantClient.postTenant(
                null,
                res2 -> {
                  postProvider(context);
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

  @Before
  public void before(TestContext ctx) {
    Async async = ctx.async();
    PostgresClient.getInstance(vertx, TENANT)
        .delete(
            "counter_reports",
            new Criterion(),
            ar -> {
              if (ar.failed()) ctx.fail(ar.cause());
              async.complete();
            });
    async.await();

    testThatDBIsEmpty();
  }

  private static void postProvider(TestContext ctx) {
    String str;
    try {
      str =
          Resources.toString(
              Resources.getResource("fileupload/provider.json"), StandardCharsets.UTF_8);
    } catch (Exception e) {
      ctx.fail(e);
      return;
    }

    Async async = ctx.async();
    PostgresClient.getInstance(vertx, TENANT)
        .save(
            "usage_data_providers",
            PROVIDER_ID,
            Json.decodeValue(str, UsageDataProvider.class).withId(null),
            ar -> {
              if (ar.succeeded()) {
                async.complete();
              } else {
                ctx.fail(ar.cause());
              }
            });
    async.await();
  }

  private void testThatDBSizeIsSize(int i) {
    int size =
        get("/counter-reports")
            .then()
            .statusCode(200)
            .extract()
            .as(CounterReports.class)
            .getCounterReports()
            .size();
    assertThat(size).isEqualTo(i);
  }

  private void testThatDBIsEmpty() {
    testThatDBSizeIsSize(0);
  }

  @Test
  public void testReportR4Ok() throws Exception {
    String savedReportId =
        given()
            .header(HttpHeaders.CONTENT_TYPE, ContentType.BINARY)
            .body(FILE_REPORT_OK)
            .post("/counter-reports/upload/provider/" + PROVIDER_ID)
            .then()
            .statusCode(200)
            .body(containsString("Saved report"))
            .extract()
            .asString()
            .replace("Saved report with id ", "");

    CounterReport savedReport =
        given().get("/counter-reports/" + savedReportId).then().extract().as(CounterReport.class);
    assertThat(savedReport.getProviderId()).isEqualTo(PROVIDER_ID);
    assertThat(savedReport.getRelease()).isEqualTo("4");
    assertThat(savedReport.getYearMonth()).isEqualTo("2018-03");
    assertThat(savedReport.getDownloadTime()).isNotNull();
    assertThat(savedReport.getReportName()).isEqualTo("JR1");

    Report reportFromXML = JAXB.unmarshal(FILE_REPORT_OK, Report.class);
    Report reportFromDB = Counter4Utils.fromJSON(Json.encode(savedReport.getReport()));
    assertThat(reportFromXML).isEqualToComparingFieldByFieldRecursively(reportFromDB);
  }

  @Test
  public void testReportR4OkOverwriteTrue() {
    String savedReportId =
        given()
            .header(HttpHeaders.CONTENT_TYPE, ContentType.BINARY)
            .body(FILE_REPORT_OK)
            .queryParam("overwrite", true)
            .post("/counter-reports/upload/provider/" + PROVIDER_ID)
            .then()
            .statusCode(200)
            .extract()
            .asString()
            .replace("Saved report with id ", "");

    String overwriteReportId =
        given()
            .header(HttpHeaders.CONTENT_TYPE, ContentType.BINARY)
            .body(FILE_REPORT_OK)
            .queryParam("overwrite", true)
            .post("/counter-reports/upload/provider/" + PROVIDER_ID)
            .then()
            .statusCode(200)
            .extract()
            .asString()
            .replace("Saved report with id ", "");
    assertThat(savedReportId).isEqualTo(overwriteReportId);
    testThatDBSizeIsSize(1);
  }

  @Test
  public void testReportR4OkOverwriteFalse() {
    given()
        .header(HttpHeaders.CONTENT_TYPE, ContentType.BINARY)
        .body(FILE_REPORT_OK)
        .queryParam("overwrite", false)
        .post("/counter-reports/upload/provider/" + PROVIDER_ID)
        .then()
        .statusCode(200);

    given()
        .header(HttpHeaders.CONTENT_TYPE, ContentType.BINARY)
        .body(FILE_REPORT_OK)
        .queryParam("overwrite", false)
        .post("/counter-reports/upload/provider/" + PROVIDER_ID)
        .then()
        .statusCode(500)
        .body(containsString("Report already exists"));
  }

  @Test
  public void testReportR4NssOk() {
    String savedReportId =
        given()
            .header(HttpHeaders.CONTENT_TYPE, ContentType.BINARY)
            .body(FILE_REPORT_NSS_OK)
            .post("/counter-reports/upload/provider/" + PROVIDER_ID)
            .then()
            .statusCode(200)
            .body(containsString("Saved report"))
            .extract()
            .asString()
            .replace("Saved report with id ", "");

    CounterReport savedReport =
        given().get("/counter-reports/" + savedReportId).then().extract().as(CounterReport.class);
    assertThat(savedReport.getProviderId()).isEqualTo(PROVIDER_ID);
    assertThat(savedReport.getRelease()).isEqualTo("4");
    assertThat(savedReport.getYearMonth()).isEqualTo("2018-01");
    assertThat(savedReport.getDownloadTime()).isNotNull();
    assertThat(savedReport.getReportName()).isEqualTo("JR1");

    Report reportFromXML =
        JAXB.unmarshal(FILE_REPORT_NSS_OK, CounterReportResponse.class)
            .getReport()
            .getReport()
            .get(0);
    Report reportFromDB = Counter4Utils.fromJSON(Json.encode(savedReport.getReport()));
    assertThat(reportFromXML).isEqualToComparingFieldByFieldRecursively(reportFromDB);
  }

  @Test
  public void testReportInvalidContent() {
    given()
        .header(HttpHeaders.CONTENT_TYPE, ContentType.BINARY)
        .body(FILE_NO_REPORT)
        .post("/counter-reports/upload/provider/" + PROVIDER_ID)
        .then()
        .statusCode(500)
        .body(containsString("Wrong format"));
  }

  @Test
  public void testReportMultipleMonths() {
    given()
        .header(HttpHeaders.CONTENT_TYPE, ContentType.BINARY)
        .body(FILE_REPORT_MULTI)
        .post("/counter-reports/upload/provider/" + PROVIDER_ID)
        .then()
        .statusCode(500)
        .body(containsString("exactly one month"));
  }

  @Test
  public void testProviderNotFound() {
    given()
        .header(HttpHeaders.CONTENT_TYPE, ContentType.BINARY)
        .body(FILE_REPORT_OK)
        .post("/counter-reports/upload/provider/" + PROVIDER_ID2)
        .then()
        .statusCode(500)
        .body(containsString(PROVIDER_ID2), containsString("not found"));
  }
}
