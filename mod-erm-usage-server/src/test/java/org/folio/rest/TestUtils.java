package org.folio.rest;

import io.restassured.RestAssured;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.filter.log.RequestLoggingFilter;
import io.restassured.filter.log.ResponseLoggingFilter;
import io.restassured.parsing.Parser;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import java.util.List;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.rest.client.TenantClient;
import org.folio.rest.jaxrs.model.Parameter;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.NetworkUtils;
import org.folio.rest.tools.utils.VertxUtils;

public class TestUtils {
  private static final String BASE_URI = "http://localhost";
  private static final String TENANT = "diku";
  private static final Vertx vertx = VertxUtils.getVertxFromContextOrNew();
  private static final WebClient webClient = WebClient.create(vertx);
  private static int port = -1;

  public static Vertx getVertx() {
    return vertx;
  }

  public static String getBaseURI() {
    return BASE_URI;
  }

  public static String getTenant() {
    return TENANT;
  }

  public static int getPort() {
    if (port == -1) {
      port = NetworkUtils.nextFreePort();
    }
    return port;
  }

  public static Future<HttpResponse<Buffer>> postTenant(String tenantId, boolean loadSample) {
    TenantClient tenantClient = new TenantClient(BASE_URI + ":" + port, tenantId, null, webClient);
    return tenantClient.postTenant(
        new TenantAttributes()
            .withParameters(
                List.of(
                    new Parameter()
                        .withKey("loadSample")
                        .withValue(Boolean.toString(loadSample)))));
  }

  public static Future<HttpResponse<Buffer>> deleteTenant(String tenantId, boolean purge) {
    TenantClient tenantClient = new TenantClient(BASE_URI + ":" + port, tenantId, null, webClient);
    return tenantClient.postTenant(new TenantAttributes().withModuleTo(null).withPurge(purge));
  }

  public static Future<Void> clearTable(String tenantId, String tableName) {
    PostgresClient pgClient = PostgresClient.getInstance(vertx, tenantId);
    return pgClient.delete(tableName, new Criterion()).mapEmpty();
  }

  public static void setupRestAssured(String basePath, boolean enableLogging) {
    resetRestAssured();
    RestAssured.baseURI = BASE_URI;
    RestAssured.basePath = basePath;
    RestAssured.port = port;
    RestAssured.defaultParser = Parser.JSON;
    RestAssured.requestSpecification =
        new RequestSpecBuilder().addHeader(XOkapiHeaders.TENANT, TENANT).build();

    if (enableLogging) {
      RestAssured.filters(new ResponseLoggingFilter(), new RequestLoggingFilter());
    }
  }

  public static void resetRestAssured() {
    RestAssured.reset();
  }
}
