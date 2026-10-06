package org.folio.rest.impl3;

import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import io.restassured.http.ContentType;
import io.restassured.response.Response;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.rest.Setup;
import org.folio.rest.SetupTenant;
import org.folio.rest.TestUtils;
import org.folio.rest.impl.TenantAPI;
import org.folio.rest.jaxrs.model.AggregatorSetting;
import org.folio.rest.jaxrs.model.HarvestingConfig;
import org.folio.rest.jaxrs.model.HarvestingConfig.HarvestingStatus;
import org.folio.rest.jaxrs.model.SushiConfig;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.jaxrs.model.UsageDataProvider;
import org.folio.rest.jaxrs.model.UsageDataProvider.HasFailedReport;
import org.folio.rest.jaxrs.model.UsageDataProvider.Status;
import org.folio.rest.tools.utils.ModuleName;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Setup
@SetupTenant
@Timeout(10)
class UsageDataProvidersIT {

  private static final String APPLICATION_JSON = "application/json";
  private static final String BASE_URI = "/usage-data-providers";
  private static final String AGGREGATOR_PATH = "/aggregator-settings";
  private static final String TENANT = TestUtils.getTenant();
  private static final String CONSTRAINT_VIOLATION =
      "violates check constraint \"usage_data_providers_harvestingstatus_constraint\"";
  private static UsageDataProvider udProvider;
  private static UsageDataProvider udProvider2;
  private static UsageDataProvider udProviderChanged;
  private static UsageDataProvider udProviderInvalid;
  private static AggregatorSetting aggregator;

  @BeforeAll
  static void beforeAll() throws IOException {
    // setup sample data
    var udProvider2Str =
        new String(Files.readAllBytes(Paths.get("../ramls/examples/udproviders2.sample")));
    udProvider2 = Json.decodeValue(udProvider2Str, UsageDataProvider.class);
    var sushiCredentials = udProvider2.getSushiCredentials();

    var aggregatorStr =
        new String(Files.readAllBytes(Paths.get("../ramls/examples/aggregatorsettings.sample")));
    aggregator = Json.decodeValue(aggregatorStr, AggregatorSetting.class);
    var udProviderStr =
        new String(Files.readAllBytes(Paths.get("../ramls/examples/udproviders.sample")));
    udProvider = Json.decodeValue(udProviderStr, UsageDataProvider.class);
    udProviderChanged =
        Json.decodeValue(udProviderStr, UsageDataProvider.class)
            .withLabel("CHANGED")
            .withSushiCredentials(sushiCredentials.withRequestorMail("CHANGED@ub.uni-leipzig.de"));
    udProviderInvalid = Json.decodeValue(udProviderStr, UsageDataProvider.class).withLabel(null);

    TestUtils.setupRestAssured("", false);
  }

  @AfterAll
  static void afterAll() {
    TestUtils.resetRestAssured();
  }

  @Test
  void checkThatWeCanAddAProviderWithAggregatorSettings() {
    // POST aggregator
    given()
        .body(Json.encode(aggregator))
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", APPLICATION_JSON)
        .post(AGGREGATOR_PATH)
        .then()
        .statusCode(201);

    // POST provider
    given()
        .body(Json.encode(udProvider2))
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", APPLICATION_JSON)
        .post(BASE_URI)
        .then()
        .statusCode(201);

    // GET provider && check if aggregator name got resolved
    given()
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", APPLICATION_JSON)
        .get(BASE_URI + "/" + udProvider2.getId())
        .then()
        .statusCode(200)
        .body("id", equalTo(udProvider2.getId()))
        .body("label", equalTo(udProvider2.getLabel()))
        .body("harvestingConfig.aggregator.name", equalTo(aggregator.getLabel()));

    // DELETE provider
    given()
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", "text/plain")
        .delete(BASE_URI + "/" + udProvider2.getId())
        .then()
        .statusCode(204);

    // DELETE aggregator
    given()
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", "text/plain")
        .delete(AGGREGATOR_PATH + "/" + aggregator.getId())
        .then()
        .statusCode(204);
  }

  @Test
  void checkThatWeCanAddGetPutAndDeleteUsageDataProviders() {
    // POST provider without aggregator
    given()
        .body(Json.encode(udProvider))
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", APPLICATION_JSON)
        .request()
        .post(BASE_URI)
        .then()
        .statusCode(201)
        .body("id", equalTo(udProvider.getId()))
        .body("label", equalTo(udProvider.getLabel()));

    // GET
    UsageDataProvider udproviderResult =
        given()
            .header("X-Okapi-Tenant", TENANT)
            .header("content-type", APPLICATION_JSON)
            .header("accept", APPLICATION_JSON)
            .when()
            .get(BASE_URI + "/" + udProvider.getId())
            .then()
            .contentType(ContentType.JSON)
            .statusCode(200)
            .extract()
            .as(UsageDataProvider.class);
    assertThat(udproviderResult)
        .usingRecursiveComparison()
        .ignoringFields("metadata")
        .isEqualTo(udProvider);

    // PUT
    given()
        .body(Json.encode(udProviderChanged))
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", "text/plain")
        .request()
        .put(BASE_URI + "/" + udProviderChanged.getId())
        .then()
        .statusCode(204);

    // GET again
    UsageDataProvider udproviderChangedResult =
        given()
            .header("X-Okapi-Tenant", TENANT)
            .header("content-type", APPLICATION_JSON)
            .header("accept", APPLICATION_JSON)
            .request()
            .get(BASE_URI + "/" + udProviderChanged.getId())
            .then()
            .statusCode(200)
            .extract()
            .as(UsageDataProvider.class);
    assertThat(udproviderChangedResult)
        .usingRecursiveComparison()
        .ignoringFields("metadata")
        .isEqualTo(udProviderChanged);

    // DELETE
    given()
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", "text/plain")
        .when()
        .delete(BASE_URI + "/" + udProviderChanged.getId())
        .then()
        .statusCode(204);

    // GET again
    given()
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", APPLICATION_JSON)
        .when()
        .get(BASE_URI + "/" + udProviderChanged.getId())
        .then()
        .statusCode(404);
  }

  @Test
  void checkThatWeCanSearchByCQL() {
    var udProviders = List.of(udProvider, udProvider2);

    // POST two providers
    udProviders.forEach(this::postUdp);

    // GET by CQL: search for label
    get("label=\"" + udProvider.getLabel() + "\"")
        .then()
        .statusCode(200)
        .body("usageDataProviders.label", is(List.of(udProvider.getLabel())))
        .body("usageDataProviders.id", is(List.of(udProvider.getId())));

    // GET by CQL: search for a word from aggregator name, description, and label
    get("keywords all \"digital meeting with\"")
        .then()
        .statusCode(200)
        .body("usageDataProviders.id", is(List.of(udProvider2.getId())));

    // DELETE
    udProviders.forEach(
        udp ->
            given()
                .header("X-Okapi-Tenant", TENANT)
                .header("content-type", APPLICATION_JSON)
                .header("accept", "text/plain")
                .when()
                .delete(BASE_URI + "/" + udp.getId())
                .then()
                .statusCode(204));
  }

  @Test
  void checkThatInvalidUsageDataProviderIsNotPosted() {
    given()
        .body(udProviderInvalid)
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", APPLICATION_JSON)
        .request()
        .post(BASE_URI)
        .then()
        .statusCode(422);
  }

  @Test
  void checkThatDefaultValueForHasFailedReportsIsNo() {
    UsageDataProvider udp =
        given()
            .body(udProvider)
            .header("X-Okapi-Tenant", TENANT)
            .header("content-type", APPLICATION_JSON)
            .header("accept", APPLICATION_JSON)
            .request()
            .post(BASE_URI)
            .then()
            .statusCode(201)
            .extract()
            .as(UsageDataProvider.class);

    assertThat(udp.getHasFailedReport()).isEqualTo(HasFailedReport.NO);

    given()
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", "text/plain")
        .when()
        .delete(BASE_URI + "/" + udProvider.getId())
        .then()
        .statusCode(204);
  }

  @Test
  void checkThatWeCantPostInactiveProviderWithActiveHarvestingStatus() {
    UsageDataProvider inactiveProvider =
        new UsageDataProvider()
            .withLabel("Inactive Provider")
            .withStatus(Status.INACTIVE)
            .withHarvestingConfig(
                new HarvestingConfig().withHarvestingStatus(HarvestingStatus.ACTIVE));

    String body = postEntity(inactiveProvider).then().statusCode(500).extract().asString();
    assertThat(body).contains(CONSTRAINT_VIOLATION);
  }

  @Test
  void checkThatWeCantPutInactiveProviderWithActiveHarvestingStatus() {
    UsageDataProvider provider =
        postEntity(udProvider).then().statusCode(201).extract().as(UsageDataProvider.class);

    String body =
        putEntity(provider.withStatus(Status.INACTIVE)).then().statusCode(500).extract().asString();
    assertThat(body).contains(CONSTRAINT_VIOLATION);

    deleteEntity(provider).then().statusCode(204);
  }

  /** Tests backward compatibility with previous versions */
  @Test
  void checkThatWeCanHandleProviderWithoutStatus() {
    UsageDataProvider providerWithoutStatus =
        new UsageDataProvider()
            .withLabel("Provider without status")
            .withHarvestingConfig(
                new HarvestingConfig().withHarvestingStatus(HarvestingStatus.ACTIVE));

    // POST without status
    UsageDataProvider savedProvider =
        postEntity(providerWithoutStatus)
            .then()
            .statusCode(201)
            .extract()
            .as(UsageDataProvider.class);
    assertThat(savedProvider.getStatus()).isEqualTo(Status.ACTIVE);

    // PUT without status
    putEntity(savedProvider.withStatus(null)).then().statusCode(204);
    UsageDataProvider changedProvider =
        getEntityById(savedProvider.getId())
            .then()
            .statusCode(200)
            .extract()
            .as(UsageDataProvider.class);
    assertThat(changedProvider.getStatus()).isEqualTo(Status.ACTIVE);

    // DELETE
    deleteEntity(savedProvider).then().statusCode(204);
  }

  @Test
  void checkThatWeGetServiceTypesAndCanFilterByServiceType() {
    List<UsageDataProvider> udps = new ArrayList<>();
    try {
      Stream.of("cs51", "cs50", "cs51", "", null)
          .map(
              serviceType ->
                  new UsageDataProvider()
                      .withLabel("ServiceTypeFilter " + serviceType)
                      .withHarvestingConfig(
                          new HarvestingConfig()
                              .withHarvestingStatus(HarvestingStatus.INACTIVE)
                              .withSushiConfig(new SushiConfig().withServiceType(serviceType))))
          .forEach(
              udp ->
                  udps.add(
                      postEntity(udp)
                          .then()
                          .statusCode(201)
                          .extract()
                          .as(UsageDataProvider.class)));
      // provider without sushiConfig at all
      udps.add(
          postEntity(
                  new UsageDataProvider()
                      .withLabel("ServiceTypeFilter without sushiConfig")
                      .withHarvestingConfig(
                          new HarvestingConfig().withHarvestingStatus(HarvestingStatus.INACTIVE)))
              .then()
              .statusCode(201)
              .extract()
              .as(UsageDataProvider.class));

      List<String> serviceTypes =
          getServiceTypes().then().statusCode(200).extract().jsonPath().getList("serviceTypes");
      assertThat(serviceTypes).containsExactly("cs50", "cs51");

      // filter as sent by the UI service type filter, restricted to the providers of this test
      String ownProviders = "label=\"ServiceTypeFilter*\" and ";
      get(ownProviders + "harvestingConfig.sushiConfig.serviceType=(\"cs50\" or \"cs51\")")
          .then()
          .statusCode(200)
          .body(
              "usageDataProviders.id",
              containsInAnyOrder(udps.get(0).getId(), udps.get(1).getId(), udps.get(2).getId()));
      get(ownProviders + "harvestingConfig.sushiConfig.serviceType=(\"cs50\")")
          .then()
          .statusCode(200)
          .body("usageDataProviders.id", is(List.of(udps.get(1).getId())));
      // missing or empty service type
      get(ownProviders
              + "((cql.allRecords=1 NOT harvestingConfig.sushiConfig.serviceType=\"\")"
              + " or harvestingConfig.sushiConfig.serviceType==\"\")")
          .then()
          .statusCode(200)
          .body(
              "usageDataProviders.id",
              containsInAnyOrder(udps.get(3).getId(), udps.get(4).getId(), udps.get(5).getId()));
    } finally {
      udps.forEach(udp -> deleteEntity(udp).then().statusCode(204));
    }

    getServiceTypes().then().statusCode(200).body("serviceTypes", is(List.of()));
  }

  @Test
  void checkThatUpgradeSetsHarvestingStatusOfCs41ProvidersToInactive()
      throws ExecutionException, InterruptedException {
    var udProvider41Active =
        new UsageDataProvider()
            .withLabel("Migration cs41 active")
            .withHarvestingConfig(
                new HarvestingConfig()
                    .withHarvestingStatus(HarvestingStatus.ACTIVE)
                    .withHarvestVia(HarvestingConfig.HarvestVia.SUSHI)
                    .withSushiConfig(
                        new SushiConfig()
                            .withServiceType("cs41")
                            .withServiceUrl("http://localhost/sushi"))
                    .withReportRelease("4")
                    .withRequestedReports(List.of("JR1", "BR1")));
    var udProvider50Active =
        new UsageDataProvider()
            .withLabel("Migration cs50 active")
            .withHarvestingConfig(
                new HarvestingConfig()
                    .withHarvestingStatus(HarvestingStatus.ACTIVE)
                    .withSushiConfig(new SushiConfig().withServiceType("cs50")));
    var udProvider41Inactive =
        new UsageDataProvider()
            .withLabel("Migration cs41 inactive")
            .withHarvestingConfig(
                new HarvestingConfig()
                    .withHarvestingStatus(HarvestingStatus.INACTIVE)
                    .withSushiConfig(new SushiConfig().withServiceType("cs41")));
    var udps =
        Stream.of(udProvider41Active, udProvider50Active, udProvider41Inactive)
            .map(
                udp -> postEntity(udp).then().statusCode(201).extract().as(UsageDataProvider.class))
            .toList();

    var moduleFrom = ModuleName.getModuleName() + "-5.2.0";
    var moduleTo = ModuleName.getModuleName() + "-" + ModuleName.getModuleVersion();

    try {
      var before = udps.stream().map(udp -> getEntityJson(udp.getId())).toList();
      var resp =
          new TenantAPI()
              .postTenantSync(
                  new TenantAttributes().withModuleFrom(moduleFrom).withModuleTo(moduleTo),
                  Map.of(XOkapiHeaders.TENANT, TENANT),
                  TestUtils.getVertx().getOrCreateContext())
              .toCompletionStage()
              .toCompletableFuture()
              .get();
      assertThat(resp.getStatus()).isEqualTo(204);

      var after = udps.stream().map(udp -> getEntityJson(udp.getId())).toList();
      // cs41 and active: only harvestingStatus changes
      JsonObject expected = before.getFirst().copy();
      expected.getJsonObject("harvestingConfig").put("harvestingStatus", "inactive");
      assertThat(after.get(0)).isEqualTo(expected);
      // not cs41 or already inactive: unchanged
      assertThat(after.get(1)).isEqualTo(before.get(1));
      assertThat(after.get(2)).isEqualTo(before.get(2));
    } finally {
      udps.forEach(udp -> deleteEntity(udp).then().statusCode(204));
    }
  }

  @Test
  void checkThatGetServiceTypesReturns500OnDatabaseError() {
    // tenant without schema, so the query fails
    getServiceTypes("notenant").then().statusCode(500).contentType(ContentType.TEXT);
  }

  private Response getServiceTypes() {
    return getServiceTypes(TENANT);
  }

  private Response getServiceTypes(String tenant) {
    return given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header("accept", "application/json, text/plain")
        .get(BASE_URI + "/sushi-config/service-types");
  }

  private void postUdp(UsageDataProvider udProvider) {
    UsageDataProvider udp =
        given()
            .body(Json.encode(udProvider))
            .header("X-Okapi-Tenant", TENANT)
            .header("content-type", APPLICATION_JSON)
            .header("accept", APPLICATION_JSON)
            .request()
            .post(BASE_URI)
            .thenReturn()
            .as(UsageDataProvider.class);
    assertThat(udp.getLabel()).isEqualTo(udProvider.getLabel());
    assertThat(udp.getId()).isNotEmpty();
  }

  private Response get(String cql) {
    return given()
        .header("X-Okapi-Tenant", TENANT)
        .header("content-type", APPLICATION_JSON)
        .header("accept", APPLICATION_JSON)
        .when()
        .param("query", cql)
        .get(BASE_URI);
  }

  private Response postEntity(Object entity) {
    return given()
        .body(entity)
        .header(XOkapiHeaders.TENANT, TENANT)
        .header(CONTENT_TYPE, APPLICATION_JSON)
        .post(BASE_URI);
  }

  private Response putEntity(Object entity) {
    JsonObject jsonObject = JsonObject.mapFrom(entity);
    String id = jsonObject.getString("id");
    return given()
        .body(entity)
        .header(XOkapiHeaders.TENANT, TENANT)
        .header(CONTENT_TYPE, APPLICATION_JSON)
        .put(BASE_URI + "/{id}", id);
  }

  private Response getEntityById(String id) {
    return given().header(XOkapiHeaders.TENANT, TENANT).get(BASE_URI + "/{id}", id);
  }

  private JsonObject getEntityJson(String id) {
    return new JsonObject(getEntityById(id).then().statusCode(200).extract().asString());
  }

  private Response deleteEntity(Object entity) {
    JsonObject jsonObject = JsonObject.mapFrom(entity);
    String id = jsonObject.getString("id");
    return given().body(entity).header(XOkapiHeaders.TENANT, TENANT).delete(BASE_URI + "/{id}", id);
  }
}
