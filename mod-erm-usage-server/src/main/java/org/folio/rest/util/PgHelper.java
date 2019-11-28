package org.folio.rest.util;

import io.vertx.core.*;
import org.folio.rest.jaxrs.model.CounterReport;
import org.folio.rest.jaxrs.model.UsageDataProvider;
import org.folio.rest.persist.Criteria.Criteria;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.cql.CQLWrapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.folio.rest.util.Constants.TABLE_NAME_COUNTER_REPORTS;
import static org.folio.rest.util.Constants.TABLE_NAME_UDP;

public class PgHelper {

  private static final String EQUALS = "=";

  private PgHelper() {}

  public static Future<UsageDataProvider> getUDPfromDbById(
      Vertx vertx, String tenantId, String id) {
    Promise<UsageDataProvider> udpPromise = Promise.promise();
    PostgresClient.getInstance(vertx, tenantId)
        .getById(
            TABLE_NAME_UDP,
            id,
            UsageDataProvider.class,
            ar -> {
              if (ar.succeeded()) {
                if (ar.result() != null) {
                  udpPromise.complete(ar.result());
                } else {
                  udpPromise.fail(String.format("Provider with id %s not found", id));
                }
              } else {
                udpPromise.fail(
                    String.format(
                        "Unable to get usage data provider with id %s: %s", id, ar.cause()));
              }
            });
    return udpPromise.future();
  }

  private static CQLWrapper createGetReportCQL(CounterReport counterReport) {
    Criteria idCrit =
        new Criteria()
            .addField(Constants.FIELD_NAME_PROVIDER_ID)
            .setJSONB(true)
            .setOperation(EQUALS)
            .setVal(counterReport.getProviderId());
    Criteria releaseCrit =
        new Criteria()
            .addField(Constants.FIELD_NAME_RELEASE)
            .setOperation(EQUALS)
            .setVal(counterReport.getRelease());
    Criteria reportNameCrit =
        new Criteria()
            .addField(Constants.FIELD_NAME_REPORT_NAME)
            .setOperation(EQUALS)
            .setVal(counterReport.getReportName());
    Criteria yearMonthCrit =
        new Criteria()
            .addField(Constants.FIELD_NAME_YEAR_MONTH)
            .setOperation(EQUALS)
            .setVal(counterReport.getYearMonth());
    Criterion criterion =
        new Criterion()
            .addCriterion(idCrit)
            .addCriterion(releaseCrit)
            .addCriterion(reportNameCrit)
            .addCriterion(yearMonthCrit);
    return new CQLWrapper(criterion);
  }

  public static Future<String> saveCounterReportToDb(
      Vertx vertx, String tenantId, CounterReport counterReport, boolean overwrite) {

    // check if CounterReport already exists
    CQLWrapper cql = createGetReportCQL(counterReport);

    Promise<String> idPromise = Promise.promise();
    PostgresClient.getInstance(vertx, tenantId)
        .get(
            TABLE_NAME_COUNTER_REPORTS,
            CounterReport.class,
            cql,
            false,
            h -> {
              if (h.succeeded()) {
                int resultSize = h.result().getResults().size();
                if (resultSize == 1) {
                  idPromise.complete(h.result().getResults().get(0).getId());
                } else if (resultSize > 1) {
                  idPromise.fail("Too many results");
                } else {
                  idPromise.complete(null);
                }
              } else {
                idPromise.fail(h.cause());
              }
            });

    // save report
    return idPromise
        .future()
        .compose(
            id -> {
              if (id != null && !overwrite) {
                return Future.failedFuture("Report already exists");
              }

              Promise<String> upsertPromise = Promise.promise();
              PostgresClient.getInstance(vertx, tenantId)
                  .upsert(TABLE_NAME_COUNTER_REPORTS, id, counterReport, upsertPromise);
              return upsertPromise.future();
            });
  }

  public static Future<List<String>> saveCounterReportsToDb(
      Context vertxContext,
      String tenantId,
      List<CounterReport> counterReports,
      boolean overwrite) {

    // check required attributes for equality
    long count =
        counterReports.stream()
            .map(cr -> Arrays.asList(cr.getReportName(), cr.getRelease(), cr.getProviderId()))
            .distinct()
            .count();
    if (count != 1) {
      return Future.failedFuture(
          "Report attributes 'reportName', 'release' and 'providerId' are not equal.");
    }

    String providerId = counterReports.get(0).getProviderId();
    String release = counterReports.get(0).getRelease();
    String reportName = counterReports.get(0).getReportName();

    Future<List<CounterReport>> existingReports =
        PgHelper.getExistingReports(
            vertxContext,
            tenantId,
            providerId,
            reportName,
            release,
            counterReports.stream().map(CounterReport::getYearMonth).collect(Collectors.toList()));

    return existingReports.compose(
        existingList -> {
          if (!overwrite && !existingList.isEmpty()) {
            return Future.failedFuture(
                "Report already existing for months: "
                    + existingList.stream()
                        .map(CounterReport::getYearMonth)
                        .collect(Collectors.joining(", ")));
          } else {
            List<Future> saveFutures = new ArrayList<>();
            counterReports.forEach(
                cr ->
                    saveFutures.add(
                        saveCounterReportToDb(vertxContext.owner(), tenantId, cr, true)));

            return CompositeFuture.join(saveFutures)
                .map(cf -> cf.list().stream().map(o -> (String) o).collect(Collectors.toList()));
          }
        });
  }

  /**
   * Returns those CounterReports that are present in the database.
   *
   * @param vertxContext Vertx context
   * @param tenantId Tenant
   * @param providerId ProviderId
   * @param reportName Report name
   * @param release Counter release/version
   * @param yearMonths Months to check
   * @return List of CounterReport
   */
  public static Future<List<CounterReport>> getExistingReports(
      Context vertxContext,
      String tenantId,
      String providerId,
      String reportName,
      String release,
      List<String> yearMonths) {
    String months = yearMonths.stream().map(ym -> "'" + ym + "'").collect(Collectors.joining(","));
    String where =
        String.format(
            " WHERE (jsonb->>'providerId' = '%s') AND "
                + "(jsonb->>'reportName' = '%s') AND "
                + "(jsonb->>'release' = '%s') AND "
                + "(jsonb->'yearMonth' ?| array[%s])",
            providerId, reportName, release, months);

    Promise<List<CounterReport>> result = Promise.promise();
    PostgresClient.getInstance(vertxContext.owner(), tenantId)
        .get(
            TABLE_NAME_COUNTER_REPORTS,
            CounterReport.class,
            where,
            false,
            false,
            ar -> {
              if (ar.succeeded()) {
                result.complete(ar.result().getResults());
              } else {
                result.tryFail(ar.cause());
              }
            });
    return result.future();
  }
}
