package org.folio.rest.util;

import static org.folio.rest.util.Constants.FIELD_NAME_PROVIDER_ID;
import static org.folio.rest.util.Constants.FIELD_NAME_RELEASE;
import static org.folio.rest.util.Constants.FIELD_NAME_REPORT_NAME;
import static org.folio.rest.util.Constants.FIELD_NAME_YEAR_MONTH;
import static org.folio.rest.util.Constants.OPERATOR_EQUALS;
import static org.folio.rest.util.Constants.TABLE_NAME_COUNTER_REPORTS;
import static org.folio.rest.util.Constants.TABLE_NAME_UDP;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.folio.rest.jaxrs.model.CounterReport;
import org.folio.rest.jaxrs.model.ErrorCodes;
import org.folio.rest.jaxrs.model.ReportReleases;
import org.folio.rest.jaxrs.model.ReportTypes;
import org.folio.rest.jaxrs.model.UsageDataProvider;
import org.folio.rest.persist.Criteria.Criteria;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.PgUtil;
import org.folio.rest.persist.cql.CQLWrapper;

public class PgHelper {

  private PgHelper() {}

  public static Future<UsageDataProvider> getUDPfromDbById(
      Context vertxContext, Map<String, String> okapiHeaders, String id) {
    Promise<UsageDataProvider> udpPromise = Promise.promise();
    PgUtil.postgresClient(vertxContext, okapiHeaders)
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
            .addField(FIELD_NAME_PROVIDER_ID)
            .setJSONB(true)
            .setOperation(OPERATOR_EQUALS)
            .setVal(counterReport.getProviderId());
    Criteria releaseCrit =
        new Criteria()
            .addField(FIELD_NAME_RELEASE)
            .setOperation(OPERATOR_EQUALS)
            .setVal(counterReport.getRelease());
    Criteria reportNameCrit =
        new Criteria()
            .addField(FIELD_NAME_REPORT_NAME)
            .setOperation(OPERATOR_EQUALS)
            .setVal(counterReport.getReportName());
    Criteria yearMonthCrit =
        new Criteria()
            .addField(FIELD_NAME_YEAR_MONTH)
            .setOperation(OPERATOR_EQUALS)
            .setVal(counterReport.getYearMonth());
    Criterion criterion =
        new Criterion()
            .addCriterion(idCrit)
            .addCriterion(releaseCrit)
            .addCriterion(reportNameCrit)
            .addCriterion(yearMonthCrit);
    return new CQLWrapper(criterion);
  }

  // index: counter_reports_custom_getcsv_idx
  public static Future<String> saveCounterReportToDb(
      Context vertxContext,
      Map<String, String> okapiHeaders,
      CounterReport counterReport,
      boolean overwrite) {

    // check if CounterReport already exists
    CQLWrapper cql = createGetReportCQL(counterReport);

    Promise<String> idPromise = Promise.promise();
    PgUtil.postgresClient(vertxContext, okapiHeaders)
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
              PgUtil.postgresClient(vertxContext, okapiHeaders)
                  .upsert(TABLE_NAME_COUNTER_REPORTS, id, counterReport, upsertPromise);
              return upsertPromise.future();
            });
  }

  public static Future<List<String>> saveCounterReportsToDb(
      Context vertxContext,
      Map<String, String> okapiHeaders,
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
            okapiHeaders,
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
            @SuppressWarnings({"rawtypes", "squid:S3740"})
            List<Future> saveFutures = new ArrayList<>();
            counterReports.forEach(
                cr -> saveFutures.add(saveCounterReportToDb(vertxContext, okapiHeaders, cr, true)));

            return CompositeFuture.join(saveFutures)
                .map(cf -> cf.list().stream().map(o -> (String) o).collect(Collectors.toList()));
          }
        });
  }

  /**
   * Returns those CounterReports that are present in the database.
   *
   * @param vertxContext Vertx context
   * @param okapiHeaders okapiHeaders
   * @param providerId ProviderId
   * @param reportName Report name
   * @param release Counter release/version
   * @param yearMonths Months to check
   * @return List of CounterReport
   */
  // index: counter_reports_custom_getcsv_idx
  public static Future<List<CounterReport>> getExistingReports(
      Context vertxContext,
      Map<String, String> okapiHeaders,
      String providerId,
      String reportName,
      String release,
      List<String> yearMonths) {
    String months = String.join(",", yearMonths);

    Criteria providerCrit =
        new Criteria()
            .addField(FIELD_NAME_PROVIDER_ID)
            .setOperation(OPERATOR_EQUALS)
            .setVal(providerId);
    Criteria reportNameCrit =
        new Criteria()
            .addField(FIELD_NAME_REPORT_NAME)
            .setOperation(OPERATOR_EQUALS)
            .setVal(reportName);
    Criteria releaseCrit =
        new Criteria().addField(FIELD_NAME_RELEASE).setOperation(OPERATOR_EQUALS).setVal(release);
    Criteria yearMonthCrit =
        new Criteria()
            .addField("jsonb->" + FIELD_NAME_YEAR_MONTH)
            .setJSONB(false)
            .setOperation("?|")
            .setVal("{" + months + "}");
    Criterion criterion =
        new Criterion()
            .addCriterion(providerCrit)
            .addCriterion(reportNameCrit)
            .addCriterion(releaseCrit)
            .addCriterion(yearMonthCrit);

    // Need to CQLWrapper from Criterion cause PostgresClient sometimes returns unexpected results
    // when using just the criterion...
    CQLWrapper cql = new CQLWrapper(criterion);

    Promise<List<CounterReport>> result = Promise.promise();
    PgUtil.postgresClient(vertxContext, okapiHeaders)
        .get(
            TABLE_NAME_COUNTER_REPORTS,
            CounterReport.class,
            cql,
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

  // index: counter_reports_custom_errorcodes_idx
  public static Future<ErrorCodes> getErrorCodes(
      Context vertxContext, Map<String, String> okapiHeaders) {
    String query =
        "SELECT DISTINCT(SUBSTRING(jsonb->>'failedReason','(?:Number=|\"Code\": ?)([0-9]{1,4})'))"
            + " FROM counter_reports WHERE jsonb->>'failedReason' IS NOT NULL";
    Promise<ErrorCodes> result = Promise.promise();
    PgUtil.postgresClient(vertxContext, okapiHeaders)
        .select(
            query,
            updateResultAsyncResult -> {
              if (updateResultAsyncResult.succeeded()) {
                List<String> collect =
                    StreamSupport.stream(updateResultAsyncResult.result().spliterator(), false)
                        .map(row -> Optional.ofNullable(row.getString(0)).orElse("other"))
                        .collect(Collectors.toList());
                ErrorCodes errorCodes = new ErrorCodes().withErrorCodes(collect);
                result.complete(errorCodes);
              } else {
                result.fail(updateResultAsyncResult.cause());
              }
            });
    return result.future();
  }

  public static Future<ReportTypes> getReportTypes(
      Context vertxContext, Map<String, String> okapiHeaders) {
    String query = "SELECT DISTINCT(jsonb->>'reportName') FROM counter_reports";
    Promise<ReportTypes> result = Promise.promise();
    PgUtil.postgresClient(vertxContext, okapiHeaders)
        .select(
            query,
            ar -> {
              if (ar.succeeded()) {
                List<String> collect =
                    StreamSupport.stream(ar.result().spliterator(), false)
                        .map(row -> row.getString(0))
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
                ReportTypes reportTypes = new ReportTypes().withReportTypes(collect);
                result.complete(reportTypes);
              } else {
                result.fail(ar.cause());
              }
            });
    return result.future();
  }

  public static Future<ReportReleases> getReportReleases(
      Context vertxContext, Map<String, String> okapiHeaders) {
    String query =
        "SELECT DISTINCT(jsonb->>'release') FROM counter_reports ORDER BY jsonb->>'release'";
    Promise<ReportReleases> result = Promise.promise();
    PgUtil.postgresClient(vertxContext, okapiHeaders)
        .select(
            query,
            ar -> {
              if (ar.succeeded()) {
                List<String> collect =
                    StreamSupport.stream(ar.result().spliterator(), false)
                        .map(row -> row.getString(0))
                        .filter(Objects::nonNull)
                        .toList();
                ReportReleases reportReleases = new ReportReleases().withReportReleases(collect);
                result.complete(reportReleases);
              } else {
                result.fail(ar.cause());
              }
            });
    return result.future();
  }
}
