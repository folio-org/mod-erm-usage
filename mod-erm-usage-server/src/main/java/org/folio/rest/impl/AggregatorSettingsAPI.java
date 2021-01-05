package org.folio.rest.impl;

import static io.vertx.core.Future.succeededFuture;
import static org.folio.rest.util.Constants.TABLE_NAME_UDP;
import static org.folio.rest.util.VertxUtil.executeBlocking;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.google.common.io.ByteStreams;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.ws.rs.core.Response;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.cql2pgjson.CQL2PgJSON;
import org.folio.cql2pgjson.exception.FieldException;
import org.folio.rest.annotations.Validate;
import org.folio.rest.jaxrs.model.AggregatorSetting;
import org.folio.rest.jaxrs.model.AggregatorSettings;
import org.folio.rest.jaxrs.model.AggregatorSettingsGetOrder;
import org.folio.rest.jaxrs.model.UsageDataProvider;
import org.folio.rest.persist.Criteria.Criteria;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.PgUtil;
import org.folio.rest.persist.cql.CQLWrapper;
import org.folio.rest.tools.utils.BinaryOutStream;
import org.folio.rest.tools.utils.ValidationHelper;
import org.folio.rest.util.Constants;
import org.folio.rest.util.ExportObject;
import org.olf.erm.usage.counter.common.ExcelUtil;

public class AggregatorSettingsAPI implements org.folio.rest.jaxrs.resource.AggregatorSettings {

  private static final String TABLE_NAME_AGGREGATOR_SETTINGS = "aggregator_settings";
  private static final List<String> SUPPORTED_FORMATS = Arrays.asList("csv", "xlsx");
  private static final String UNSUPPORTED_MSG = "Requested format \"%s\" is not supported.";

  private final Logger logger = LogManager.getLogger(AggregatorSettingsAPI.class);

  public static String getCredentialsCSV(List<UsageDataProvider> udps)
      throws JsonProcessingException {
    List<ExportObject> exportObjectList =
        udps.stream().map(ExportObject::new).collect(Collectors.toList());
    CsvMapper csvMapper = new CsvMapper();
    return csvMapper
        .writerFor(ExportObject.class)
        .with(csvMapper.schemaFor(ExportObject.class).withHeader())
        .forType(List.class)
        .writeValueAsString(exportObjectList);
  }

  private CQLWrapper getCQL(String query, int limit, int offset) throws FieldException {
    return new CQLWrapper(
        new CQL2PgJSON(TABLE_NAME_AGGREGATOR_SETTINGS + ".jsonb"), query, limit, offset);
  }

  @Override
  @Validate
  public void getAggregatorSettings(
      String query,
      String orderBy,
      AggregatorSettingsGetOrder order,
      int offset,
      int limit,
      String lang,
      Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {
    logger.debug("Getting aggregator settings");
    logger.debug("Headers present are: {}", okapiHeaders::toString);

    CQLWrapper cql;
    try {
      cql = getCQL(query, limit, offset);
    } catch (Exception e) {
      ValidationHelper.handleError(e, asyncResultHandler);
      return;
    }

    PgUtil.postgresClient(vertxContext, okapiHeaders)
        .get(
            TABLE_NAME_AGGREGATOR_SETTINGS,
            AggregatorSetting.class,
            cql,
            true,
            reply -> {
              if (reply.succeeded()) {
                AggregatorSettings aggregatorSettings = new AggregatorSettings();
                List<AggregatorSetting> aggregatorSettingList = reply.result().getResults();
                aggregatorSettings.setAggregatorSettings(aggregatorSettingList);
                aggregatorSettings.setTotalRecords(
                    reply.result().getResultInfo().getTotalRecords());
                asyncResultHandler.handle(
                    succeededFuture(
                        GetAggregatorSettingsResponse.respond200WithApplicationJson(
                            aggregatorSettings)));
              } else {
                ValidationHelper.handleError(reply.cause(), asyncResultHandler);
              }
            });
  }

  @Override
  @Validate
  public void postAggregatorSettings(
      String lang,
      AggregatorSetting entity,
      Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {

    PgUtil.post(
        TABLE_NAME_AGGREGATOR_SETTINGS,
        entity,
        okapiHeaders,
        vertxContext,
        PostAggregatorSettingsResponse.class,
        asyncResultHandler);
  }

  @Override
  @Validate
  public void getAggregatorSettingsById(
      String id,
      String lang,
      Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {

    PgUtil.getById(
        TABLE_NAME_AGGREGATOR_SETTINGS,
        AggregatorSetting.class,
        id,
        okapiHeaders,
        vertxContext,
        GetAggregatorSettingsByIdResponse.class,
        asyncResultHandler);
  }

  @Override
  @Validate
  public void deleteAggregatorSettingsById(
      String id,
      String lang,
      Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {

    PgUtil.deleteById(
        TABLE_NAME_AGGREGATOR_SETTINGS,
        id,
        okapiHeaders,
        vertxContext,
        DeleteAggregatorSettingsByIdResponse.class,
        asyncResultHandler);
  }

  @Override
  @Validate
  public void putAggregatorSettingsById(
      String id,
      String lang,
      AggregatorSetting entity,
      Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {

    PgUtil.put(
        TABLE_NAME_AGGREGATOR_SETTINGS,
        entity,
        id,
        okapiHeaders,
        vertxContext,
        PutAggregatorSettingsByIdResponse.class,
        asyncResultHandler);
  }

  private Response createExportcredialsResponse(List<UsageDataProvider> udps, String format) {
    String csvString;
    try {
      csvString = getCredentialsCSV(udps);
    } catch (JsonProcessingException e) {
      return GetAggregatorSettingsExportcredentialsByIdResponse.respond500WithTextPlain(
          "Error creating CSV: " + e.getMessage());
    }

    if ("xlsx".equals(format)) {
      try {
        InputStream in = ExcelUtil.fromCSV(csvString);
        BinaryOutStream bos = new BinaryOutStream();
        bos.setData(ByteStreams.toByteArray(in));
        return GetAggregatorSettingsExportcredentialsByIdResponse
            .respond200WithApplicationVndOpenxmlformatsOfficedocumentSpreadsheetmlSheet(bos);
      } catch (IOException e) {
        return GetAggregatorSettingsExportcredentialsByIdResponse.respond500WithTextPlain(
            "Error creating xlsx content: " + e.getMessage());
      }
    }
    return GetAggregatorSettingsExportcredentialsByIdResponse.respond200WithTextCsv(csvString);
  }

  // index: usage_data_providers_custom_aggregatorid_idx
  @Override
  public void getAggregatorSettingsExportcredentialsById(
      String id,
      String format,
      Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {

    if (SUPPORTED_FORMATS.contains(format)) {
      Criteria criteria =
          new Criteria()
              .addField(Constants.FIELD_NAME_HARVESTING_CONFIG)
              .addField(Constants.FIELD_NAME_AGGREGATOR)
              .addField(Constants.FIELD_NAME_ID)
              .setOperation(Constants.OPERATOR_EQUALS)
              .setVal(id);
      Criterion criterion = new Criterion(criteria);
      CQLWrapper cql = new CQLWrapper(criterion);
      PgUtil.postgresClient(vertxContext, okapiHeaders)
          .get(
              TABLE_NAME_UDP,
              UsageDataProvider.class,
              cql,
              false,
              ar -> {
                if (ar.succeeded()) {
                  executeBlocking(
                          vertxContext,
                          () -> createExportcredialsResponse(ar.result().getResults(), format))
                      .onSuccess(resp -> asyncResultHandler.handle(succeededFuture(resp)))
                      .onFailure(
                          t ->
                              asyncResultHandler.handle(
                                  succeededFuture(
                                      GetAggregatorSettingsExportcredentialsByIdResponse
                                          .respond500WithTextPlain(t.getMessage()))));
                } else {
                  ValidationHelper.handleError(ar.cause(), asyncResultHandler);
                }
              });
    } else {
      asyncResultHandler.handle(
          succeededFuture(
              GetAggregatorSettingsExportcredentialsByIdResponse.respond400WithTextPlain(
                  String.format(UNSUPPORTED_MSG, format))));
    }
  }
}
