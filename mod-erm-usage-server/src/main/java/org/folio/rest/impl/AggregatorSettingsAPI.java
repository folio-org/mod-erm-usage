package org.folio.rest.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.cql2pgjson.CQL2PgJSON;
import org.folio.cql2pgjson.exception.FieldException;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.rest.annotations.Validate;
import org.folio.rest.jaxrs.model.AggregatorSetting;
import org.folio.rest.jaxrs.model.AggregatorSettings;
import org.folio.rest.jaxrs.model.AggregatorSettingsGetOrder;
import org.folio.rest.jaxrs.model.UsageDataProvider;
import org.folio.rest.persist.Criteria.Criteria;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.Criteria.Limit;
import org.folio.rest.persist.Criteria.Offset;
import org.folio.rest.persist.PgUtil;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.cql.CQLWrapper;
import org.folio.rest.tools.messages.MessageConsts;
import org.folio.rest.tools.messages.Messages;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.rest.util.Constants;
import org.folio.rest.util.ExportObject;

import javax.ws.rs.core.Response;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.folio.rest.util.Constants.TABLE_NAME_UDP;

public class AggregatorSettingsAPI implements org.folio.rest.jaxrs.resource.AggregatorSettings {

  private static final String TABLE_NAME_AGGREGATOR_SETTINGS = "aggregator_settings";

  private final Messages messages = Messages.getInstance();
  private final Logger logger = LoggerFactory.getLogger(AggregatorSettingsAPI.class);

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
    CQL2PgJSON cql2pgJson =
        new CQL2PgJSON(Arrays.asList(TABLE_NAME_AGGREGATOR_SETTINGS + ".jsonb"));
    return new CQLWrapper(cql2pgJson, query)
        .setLimit(new Limit(limit))
        .setOffset(new Offset(offset));
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
    try {
      CQLWrapper cql = getCQL(query, limit, offset);
      vertxContext.runOnContext(
          v -> {
            String tenantId = TenantTool.calculateTenantId(okapiHeaders.get(XOkapiHeaders.TENANT));
            logger.debug("Headers present are: " + okapiHeaders.keySet().toString());
            logger.debug("tenantId = " + tenantId);
            String[] fieldList = {"*"};
            try {
              PostgresClient.getInstance(vertxContext.owner(), tenantId)
                  .get(
                      TABLE_NAME_AGGREGATOR_SETTINGS,
                      AggregatorSetting.class,
                      fieldList,
                      cql,
                      true,
                      false,
                      reply -> {
                        try {
                          if (reply.succeeded()) {
                            AggregatorSettings aggregatorSettingsDataCollection =
                                new AggregatorSettings();
                            List<AggregatorSetting> aggregatorSettings =
                                reply.result().getResults();
                            aggregatorSettingsDataCollection.setAggregatorSettings(
                                aggregatorSettings);
                            aggregatorSettingsDataCollection.setTotalRecords(
                                reply.result().getResultInfo().getTotalRecords());
                            asyncResultHandler.handle(
                                Future.succeededFuture(
                                    GetAggregatorSettingsResponse.respond200WithApplicationJson(
                                        aggregatorSettingsDataCollection)));
                          } else {
                            asyncResultHandler.handle(
                                Future.succeededFuture(
                                    GetAggregatorSettingsResponse.respond500WithTextPlain(
                                        reply.cause().getMessage())));
                          }
                        } catch (Exception e) {
                          logger.debug(e.getLocalizedMessage());

                          asyncResultHandler.handle(
                              Future.succeededFuture(
                                  GetAggregatorSettingsResponse.respond500WithTextPlain(
                                      reply.cause().getMessage())));
                        }
                      });
            } catch (IllegalStateException e) {
              logger.debug("IllegalStateException: " + e.getLocalizedMessage());
              asyncResultHandler.handle(
                  Future.succeededFuture(
                      GetAggregatorSettingsResponse.respond400WithTextPlain(
                          "CQL Illegal State Error for '"
                              + query
                              + "': "
                              + e.getLocalizedMessage())));
            } catch (Exception e) {
              Throwable cause = e;
              while (cause.getCause() != null) {
                cause = cause.getCause();
              }
              logger.debug(
                  "Got error " + cause.getClass().getSimpleName() + ": " + e.getLocalizedMessage());
              if (cause.getClass().getSimpleName().contains("CQLParseException")) {
                logger.debug("BAD CQL");
                asyncResultHandler.handle(
                    Future.succeededFuture(
                        GetAggregatorSettingsResponse.respond400WithTextPlain(
                            "CQL Parsing Error for '"
                                + query
                                + "': "
                                + cause.getLocalizedMessage())));
              } else {
                asyncResultHandler.handle(
                    io.vertx.core.Future.succeededFuture(
                        GetAggregatorSettingsResponse.respond500WithTextPlain(
                            messages.getMessage(lang, MessageConsts.InternalServerError))));
              }
            }
          });
    } catch (Exception e) {
      logger.error(e.getLocalizedMessage(), e);
      if (e.getCause() != null
          && e.getCause().getClass().getSimpleName().contains("CQLParseException")) {
        logger.debug("BAD CQL");
        asyncResultHandler.handle(
            Future.succeededFuture(
                GetAggregatorSettingsResponse.respond400WithTextPlain(
                    "CQL Parsing Error for '" + query + "': " + e.getLocalizedMessage())));
      } else {
        asyncResultHandler.handle(
            io.vertx.core.Future.succeededFuture(
                GetAggregatorSettingsResponse.respond500WithTextPlain(
                    messages.getMessage(lang, MessageConsts.InternalServerError))));
      }
    }
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

  @Override
  public void getAggregatorSettingsExportcredentialsById(
      String id,
      Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {

    Criteria criteria =
        new Criteria()
            .addField(Constants.FIELD_NAME_HARVESTING_CONFIG)
            .addField(Constants.FIELD_NAME_AGGREGATOR)
            .addField(Constants.FIELD_NAME_ID)
            .setOperation(Constants.OPERATOR_EQUALS)
            .setVal(id);
    Criterion criterion = new Criterion(criteria);
    CQLWrapper cql = new CQLWrapper(criterion);

    PostgresClient.getInstance(vertxContext.owner(), okapiHeaders.get(XOkapiHeaders.TENANT))
        .get(
            TABLE_NAME_UDP,
            UsageDataProvider.class,
            cql,
            true,
            true,
            ar -> {
              if (ar.succeeded()) {
                List<UsageDataProvider> providerList = ar.result().getResults();
                try {
                  String resultString = getCredentialsCSV(providerList);
                  asyncResultHandler.handle(
                      Future.succeededFuture(
                          GetAggregatorSettingsExportcredentialsByIdResponse.respond200WithTextCsv(
                              resultString)));
                } catch (JsonProcessingException e) {
                  asyncResultHandler.handle(
                      Future.succeededFuture(
                          GetAggregatorSettingsExportcredentialsByIdResponse
                              .respond500WithTextPlain("Error creating CSV")));
                }
              } else {
                asyncResultHandler.handle(
                    Future.succeededFuture(
                        GetAggregatorSettingsExportcredentialsByIdResponse.respond500WithTextPlain(
                            "Error creating CSV")));
              }
            });
  }
}
