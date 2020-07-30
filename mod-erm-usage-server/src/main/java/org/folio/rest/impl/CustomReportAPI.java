package org.folio.rest.impl;

import static org.folio.rest.util.Constants.TABLE_NAME_CUSTOM_REPORTS;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import java.util.Map;
import javax.ws.rs.core.Response;
import org.folio.rest.annotations.Validate;
import org.folio.rest.jaxrs.model.CustomReport;
import org.folio.rest.jaxrs.model.CustomReportsGetOrder;
import org.folio.rest.jaxrs.resource.CustomReports;
import org.folio.rest.persist.PgUtil;

public class CustomReportAPI implements CustomReports {

  @Override
  @Validate
  public void getCustomReports(String query, String orderBy,
      CustomReportsGetOrder order, int offset, int limit, String lang,
      Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {
    PgUtil.get(TABLE_NAME_CUSTOM_REPORTS, CustomReport.class,
        org.folio.rest.jaxrs.model.CustomReports.class, query, offset, limit, okapiHeaders,
        vertxContext, GetCustomReportsResponse.class, asyncResultHandler);
  }

  @Override
  @Validate
  public void postCustomReports(String lang, CustomReport entity, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    PgUtil.post(TABLE_NAME_CUSTOM_REPORTS, entity, okapiHeaders, vertxContext,
        PostCustomReportsResponse.class, asyncResultHandler);
  }

  @Override
  @Validate
  public void getCustomReportsById(String id, String lang, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    PgUtil.getById(TABLE_NAME_CUSTOM_REPORTS, CustomReport.class, id, okapiHeaders, vertxContext,
        GetCustomReportsByIdResponse.class, asyncResultHandler);
  }

  @Override
  @Validate
  public void deleteCustomReportsById(String id, String lang, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    PgUtil.deleteById(TABLE_NAME_CUSTOM_REPORTS, id, okapiHeaders, vertxContext,
        DeleteCustomReportsByIdResponse.class, asyncResultHandler);
  }

  @Override
  @Validate
  public void putCustomReportsById(String id, String lang, CustomReport entity,
      Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {
    PgUtil.put(TABLE_NAME_CUSTOM_REPORTS, entity, id, okapiHeaders, vertxContext,
        PutCustomReportsByIdResponse.class, asyncResultHandler);
  }
}
