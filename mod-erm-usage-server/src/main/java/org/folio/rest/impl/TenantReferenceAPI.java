package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import java.util.Map;
import javax.ws.rs.core.Response;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.tools.utils.TenantLoading;

public class TenantReferenceAPI extends TenantAPI {

  @Override
  Future<Integer> loadData(
      TenantAttributes attributes,
      String tenantId,
      Map<String, String> headers,
      Context vertxContext) {

    return super.loadData(attributes, tenantId, headers, vertxContext)
        .compose(
            i ->
                new TenantLoading()
                    .withKey("loadSample")
                    .withLead("sample-data")
                    .add("usage-data-providers")
                    .add("counter-reports")
                    .perform(attributes, headers, vertxContext, i));
  }

  @Override
  public void postTenant(
      TenantAttributes entity,
      Map<String, String> headers,
      Handler<AsyncResult<Response>> handlers,
      Context context) {
    super.postTenantSync(entity, headers, handlers, context);
  }
}
