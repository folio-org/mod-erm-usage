package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
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
            i -> {
              Promise<Integer> promise = Promise.promise();
              TenantLoading tl = new TenantLoading();
              tl.withKey("loadSample")
                  .withLead("sample-data")
                  .add("aggregator-settings")
                  .add("usage-data-providers")
                  .add("counter-reports")
                  .perform(attributes, headers, vertxContext.owner(), promise);
              return promise.future();
            });
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
