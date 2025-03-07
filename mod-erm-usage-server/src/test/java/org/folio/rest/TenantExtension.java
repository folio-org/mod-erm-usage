package org.folio.rest;

import static org.folio.rest.TestUtils.deleteTenant;
import static org.folio.rest.TestUtils.postTenant;

import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public class TenantExtension implements BeforeAllCallback, AfterAllCallback {
  private static final boolean LOAD_SAMPLE_DEFAULT = false;
  private static final String TENANT_ID_DEFAULT = "diku";

  @Override
  public void beforeAll(ExtensionContext context) throws ExecutionException, InterruptedException {
    Class<?> testClass = context.getRequiredTestClass();
    SetupTenant[] annotations = testClass.getAnnotationsByType(SetupTenant.class);

    List<Future<HttpResponse<Buffer>>> futures = new ArrayList<>();
    if (annotations.length == 0) {
      futures.add(postTenant(TENANT_ID_DEFAULT, LOAD_SAMPLE_DEFAULT));
    } else {
      for (SetupTenant annotation : annotations) {
        futures.add(postTenant(annotation.tenantId(), annotation.loadSample()));
      }
    }
    Future.all(futures).toCompletionStage().toCompletableFuture().get();
  }

  @Override
  public void afterAll(ExtensionContext context) throws ExecutionException, InterruptedException {
    Class<?> testClass = context.getRequiredTestClass();
    SetupTenant[] annotations = testClass.getAnnotationsByType(SetupTenant.class);

    List<Future<HttpResponse<Buffer>>> futures = new ArrayList<>();
    if (annotations.length == 0) {
      futures.add(deleteTenant(TENANT_ID_DEFAULT, true));
    } else {
      for (SetupTenant annotation : annotations) {
        futures.add(deleteTenant(annotation.tenantId(), true));
      }
    }
    Future.all(futures).toCompletionStage().toCompletableFuture().get();
  }
}
