package org.folio.rest;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public class RestVerticleExtension implements BeforeAllCallback, AfterAllCallback {
  private static final Vertx vertx = TestUtils.getVertx();
  private final int port = TestUtils.getPort();
  private String contextId = null;
  private String deploymentId = null;

  @Override
  public void beforeAll(ExtensionContext context) throws ExecutionException, InterruptedException {
    if (contextId == null) {
      DeploymentOptions options =
          new DeploymentOptions().setConfig(new JsonObject().put("http.port", port));
      deploymentId =
          vertx
              .deployVerticle(RestVerticle.class.getName(), options)
              .toCompletionStage()
              .toCompletableFuture()
              .get();
      contextId = context.getUniqueId();
    }
  }

  @Override
  public void afterAll(ExtensionContext context) throws ExecutionException, InterruptedException {
    if (context.getUniqueId().equals(contextId)) {
      vertx.undeploy(deploymentId).toCompletionStage().toCompletableFuture().get();
    }
  }
}
