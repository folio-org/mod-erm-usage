package org.folio.rest;

import io.vertx.core.Vertx;
import org.folio.postgres.testing.PostgresTesterContainer;
import org.folio.rest.persist.PostgresClient;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public class PostgresExtension implements BeforeAllCallback, AfterAllCallback {
  private static final Vertx vertx = TestUtils.getVertx();
  private static String contextId = null;

  @Override
  public void beforeAll(ExtensionContext context) {
    if (contextId == null) {
      PostgresClient.setPostgresTester(new PostgresTesterContainer());
      PostgresClient.getInstance(vertx).startPostgresTester();
      contextId = context.getUniqueId();
    }
  }

  @Override
  public void afterAll(ExtensionContext context) {
    if (context.getUniqueId().equals(contextId)) {
      contextId = null;
      PostgresClient.stopPostgresTester();
    }
  }
}
