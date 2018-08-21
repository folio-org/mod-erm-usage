package org.folio.rest.impl;

import org.apache.log4j.Logger;
import org.folio.rest.resource.interfaces.ShutdownAPI;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;

/**
 * This will occur on graceful shutdowns, but can not be guaranteed to run if the JVM is forcefully
 * shutdown.
 *
 */
public class ShutdownTest implements ShutdownAPI {

  private static final Logger LOG = Logger.getLogger(ShutdownTest.class);

  @Override
  public void shutdown(Vertx arg0, Context arg1, Handler<AsyncResult<Void>> arg2) {
    LOG.info("PostDeployTest: Just testing code execution after shutdown here.");
    arg2.handle(Future.succeededFuture());
  }

}
