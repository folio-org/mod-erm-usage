package org.folio.rest.impl;

import org.apache.log4j.Logger;
import org.folio.rest.resource.interfaces.PostDeployVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;

/**
 * It is possible to add custom code that will be run immediately after the verticle running the
 * module is deployed.
 *
 */
public class PostDeployTest implements PostDeployVerticle {

  private static final Logger LOG = Logger.getLogger(PostDeployTest.class);

  @Override
  public void init(Vertx arg0, Context arg1, Handler<AsyncResult<Boolean>> arg2) {
    LOG.info("PostDeployTest: Just testing code execution after deployment here.");
  }

}
