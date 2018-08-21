package org.folio.rest.impl;

import org.apache.log4j.Logger;
import org.folio.rest.resource.interfaces.InitAPI;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;

/**
 * It is possible to add custom code that will run once before the application is deployed (e.g. to
 * init a DB, create a cache, create static variables, etc.) by implementing the InitAPIs interface.
 * You must implement the init(Vertx vertx, Context context, Handler<AsyncResult<Boolean>>
 * resultHandler). Only one implementation per module is supported. Currently the implementation
 * should sit in the org.folio.rest.impl package in the implementing project. The implementation
 * will run during verticle deployment. The verticle will not complete deployment until the init()
 * completes. The init() function can do anything basically, but it must call back the Handler.
 *
 */
public class InitAPITest implements InitAPI {

  private static final Logger LOG = Logger.getLogger(InitAPITest.class);

  @Override
  public void init(Vertx arg0, Context arg1, Handler<AsyncResult<Boolean>> arg2) {
    LOG.info("InitAPI: Just testing the initial execution of code here.");
    arg2.handle(Future.succeededFuture());
  }

}
