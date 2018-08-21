package org.folio.rest.impl;

import org.apache.log4j.Logger;
import org.folio.rest.resource.interfaces.PeriodicAPI;
import io.vertx.core.Context;
import io.vertx.core.Vertx;

/**
 * 
 * It is possible to add custom code that will run periodically. For example, to ongoingly check
 * status of something in the system and act upon that. Need to implement the PeriodicAPI interface:
 *
 */
public class PeriodicAPIImpl implements PeriodicAPI {

  private static final Logger LOG = Logger.getLogger(PeriodicAPIImpl.class);

  @Override
  public void run(Vertx arg0, Context arg1) {
    LOG.info("PeriodicAPITest: Just testing running code periodically every " + runEvery()
        + " ms here.");
  }

  @Override
  public long runEvery() {
    // TODO Auto-generated method stub
    return 45000;
  }

}
