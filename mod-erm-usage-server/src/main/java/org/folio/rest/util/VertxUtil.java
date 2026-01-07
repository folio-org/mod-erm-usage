package org.folio.rest.util;

import io.vertx.core.Context;
import io.vertx.core.Future;
import java.util.function.Supplier;

public class VertxUtil {

  private VertxUtil() {}

  // TODO: useless now, refactor usages to use vertxContext directly
  public static <T> Future<T> executeBlocking(Context vertxContext, Supplier<T> supplier) {
    return vertxContext.executeBlocking(supplier::get);
  }
}
