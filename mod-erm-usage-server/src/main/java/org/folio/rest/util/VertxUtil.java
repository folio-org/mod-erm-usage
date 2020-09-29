package org.folio.rest.util;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import java.util.function.Supplier;

public class VertxUtil {

  private VertxUtil() {}

  public static <T> Future<T> executeBlocking(Context vertxContext, Supplier<T> supplier) {
    Promise<T> result = Promise.promise();
    vertxContext.executeBlocking(promise -> promise.complete(supplier.get()), result);
    return result.future();
  }
}
