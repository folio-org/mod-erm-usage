package org.folio.rest.util;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class VertxUtil {

  private VertxUtil() {}

  // TODO: useless now, refactor usages to use vertxContext directly
  public static <T> Future<T> executeBlocking(Context vertxContext, Supplier<T> supplier) {
    return vertxContext.executeBlocking(supplier::get);
  }

  /**
   * Converts a callback-based method to a Future-based method.
   *
   * <p>This is useful for bridging legacy callback-based APIs with modern Future-based code.
   *
   * <p>Example usage:
   *
   * <pre>{@code
   * return VertxUtil.toFuture(handler ->
   *   someCallbackMethod(param1, param2, handler)
   * );
   * }</pre>
   *
   * @param callback a consumer that accepts a handler and invokes the callback-based method
   * @param <T> the type of the result
   * @return a Future that completes with the result of the callback
   */
  public static <T> Future<T> toFuture(Consumer<Handler<AsyncResult<T>>> callback) {
    Promise<T> promise = Promise.promise();
    callback.accept(
        ar -> {
          if (ar.succeeded()) {
            promise.complete(ar.result());
          } else {
            promise.fail(ar.cause());
          }
        });
    return promise.future();
  }
}
