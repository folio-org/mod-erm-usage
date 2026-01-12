package org.folio.rest.util;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServer;
import io.vertx.core.net.impl.NetServerInternal;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
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

  /**
   * Extracts the HttpServer instance from a NetServerInternal using reflection.
   *
   * @param netServer the NetServerInternal to extract from
   * @return the HttpServer instance
   * @throws IllegalStateException if the HttpServer cannot be extracted
   */
  public static HttpServer extractHttpServer(NetServerInternal netServer) {
    Object handlerLambda = getField(netServer, "handler");
    if (handlerLambda == null) {
      throw new IllegalStateException("NetServerImpl.handler is null");
    }

    Object httpServerImpl = getField(handlerLambda, "arg$1");
    if (httpServerImpl == null) {
      throw new IllegalStateException("Handler lambda arg$1 is null");
    }

    if (!(httpServerImpl instanceof HttpServer httpServer)) {
      throw new IllegalStateException(
          "arg$1 is not an HttpServer: " + httpServerImpl.getClass().getName());
    }

    return httpServer;
  }

  /**
   * Uses reflection to get a field value from an object, searching up the class hierarchy.
   *
   * @param target the object to get the field from
   * @param name the field name
   * @return the field value, or null if not found
   * @throws IllegalStateException if reflection access fails
   */
  private static Object getField(Object target, String name) {
    Class<?> c = target.getClass();
    while (c != null) {
      try {
        MethodHandles.Lookup lookup = MethodHandles.privateLookupIn(c, MethodHandles.lookup());
        Field f = c.getDeclaredField(name);
        return lookup.unreflectGetter(f).invoke(target);
      } catch (NoSuchFieldException e) {
        c = c.getSuperclass();
      } catch (IllegalAccessException e) {
        throw new IllegalStateException("Cannot access field: " + name, e);
      } catch (Throwable e) {
        throw new IllegalStateException("Failed to get field: " + name, e);
      }
    }
    return null;
  }
}
