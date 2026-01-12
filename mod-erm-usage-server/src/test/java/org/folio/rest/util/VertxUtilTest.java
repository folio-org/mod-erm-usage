package org.folio.rest.util;

import static org.assertj.core.api.Assertions.assertThat;

import io.vertx.core.Future;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class VertxUtilTest {

  @Test
  public void testToFutureSuccess(TestContext context) {
    String expectedResult = "test result";

    Future<String> future =
        VertxUtil.toFuture(handler -> handler.handle(Future.succeededFuture(expectedResult)));

    future.onComplete(
        context.asyncAssertSuccess(result -> assertThat(result).isEqualTo(expectedResult)));
  }

  @Test
  public void testToFutureFailure(TestContext context) {
    Exception expectedException = new RuntimeException("test error");

    Future<String> future =
        VertxUtil.toFuture(handler -> handler.handle(Future.failedFuture(expectedException)));

    future.onComplete(
        context.asyncAssertFailure(
            error -> {
              assertThat(error).isEqualTo(expectedException);
              assertThat(error.getMessage()).isEqualTo("test error");
            }));
  }
}
