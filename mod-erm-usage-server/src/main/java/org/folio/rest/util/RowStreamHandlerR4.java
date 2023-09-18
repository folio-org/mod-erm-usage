package org.folio.rest.util;

import static io.vertx.core.Future.failedFuture;
import static io.vertx.core.Future.succeededFuture;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.json.Json;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowStream;
import org.folio.rest.jaxrs.model.CounterReport;
import org.niso.schemas.counter.Report;
import org.olf.erm.usage.counter41.Counter4Utils;

/**
 * A handler for processing {@code RowStream<Row>} events, where the first column of each {@code
 * Row} represents a Counter4 report, and merging them into a single Counter4 Report.
 */
public class RowStreamHandlerR4 implements Handler<RowStream<Row>> {

  private final Context vertxContext;
  private final Handler<AsyncResult<Report>> resultHandler;
  private Report mergedReport = null;

  /**
   * Constructor for RowStreamHandlerR4.
   *
   * @param vertxContext The Vert.x context for handling blocking operations.
   * @param resultHandler The handler for the final result of merged Counter4 Reports.
   */
  public RowStreamHandlerR4(Context vertxContext, Handler<AsyncResult<Report>> resultHandler) {
    this.vertxContext = vertxContext;
    this.resultHandler = resultHandler;
  }

  /**
   * Handles incoming rows from a {@code RowStream<Row>} event, where the first column of each
   * {@code Row} represents a Counter4 report. These reports are merged into a single Counter4
   * Report and the result is passed to the resultHandler.
   *
   * @param event The {@code RowStream<Row>} event to handle.
   */
  @Override
  public void handle(RowStream<Row> event) {
    event
        .handler(
            r -> {
              event.pause();
              vertxContext
                  .executeBlocking(
                      promise -> {
                        try {
                          CounterReport counterReport =
                              r.getJsonObject(0).mapTo(CounterReport.class);
                          Report report =
                              Counter4Utils.fromJSON(Json.encode(counterReport.getReport()));
                          mergedReport =
                              mergedReport == null
                                  ? report
                                  : Counter4Utils.merge(mergedReport, report);
                          promise.complete();
                        } catch (Exception e) {
                          promise.fail(e);
                        }
                      })
                  .onSuccess(h -> event.resume())
                  .onFailure(t -> event.close(v -> resultHandler.handle(failedFuture(t))));
            })
        .endHandler(
            v ->
                resultHandler.handle(
                    mergedReport == null
                        ? failedFuture("Merged report is null")
                        : succeededFuture(mergedReport)))
        .exceptionHandler(t -> resultHandler.handle(failedFuture(t)));
  }
}
