package org.folio.rest.util;

import static io.vertx.core.Future.failedFuture;
import static io.vertx.core.Future.succeededFuture;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
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
public class RowStreamHandlerR4 {

  private final Context vertxContext;
  private Report mergedReport = null;

  /**
   * Constructor for RowStreamHandlerR4.
   *
   * @param vertxContext The Vert.x context for handling blocking operations.
   */
  public RowStreamHandlerR4(Context vertxContext) {
    this.vertxContext = vertxContext;
  }

  /**
   * Handles incoming rows from a {@code RowStream<Row>} event, where the first column of each
   * {@code Row} represents a Counter4 report. These reports are merged into a single Counter4
   * Report.
   *
   * @param rowStream The {@code RowStream<Row>} to process.
   * @return A Future that completes with the merged Report
   */
  public Future<Report> handle(RowStream<Row> rowStream) {
    Promise<Report> promise = Promise.promise();

    rowStream
        .handler(
            r -> {
              rowStream.pause();
              vertxContext
                  .executeBlocking(
                      () -> {
                        CounterReport counterReport = r.getJsonObject(0).mapTo(CounterReport.class);
                        Report report =
                            Counter4Utils.fromJSON(Json.encode(counterReport.getReport()));
                        mergedReport =
                            mergedReport == null
                                ? report
                                : Counter4Utils.merge(mergedReport, report);
                        return null;
                      })
                  .onSuccess(h -> rowStream.resume())
                  .onFailure(t -> rowStream.close().onComplete(v -> promise.fail(t)));
            })
        .endHandler(
            v ->
                promise.handle(
                    mergedReport == null
                        ? failedFuture("Merged report is null")
                        : succeededFuture(mergedReport)))
        .exceptionHandler(promise::fail);

    return promise.future();
  }
}
