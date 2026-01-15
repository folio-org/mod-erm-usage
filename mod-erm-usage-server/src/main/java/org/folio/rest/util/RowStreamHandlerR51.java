package org.folio.rest.util;

import static io.vertx.core.Future.failedFuture;
import static io.vertx.core.Future.succeededFuture;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowStream;
import java.util.List;
import org.folio.rest.jaxrs.model.CounterReport;
import org.olf.erm.usage.counter51.Counter51Utils;
import org.olf.erm.usage.counter51.ReportType;

/**
 * A handler for processing {@code RowStream<Row>} events for COUNTER 5.1 reports, where the first
 * column of each {@code Row} represents a COUNTER 5.1 report, and merging them into a single
 * COUNTER 5.1 report. Optionally converts Master Reports into Standard Views.
 */
public class RowStreamHandlerR51 {

  private final Context vertxContext;
  private final ReportType reportType;
  private ObjectNode mergedReport = null;

  /**
   * Constructor for RowStreamHandlerR51.
   *
   * @param vertxContext The Vert.x context for handling blocking operations.
   * @param reportName The name of the COUNTER 5.1 report
   */
  public RowStreamHandlerR51(Context vertxContext, String reportName) {
    this.vertxContext = vertxContext;
    this.reportType = ReportType.valueOf(reportName);
  }

  /**
   * Handles incoming rows from a {@code RowStream<Row>} event, where the first column of each
   * {@code Row} represents a COUNTER 5.1 report. These reports are converted and merged.
   *
   * @param rowStream The {@code RowStream<Row>} to process.
   * @return A Future that completes with the merged report
   */
  public Future<Object> handle(RowStream<Row> rowStream) {
    Promise<Object> promise = Promise.promise();

    rowStream
        .handler(
            r -> {
              rowStream.pause();
              vertxContext
                  .executeBlocking(
                      () -> {
                        CounterReport counterReport = r.getJsonObject(0).mapTo(CounterReport.class);
                        ObjectNode report =
                            Counter51Utils.getDefaultObjectMapper()
                                .valueToTree(counterReport.getReport());

                        if (reportType.isStandardView()) {
                          report = Counter51Utils.convertReport(report, reportType);
                        }

                        mergedReport =
                            mergedReport == null
                                ? report
                                : Counter51Utils.mergeReports(List.of(mergedReport, report));
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
