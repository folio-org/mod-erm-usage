package org.folio.rest.util;

import static io.vertx.core.Future.failedFuture;
import static io.vertx.core.Future.succeededFuture;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
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
public class RowStreamHandlerR51 implements Handler<RowStream<Row>> {

  private final Context vertxContext;
  private final Handler<AsyncResult<Object>> resultHandler;
  private final ReportType reportType;
  private ObjectNode mergedReport = null;

  public RowStreamHandlerR51(
      Context vertxContext, String reportName, Handler<AsyncResult<Object>> resultHandler) {
    this.vertxContext = vertxContext;
    this.reportType = ReportType.valueOf(reportName);
    this.resultHandler = resultHandler;
  }

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
