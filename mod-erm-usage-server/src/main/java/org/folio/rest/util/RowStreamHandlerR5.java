package org.folio.rest.util;

import static io.vertx.core.Future.failedFuture;
import static io.vertx.core.Future.succeededFuture;
import static org.folio.rest.util.ReportExportHelper.SUPPORTED_VIEWS;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.json.Json;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowStream;
import java.util.List;
import org.folio.rest.jaxrs.model.CounterReport;
import org.olf.erm.usage.counter50.Counter5Utils;
import org.olf.erm.usage.counter50.converter.Converter;
import org.olf.erm.usage.counter50.converter.ReportConverter;

/**
 * A handler for processing {@code RowStream<Row>} events for Counter5 reports, where the first
 * column of each {@code Row} represents a Counter5 report, and merging them into a single Counter5
 * Report. Optionally converts Master Reports into Standard Views.
 */
public class RowStreamHandlerR5 implements Handler<RowStream<Row>> {

  private final Context vertxContext;
  private final Handler<AsyncResult<Object>> resultHandler;
  private final String reportName;
  private final boolean isConvertReport;
  private Object mergedReport = null;

  /**
   * Constructor for RowStreamHandlerR5.
   *
   * @param vertxContext The Vert.x context for handling blocking operations.
   * @param reportName The name of the Counter5 report, used to convert Master Reports into Standard
   *     Views
   * @param resultHandler The handler for the final result of merged Counter5 Reports.
   */
  public RowStreamHandlerR5(
      Context vertxContext, String reportName, Handler<AsyncResult<Object>> resultHandler) {
    this.vertxContext = vertxContext;
    this.reportName = reportName;
    this.resultHandler = resultHandler;
    this.isConvertReport = SUPPORTED_VIEWS.contains(reportName.toUpperCase());
  }

  /**
   * Handles incoming rows from a {@code RowStream<Row>} event, where the first column of each
   * {@code Row} represents a Counter5 report. These reports are converted based on the report name,
   * merged and the result is passed to the resultHandler.
   *
   * @param event The {@code RowStream<Row>} event to handle.
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
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
                          Object cop5Report =
                              Counter5Utils.fromJSON(Json.encode(counterReport.getReport()));
                          if (isConvertReport) {
                            Converter converter = ReportConverter.create(reportName);
                            cop5Report = converter.convert(cop5Report);
                          }

                          mergedReport =
                              mergedReport == null
                                  ? cop5Report
                                  : Counter5Utils.merge(List.of(mergedReport, cop5Report));
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
