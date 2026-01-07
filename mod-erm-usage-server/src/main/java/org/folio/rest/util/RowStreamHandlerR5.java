package org.folio.rest.util;

import static io.vertx.core.Future.failedFuture;
import static io.vertx.core.Future.succeededFuture;
import static org.folio.rest.util.ReportExportHelper.SUPPORTED_VIEWS;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
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
public class RowStreamHandlerR5 {

  private final Context vertxContext;
  private final String reportName;
  private final boolean isConvertReport;
  private Object mergedReport = null;

  /**
   * Constructor for RowStreamHandlerR5.
   *
   * @param vertxContext The Vert.x context for handling blocking operations.
   * @param reportName The name of the Counter5 report, used to convert Master Reports into Standard
   *     Views
   */
  public RowStreamHandlerR5(Context vertxContext, String reportName) {
    this.vertxContext = vertxContext;
    this.reportName = reportName;
    this.isConvertReport = SUPPORTED_VIEWS.contains(reportName.toUpperCase());
  }

  /**
   * Handles incoming rows from a {@code RowStream<Row>} event, where the first column of each
   * {@code Row} represents a Counter5 report. These reports are converted based on the report name
   * and merged.
   *
   * @param rowStream The {@code RowStream<Row>} to process.
   * @return A Future that completes with the merged report
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
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
