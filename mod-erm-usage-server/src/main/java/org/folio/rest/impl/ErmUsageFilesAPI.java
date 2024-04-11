package org.folio.rest.impl;

import static org.folio.rest.util.Constants.TABLE_NAME_FILES;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowIterator;
import io.vertx.sqlclient.Tuple;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.UUID;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.ArrayUtils;
import org.folio.rest.annotations.Stream;
import org.folio.rest.annotations.Validate;
import org.folio.rest.jaxrs.resource.ErmUsageFiles;
import org.folio.rest.persist.PgUtil;
import org.folio.rest.tools.utils.BinaryOutStream;

public class ErmUsageFilesAPI implements ErmUsageFiles {

  private byte[] stream = new byte[0];

  @Stream
  @Override
  @Validate
  public void postErmUsageFiles(
      InputStream entity,
      Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {

    if (okapiHeaders.containsKey("streamed_abort")) {
      PostErmUsageFilesResponse.respond500WithTextPlain("Stream aborted");
      return;
    }

    byte[] readBytes;
    try {
      readBytes = entity.readAllBytes();
    } catch (IOException e) {
      PostErmUsageFilesResponse.respond500WithTextPlain("Error reading stream");
      return;
    }

    if (okapiHeaders.containsKey("complete")) {
      String uuid = UUID.randomUUID().toString();
      PgUtil.postgresClient(vertxContext, okapiHeaders)
          .execute(
              "INSERT INTO " + TABLE_NAME_FILES + " (id, data) VALUES ($1, $2)",
              Tuple.of(uuid, stream))
          .onComplete(
              ar -> {
                if (ar.succeeded()) {
                  JsonObject result = new JsonObject();
                  result.put("id", uuid);
                  result.put("size", stream.length / 1000F);
                  asyncResultHandler.handle(
                      Future.succeededFuture(
                          PostErmUsageFilesResponse.respond200WithTextJson(
                              result.encodePrettily())));
                } else {
                  asyncResultHandler.handle(
                      Future.succeededFuture(
                          PostErmUsageFilesResponse.respond500WithTextPlain(
                              "Cannot insert file. " + ar.cause())));
                }
              });
    } else {
      stream = ArrayUtils.addAll(stream, readBytes);
    }
  }

  @Override
  @Validate
  public void getErmUsageFilesById(
      String id,
      Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {

    PgUtil.postgresClient(vertxContext, okapiHeaders)
        .execute("SELECT data FROM " + TABLE_NAME_FILES + " WHERE id = $1", Tuple.of(id))
        .onComplete(
            ar -> {
              if (ar.succeeded()) {
                RowIterator<Row> iterator = ar.result().iterator();
                if (iterator.hasNext()) {
                  Row next = iterator.next();
                  Buffer buffer = next.getBuffer(0);
                  BinaryOutStream binaryOutStream = new BinaryOutStream();
                  binaryOutStream.setData(buffer.getBytes());
                  asyncResultHandler.handle(
                      Future.succeededFuture(
                          GetErmUsageFilesByIdResponse.respond200WithApplicationOctetStream(
                              binaryOutStream)));
                } else {
                  asyncResultHandler.handle(
                      Future.succeededFuture(
                          GetErmUsageFilesByIdResponse.respond404WithTextPlain("Not found.")));
                }
              } else {
                asyncResultHandler.handle(
                    Future.succeededFuture(
                        GetErmUsageFilesByIdResponse.respond500WithTextPlain(
                            "Cannot get file. " + ar.cause())));
              }
            });
  }

  @Override
  @Validate
  public void deleteErmUsageFilesById(
      String id,
      Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {

    PgUtil.postgresClient(vertxContext, okapiHeaders)
        .delete(
            TABLE_NAME_FILES,
            id,
            ar -> {
              if (ar.succeeded()) {
                asyncResultHandler.handle(
                    Future.succeededFuture(DeleteErmUsageFilesByIdResponse.respond204()));
              } else {
                asyncResultHandler.handle(
                    Future.succeededFuture(
                        DeleteErmUsageFilesByIdResponse.respond500WithTextPlain(ar.cause())));
              }
            });
  }
}
