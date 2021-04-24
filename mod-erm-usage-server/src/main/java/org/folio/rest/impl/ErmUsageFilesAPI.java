package org.folio.rest.impl;

import static org.folio.rest.util.Constants.TABLE_NAME_FILES;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import java.io.IOException;
import java.io.InputStream;
import java.util.Base64;
import java.util.Map;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.ArrayUtils;
import org.folio.rest.annotations.Stream;
import org.folio.rest.annotations.Validate;
import org.folio.rest.jaxrs.resource.ErmUsageFiles;
import org.folio.rest.model.ErmUsageFile;
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
      String base64 = Base64.getEncoder().encodeToString(stream);
      ErmUsageFile file = new ErmUsageFile().withData(base64);
      PgUtil.postgresClient(vertxContext, okapiHeaders)
          .save(
              TABLE_NAME_FILES,
              file,
              ar -> {
                if (ar.succeeded()) {
                  JsonObject result = new JsonObject();
                  result.put("id", ar.result());
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
        .getById(
            TABLE_NAME_FILES,
            id,
            ErmUsageFile.class,
            ar -> {
              if (ar.succeeded()) {
                if (ar.result() == null) {
                  asyncResultHandler.handle(
                      Future.succeededFuture(
                          GetErmUsageFilesByIdResponse.respond404WithTextPlain("Not found.")));
                } else {
                  String dataAsString = ar.result().getData();
                  byte[] decoded = Base64.getDecoder().decode(dataAsString);
                  BinaryOutStream binaryOutStream = new BinaryOutStream();
                  binaryOutStream.setData(decoded);
                  asyncResultHandler.handle(
                      Future.succeededFuture(
                          GetErmUsageFilesByIdResponse.respond200WithApplicationOctetStream(
                              binaryOutStream)));
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
