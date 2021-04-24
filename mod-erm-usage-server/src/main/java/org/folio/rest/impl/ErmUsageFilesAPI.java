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
import org.apache.commons.io.IOUtils;
import org.folio.rest.annotations.Validate;
import org.folio.rest.jaxrs.resource.ErmUsageFiles;
import org.folio.rest.model.ErmUsageFile;
import org.folio.rest.persist.PgUtil;
import org.folio.rest.tools.utils.BinaryOutStream;

public class ErmUsageFilesAPI implements ErmUsageFiles {

  @Override
  @Validate
  public void postErmUsageFiles(
      InputStream entity,
      Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {

    byte[] bytes = new byte[0];
    try {
      bytes = IOUtils.toByteArray(entity);
    } catch (IOException e) {
      asyncResultHandler.handle(
          Future.succeededFuture(
              PostErmUsageFilesResponse.respond404WithTextPlain("Cannot read file.")));
    }

    String base64 = Base64.getEncoder().encodeToString(bytes);
    ErmUsageFile file = new ErmUsageFile().withData(base64);

    float size = bytes.length / 1000F;

    PgUtil.postgresClient(vertxContext, okapiHeaders)
        .save(
            TABLE_NAME_FILES,
            file,
            ar -> {
              if (ar.succeeded()) {
                JsonObject result = new JsonObject();
                result.put("id", ar.result());
                result.put("size", size);
                asyncResultHandler.handle(
                    Future.succeededFuture(
                        PostErmUsageFilesResponse.respond200WithTextJson(result.encodePrettily())));
              } else {
                asyncResultHandler.handle(
                    Future.succeededFuture(
                        PostErmUsageFilesResponse.respond500WithTextPlain(
                            "Cannot insert file. " + ar.cause())));
              }
            });
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
