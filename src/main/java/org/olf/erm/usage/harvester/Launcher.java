package org.olf.erm.usage.harvester;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonObject;

public class Launcher extends io.vertx.core.Launcher {

  public static void main(String[] args) {
    new Launcher().dispatch(args);
  }

  @Override
  public void beforeDeployingVerticle(DeploymentOptions deploymentOptions) {
    super.beforeDeployingVerticle(deploymentOptions);

    if (deploymentOptions.getConfig() == null) {
      deploymentOptions.setConfig(new JsonObject());
    }

    try {
      String config = FileUtils.readFileToString(new File("config.json"));
      deploymentOptions.getConfig().mergeIn(new JsonObject(config));
    } catch (DecodeException e) {
      System.err.println("Couldnt decode JSON configuration");
    } catch (FileNotFoundException e) {
      // ignore
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    System.out.println(deploymentOptions.getConfig().encodePrettily());
  }
}
