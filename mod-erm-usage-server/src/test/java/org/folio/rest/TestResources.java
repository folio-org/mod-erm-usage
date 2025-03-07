package org.folio.rest;

import com.google.common.io.Resources;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;

public enum TestResources {
  PROVIDER("fileupload/provider.json"),
  R51_SAMPLE_DR_OK("fileupload/DR_sample_r51.json"),
  R51_SAMPLE_DR_INVALID_ATTRIBUTES("fileupload/DR_sample_r51_invalid_attributes.json"),
  R51_SAMPLE_DR_INVALID_DATA("fileupload/DR_sample_r51_invalid_data.json"),
  R51_SAMPLE_DRD2_OK("fileupload/DRD2_sample_r51.json"),
  ;

  private final URL url;

  TestResources(String path) {
    this.url = Resources.getResource(path);
  }

  public String getAsString() {
    try {
      return Resources.toString(this.url, StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public File getAsFile() {
    try {
      return new File(this.url.toURI());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
