package org.folio.rest.util;

import java.util.Arrays;

/** Represents different versions of COUNTER report releases. */
public enum ReportReleaseVersion {
  R4("4"),
  R5("5"),
  R51("5.1");

  private final String version;

  ReportReleaseVersion(String version) {
    this.version = version;
  }

  public String getVersion() {
    return version;
  }

  public static ReportReleaseVersion fromVersion(String version) {
    return Arrays.stream(values())
        .filter(rrv -> rrv.version.equals(version))
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("Unknown version: " + version));
  }
}
