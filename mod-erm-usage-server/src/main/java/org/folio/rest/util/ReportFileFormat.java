package org.folio.rest.util;

import java.util.Arrays;
import java.util.Optional;

/** Represents the file formats supported for COUNTER report uploads. */
public enum ReportFileFormat {
  CSV(".csv"),
  JSON(".json"),
  XML(".xml");

  private final String extension;

  ReportFileFormat(String extension) {
    this.extension = extension;
  }

  /**
   * Determines the {@link ReportFileFormat} based on the given filename.
   *
   * <p>This method extracts the file extension from the provided filename and matches it against
   * the supported formats defined in this enum. The comparison is case-insensitive.
   *
   * @param filename The full name of the file, including its extension.
   * @return The matching {@link ReportFileFormat} enum value.
   * @throws IllegalArgumentException If the filename is null, doesn't contain a dot, or has an
   *     unsupported file extension.
   */
  public static ReportFileFormat fromFilename(String filename) {
    String extension =
        Optional.ofNullable(filename)
            .filter(f -> f.contains("."))
            .map(f -> f.substring(f.lastIndexOf(".")))
            .orElseThrow(() -> new IllegalArgumentException("Invalid filename"));

    return Arrays.stream(values())
        .filter(format -> format.extension.equalsIgnoreCase(extension))
        .findFirst()
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    "Unsupported file extension: %s".formatted(extension)));
  }
}
