package org.folio.rest.util;

import java.util.Arrays;
import java.util.Optional;

/**
 * Represents the file formats supported for COUNTER report uploads.
 *
 * <p>This enum defines the supported file extensions for COUNTER reports.
 *
 * <p>Supported formats include:
 *
 * <ul>
 *   <li>CSV - Comma-separated values with RFC4180 format
 *   <li>TSV - Tab-separated values with tab-delimited format
 *   <li>JSON - JSON format for COUNTER Release 5 and 5.1
 *   <li>XML - XML format for COUNTER Release 4
 *   <li>XLSX - Excel format converted to CSV for processing
 * </ul>
 */
public enum ReportFileFormat {
  CSV(".csv"),
  JSON(".json"),
  TSV(".tsv"),
  XML(".xml"),
  XLSX(".xlsx");

  private final String extension;

  ReportFileFormat(String extension) {
    this.extension = extension;
  }

  /**
   * Gets the file extension for this format.
   *
   * @return the file extension including the dot (e.g., ".csv")
   */
  public String getExtension() {
    return extension;
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
