package org.folio.rest.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class ReportFileFormatTest {

  @ParameterizedTest
  @CsvSource({
    "report.CSV, CSV",
    "data.JSON, JSON",
    "file.XML, XML",
    "report.csv, CSV",
    "data.json, JSON",
    "file.xml, XML"
  })
  void fromFilenameShouldReturnCorrectFormat(String filename, ReportFileFormat expectedFormat) {
    assertThat(ReportFileFormat.fromFilename(filename)).isEqualTo(expectedFormat);
  }

  @ParameterizedTest
  @ValueSource(strings = {"report.txt", "data.pdf", "file.docx"})
  void fromFilenameShouldThrowExceptionForUnsupportedExtension(String filename) {
    assertThatThrownBy(() -> ReportFileFormat.fromFilename(filename))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Unsupported file extension:");
  }

  @Test
  void fromFilenameShouldThrowExceptionForNullFilename() {
    assertThatThrownBy(() -> ReportFileFormat.fromFilename(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid filename");
  }

  @Test
  void fromFilenameShouldThrowExceptionForFilenameWithoutExtension() {
    assertThatThrownBy(() -> ReportFileFormat.fromFilename("reportwithoutextension"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid filename");
  }

  @Test
  void fromFilenameShouldHandleFilenamesWithMultipleDots() {
    assertThat(ReportFileFormat.fromFilename("report.2023.04.csv")).isEqualTo(ReportFileFormat.CSV);
  }
}
