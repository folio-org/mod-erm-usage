package org.folio.rest.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class ReportReleaseVersionTest {

  @ParameterizedTest
  @CsvSource({"4, R4", "5, R5", "5.1, R51"})
  void fromVersionShouldReturnCorrectEnum(String version, ReportReleaseVersion expected) {
    assertThat(ReportReleaseVersion.fromVersion(version)).isEqualTo(expected);
  }

  @ParameterizedTest
  @ValueSource(strings = {"3", "6", "5.2", ""})
  void fromVersionShouldThrowExceptionForUnknownVersion(String version) {
    assertThatThrownBy(() -> ReportReleaseVersion.fromVersion(version))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unknown version: " + version);
  }

  @Test
  void getVersionShouldReturnCorrectVersion() {
    assertThat(ReportReleaseVersion.R4.getVersion()).isEqualTo("4");
    assertThat(ReportReleaseVersion.R5.getVersion()).isEqualTo("5");
    assertThat(ReportReleaseVersion.R51.getVersion()).isEqualTo("5.1");
  }

  @Test
  void valuesShouldContainAllEnumValues() {
    assertThat(ReportReleaseVersion.values())
        .containsExactlyInAnyOrder(
            ReportReleaseVersion.R4, ReportReleaseVersion.R5, ReportReleaseVersion.R51);
  }

  @Test
  void fromVersionShouldThrowExceptionForNullVersion() {
    assertThatThrownBy(() -> ReportReleaseVersion.fromVersion(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unknown version: " + null);
  }
}
