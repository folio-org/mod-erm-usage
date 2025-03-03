package org.folio.rest.impl3;

import org.folio.rest.Setup;
import org.junit.jupiter.api.Nested;

@Setup
public class ITSuite {

  @Nested
  class CounterReportR5UploadITNested extends CounterReportR5UploadIT {}

  @Nested
  class CounterReportExportITNested extends CounterReportExportIT {}
}
