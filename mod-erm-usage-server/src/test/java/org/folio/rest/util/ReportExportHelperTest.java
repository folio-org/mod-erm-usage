package org.folio.rest.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.folio.rest.util.ReportExportHelper.CREATED_BY_SUFFIX;
import static org.folio.rest.util.ReportExportHelper.replaceCreatedBy;

import org.junit.Test;

public class ReportExportHelperTest {

  @Test
  public void testReplaceCreatedBy() {
    assertThat(replaceCreatedBy("Created_By,Provider"))
        .isEqualTo("Created_By,Provider " + CREATED_BY_SUFFIX);
    assertThat(replaceCreatedBy("\n\nCreated_By,Provider\n"))
        .isEqualTo("\n\nCreated_By,Provider " + CREATED_BY_SUFFIX + "\n");
    assertThat(replaceCreatedBy("Created_By,Provider " + CREATED_BY_SUFFIX + ""))
        .isEqualTo("Created_By,Provider " + CREATED_BY_SUFFIX + "");
    assertThat(replaceCreatedBy("Created_By,\"Provider, named abc\""))
        .isEqualTo("Created_By,\"Provider, named abc " + CREATED_BY_SUFFIX + "\"");
    assertThat(replaceCreatedBy("Created_By,\"Provider, named abc " + CREATED_BY_SUFFIX + "\""))
        .isEqualTo("Created_By,\"Provider, named abc " + CREATED_BY_SUFFIX + "\"");
    assertThat(replaceCreatedBy("Created_By,\"\"Provider, named abc\"\""))
        .isEqualTo("Created_By,\"\"Provider, named abc\" " + CREATED_BY_SUFFIX + "\"");
    assertThat(replaceCreatedBy("Created_By,\"\"\"Provider, named abc\"\"\""))
        .isEqualTo("Created_By,\"\"\"Provider, named abc\"\" " + CREATED_BY_SUFFIX + "\"");
    assertThat(replaceCreatedBy("Created_By,\"Provider \"\"\""))
        .isEqualTo("Created_By,\"Provider \"\" " + CREATED_BY_SUFFIX + "\"");
    assertThat(replaceCreatedBy("Created_By,")).isEqualTo("Created_By, " + CREATED_BY_SUFFIX);
    assertThat(replaceCreatedBy("Created_by,")).isEqualTo("Created_by,");
    assertThat(replaceCreatedBy(null)).isNull();
  }
}
