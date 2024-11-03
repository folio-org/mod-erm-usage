package org.folio.rest.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.folio.rest.util.ReportExportHelper.CREATED_BY_SUFFIX;
import static org.folio.rest.util.ReportExportHelper.createDownloadResponseByReportVersion;
import static org.folio.rest.util.ReportExportHelper.replaceCreatedBy;

import javax.ws.rs.core.MediaType;
import org.folio.rest.jaxrs.model.CounterReport;
import org.folio.rest.jaxrs.model.Report;
import org.junit.Test;
import org.olf.erm.usage.counter41.Counter4Utils;

public class ReportExportHelperTest {

  @Test
  public void testReplaceCreatedBy() {
    assertThat(replaceCreatedBy("Created_By,Provider"))
        .isEqualTo("Created_By,Provider " + CREATED_BY_SUFFIX);
    assertThat(replaceCreatedBy("\n\nCreated_By,Provider\n"))
        .isEqualTo("\n\nCreated_By,Provider " + CREATED_BY_SUFFIX + "\n");
    assertThat(replaceCreatedBy("Created_By,Provider " + CREATED_BY_SUFFIX))
        .isEqualTo("Created_By,Provider " + CREATED_BY_SUFFIX);
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

  @Test
  public void testCreateDownloadResponseByReportVersion() {
    assertThat(
            createDownloadResponseByReportVersion(
                new CounterReport().withRelease("4").withReport(new Report())))
        .satisfies(
            response -> {
              assertThat(response.getMediaType()).isEqualTo(MediaType.APPLICATION_XML_TYPE);
              assertThat(response.getEntity()).isEqualTo(Counter4Utils.toXML("{}"));
            });

    assertThat(
            createDownloadResponseByReportVersion(
                new CounterReport().withRelease("4").withReport(null)))
        .isNull();

    assertThat(
            createDownloadResponseByReportVersion(
                new CounterReport().withRelease("5").withReport(new Report())))
        .satisfies(
            response -> {
              assertThat(response.getMediaType()).isEqualTo(MediaType.APPLICATION_JSON_TYPE);
              assertThat(response.getEntity()).isEqualTo("{}");
            });

    assertThat(
            createDownloadResponseByReportVersion(
                new CounterReport().withRelease("5").withReport(null)))
        .isNull();
  }
}
