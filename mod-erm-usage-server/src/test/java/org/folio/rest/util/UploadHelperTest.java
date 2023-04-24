package org.folio.rest.util;

import static org.assertj.core.api.Assertions.assertThatCode;

import io.vertx.core.json.Json;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.assertj.core.api.AbstractThrowableAssert;
import org.folio.rest.util.UploadHelper.FileUploadException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.openapitools.client.model.COUNTERTitleReport;
import org.openapitools.client.model.SUSHIReportHeader;
import org.openapitools.client.model.SUSHIReportHeaderReportAttributes;
import org.openapitools.client.model.SUSHIReportHeaderReportFilters;

@RunWith(Parameterized.class)
public class UploadHelperTest {

  private final String reportStr;
  private final boolean isValid;

  @Parameters
  public static Collection<Object[]> data() {
    return List.of(
        new Object[] {"Title Master Report", Collections.emptyMap(), false},
        new Object[] {
          "Title Master Report",
          Map.of("Attributes_To_Show", "Data_Type|Section_Type|YOP|Access_Type|Access_Method"),
          true
        },
        new Object[] {
          "Some Report",
          Map.of("Attributes_To_Show", "Data_Type|Section_Type|YOP|Access_Type|Access_Method"),
          false
        },
        new Object[] {
          "Title Master Report",
          Map.of("Attributes_To_Show", "Data_Type|Section_Type|YOP|Access_Type"),
          false
        },
        new Object[] {
          "Title Master Report",
          Map.of(
              "Attributes_To_Show",
              "Data_Type|Section_Type|YOP|Access_Type",
              "Exclude_Monthly_Details",
              "True"),
          false
        },
        new Object[] {
          "Item Master Report",
          Map.of(
              "Attributes_To_Show",
              "Authors|Publication_Date|Article_Version|Data_Type|YOP|Access_Type|Access_Method"),
          false
        },
        new Object[] {
          "Item Master Report",
          Map.of(
              "Attributes_To_Show",
              "Authors|Publication_Date|Article_Version|Data_Type|YOP|Access_Type|Access_Method",
              "Include_Parent_Details",
              "True"),
          true
        });
  }

  public UploadHelperTest(String name, Map<String, String> attributes, boolean isValid) {
    this.reportStr =
        Json.encode(
            new COUNTERTitleReport().reportHeader(createSUSHIReportHeader(name, attributes)));
    this.isValid = isValid;
  }

  private SUSHIReportHeader createSUSHIReportHeader(
      String name, Map<String, String> headerAttributeMap) {
    return new SUSHIReportHeader()
        .reportName(name)
        .reportAttributes(
            headerAttributeMap.entrySet().stream()
                .map(
                    e ->
                        new SUSHIReportHeaderReportAttributes()
                            .name(e.getKey())
                            .value(e.getValue()))
                .collect(
                    Collectors.collectingAndThen(
                        Collectors.toList(), l -> (l.isEmpty()) ? null : l)))
        .reportFilters(
            List.of(
                new SUSHIReportHeaderReportFilters().name("Begin_Date").value("2022-01-01"),
                new SUSHIReportHeaderReportFilters().name("End_Date").value("2022-01-31")))
        .reportID("TR")
        .release("5");
  }

  @Test
  public void testReportHeaders() {
    AbstractThrowableAssert<?, ? extends Throwable> abstractThrowableAssert =
        assertThatCode(() -> UploadHelper.getCounterReportsFromString(reportStr));
    if (isValid) {
      abstractThrowableAssert.doesNotThrowAnyException();
    } else {
      abstractThrowableAssert.isInstanceOf(FileUploadException.class);
    }
  }
}
