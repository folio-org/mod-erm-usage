package org.folio.rest.impl;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import org.folio.rest.jaxrs.model.Metadata;
import org.folio.rest.jaxrs.model.UsageDataProvider;
import org.junit.Test;

public class AggregatorSettingsTest {

  @Test
  public void testGetCredentialsCSVMetadata() throws JsonProcessingException, ParseException {
    String created = "2019-04-30T08:39:18.206+0000";
    String updated = "2019-04-30T09:39:18.206+0000";
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    Date createdDate = dateFormat.parse(created);
    Date updatedDate = dateFormat.parse(updated);
    UsageDataProvider provider =
        new UsageDataProvider()
            .withLabel("test")
            .withMetadata(new Metadata().withCreatedDate(createdDate).withUpdatedDate(updatedDate));

    String csv = AggregatorSettingsAPI.getCredentialsCSV(Collections.singletonList(provider));
    assertThat(csv).contains("test,,,,,,,,,\"" + created + "\",\"" + updated + "\"");
  }

  @Test
  public void testGetCredentialsCSVNoMetadata() throws JsonProcessingException {
    String csv =
        AggregatorSettingsAPI.getCredentialsCSV(
            Collections.singletonList(new UsageDataProvider().withLabel("test")));
    String csv2 =
        AggregatorSettingsAPI.getCredentialsCSV(
            Collections.singletonList(
                new UsageDataProvider().withLabel("test2").withMetadata(new Metadata())));
    assertThat(csv).contains("test,,,,,,,,,,");
    assertThat(csv2).contains("test2,,,,,,,,,,");
  }
}
