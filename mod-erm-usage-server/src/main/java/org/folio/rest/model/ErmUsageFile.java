package org.folio.rest.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import javax.validation.constraints.NotNull;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"id", "data"})
public class ErmUsageFile {

  /**
   * (Required)
   */
  @JsonProperty("id")
  @NotNull
  private String id;

  /**
   * The data
   */
  @JsonProperty("data")
  @JsonPropertyDescription("The stored data")
  private String data;

  /**
   * (Required)
   */
  @JsonProperty("id")
  public String getId() {
    return id;
  }

  /**
   * (Required)
   */
  @JsonProperty("id")
  public void setId(String id) {
    this.id = id;
  }

  public ErmUsageFile withId(String id) {
    this.id = id;
    return this;
  }

  /**
   * The data
   */
  @JsonProperty("data")
  public String getData() {
    return data;
  }

  /**
   * The data
   */
  @JsonProperty("data")
  public void setData(String data) {
    this.data = data;
  }

  public ErmUsageFile withData(String data) {
    this.data = data;
    return this;
  }

}
