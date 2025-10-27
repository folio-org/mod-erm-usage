package org.folio.rest.util;

import java.util.List;
import org.folio.rest.jaxrs.model.CounterReport;

public interface ReportUploadProcessor {

  List<CounterReport> process(String reportData) throws ReportUploadException;
}
