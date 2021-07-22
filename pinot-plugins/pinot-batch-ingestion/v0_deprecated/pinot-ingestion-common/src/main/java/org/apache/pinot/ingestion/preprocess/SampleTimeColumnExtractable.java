package org.apache.pinot.ingestion.preprocess;

import java.io.IOException;


public interface SampleTimeColumnExtractable {

  String getSampleTimeColumnValue(String timeColumnName) throws IOException;
}
