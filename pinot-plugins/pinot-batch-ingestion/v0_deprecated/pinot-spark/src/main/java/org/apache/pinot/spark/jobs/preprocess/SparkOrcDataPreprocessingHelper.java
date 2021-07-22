package org.apache.pinot.spark.jobs.preprocess;

import org.apache.pinot.ingestion.preprocess.DataPreprocessingHelper;
import org.apache.spark.sql.SparkSession;


public class SparkOrcDataPreprocessingHelper extends SparkDataPreprocessingHelper {
  public SparkOrcDataPreprocessingHelper(DataPreprocessingHelper dataPreprocessingHelper) {
    super(dataPreprocessingHelper);
  }

  @Override
  public void setUpAndExecuteJob(SparkSession sparkSession) {

  }
}
