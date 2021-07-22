package org.apache.pinot.spark.jobs.preprocess;

import org.apache.pinot.ingestion.preprocess.DataPreprocessingHelper;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.spark.sql.SparkSession;


public abstract class SparkDataPreprocessingHelper {

  protected DataPreprocessingHelper _dataPreprocessingHelper;

  public SparkDataPreprocessingHelper(DataPreprocessingHelper dataPreprocessingHelper) {
    _dataPreprocessingHelper = dataPreprocessingHelper;
  }

  public void registerConfigs(TableConfig tableConfig, Schema tableSchema, String partitionColumn, int numPartitions,
      String partitionFunction, String sortingColumn, FieldSpec.DataType sortingColumnType, int numOutputFiles,
      int maxNumRecordsPerFile) {
    _dataPreprocessingHelper
        .registerConfigs(tableConfig, tableSchema, partitionColumn, numPartitions, partitionFunction, sortingColumn,
            sortingColumnType, numOutputFiles, maxNumRecordsPerFile);
  }

  public abstract void setUpAndExecuteJob(SparkSession sparkSession);
}
