package org.apache.pinot.spark.jobs;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import org.apache.hadoop.fs.Path;
import org.apache.pinot.ingestion.jobs.SegmentPreprocessingJob;
import org.apache.pinot.spark.jobs.preprocess.SparkDataPreprocessingHelper;
import org.apache.pinot.spark.jobs.preprocess.SparkDataPreprocessingHelperFactory;
import org.apache.pinot.spark.utils.HadoopUtils;
import org.apache.pinot.spark.utils.PinotSparkJobPreparationHelper;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Spark job which provides partitioning, sorting, and resizing against the input files, which is raw data in either Avro or Orc format.
 * Thus, the output files are partitioned, sorted, resized after this job.
 * In order to run this job, the following configs need to be specified in job properties:
 * * enable.preprocessing: false by default. Enables preprocessing job.
 */
public class SparkSegmentPreprocessingJob extends SegmentPreprocessingJob {
  private static final Logger LOGGER = LoggerFactory.getLogger(SparkSegmentPreprocessingJob.class);

  public SparkSegmentPreprocessingJob(Properties properties) {
    super(properties);
  }

  @Override
  protected void run()
      throws Exception {
    if (!_enablePreprocessing) {
      LOGGER.info("Pre-processing job is disabled.");
      return;
    } else {
      LOGGER.info("Starting {}", getClass().getSimpleName());
    }

    setTableConfigAndSchema();
    fetchPreProcessingOperations();
    fetchPartitioningConfig();
    fetchSortingConfig();
    fetchResizingConfig();

    // Cleans up preprocessed output dir if exists
    cleanUpPreprocessedOutputs(_preprocessedOutputDir);

    SparkDataPreprocessingHelper dataPreprocessingHelper =
        SparkDataPreprocessingHelperFactory.generateDataPreprocessingHelper(_inputSegmentDir, _preprocessedOutputDir);
    dataPreprocessingHelper
        .registerConfigs(_tableConfig, _pinotTableSchema, _partitionColumn, _numPartitions, _partitionFunction,
            _sortingColumn, _sortingColumnType, _numOutputFiles, _maxNumRecordsPerFile);

    // Set up and execute spark job.
    JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate());
    addDepsJarToDistributedCache(javaSparkContext);

    SparkSession sparkSession = SparkSession.builder().appName(SparkSegmentPreprocessingJob.class.getSimpleName()).getOrCreate();

    dataPreprocessingHelper.setUpAndExecuteJob(sparkSession);
  }

  public void setUpAndExecuteJob(JavaSparkContext sparkContext) {
  }

  /**
   * Cleans up outputs in preprocessed output directory.
   */
  public static void cleanUpPreprocessedOutputs(Path preprocessedOutputDir)
      throws IOException {
    if (HadoopUtils.DEFAULT_FILE_SYSTEM.exists(preprocessedOutputDir)) {
      LOGGER.warn("Found output folder {}, deleting", preprocessedOutputDir);
      HadoopUtils.DEFAULT_FILE_SYSTEM.delete(preprocessedOutputDir, true);
    }
  }

  protected void addDepsJarToDistributedCache(JavaSparkContext sparkContext)
      throws IOException {
    if (_pathToDependencyJar != null) {
      PinotSparkJobPreparationHelper
          .addDepsJarToDistributedCacheHelper(HadoopUtils.DEFAULT_FILE_SYSTEM, sparkContext,
              _pathToDependencyJar);
    }
  }
}
