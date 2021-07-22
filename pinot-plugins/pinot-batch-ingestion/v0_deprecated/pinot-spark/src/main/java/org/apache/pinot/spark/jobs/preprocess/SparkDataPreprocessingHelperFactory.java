package org.apache.pinot.spark.jobs.preprocess;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.pinot.ingestion.preprocess.AvroDataPreprocessingHelper;
import org.apache.pinot.ingestion.preprocess.OrcDataPreprocessingHelper;
import org.apache.pinot.ingestion.utils.preprocess.DataFileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SparkDataPreprocessingHelperFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(SparkDataPreprocessingHelperFactory.class);

  public static SparkDataPreprocessingHelper generateDataPreprocessingHelper(Path inputPaths, Path outputPath)
      throws IOException {
    final List<Path> avroFiles = DataFileUtils.getDataFiles(inputPaths, DataFileUtils.AVRO_FILE_EXTENSION);
    final List<Path> orcFiles = DataFileUtils.getDataFiles(inputPaths, DataFileUtils.ORC_FILE_EXTENSION);

    int numAvroFiles = avroFiles.size();
    int numOrcFiles = orcFiles.size();
    Preconditions.checkState(numAvroFiles == 0 || numOrcFiles == 0,
        "Cannot preprocess mixed AVRO files: %s and ORC files: %s in directories: %s", avroFiles, orcFiles,
        inputPaths);
    Preconditions
        .checkState(numAvroFiles > 0 || numOrcFiles > 0, "Failed to find any AVRO or ORC file in directories: %s",
            inputPaths);

    if (numAvroFiles > 0) {
      LOGGER.info("Found AVRO files: {} in directories: {}", avroFiles, inputPaths);
      return new SparkAvroDataPreprocessingHelper(new AvroDataPreprocessingHelper(avroFiles, outputPath));
    } else {
      LOGGER.info("Found ORC files: {} in directories: {}", orcFiles, inputPaths);
      return new SparkOrcDataPreprocessingHelper(new OrcDataPreprocessingHelper(orcFiles, outputPath));
    }
  }
}
