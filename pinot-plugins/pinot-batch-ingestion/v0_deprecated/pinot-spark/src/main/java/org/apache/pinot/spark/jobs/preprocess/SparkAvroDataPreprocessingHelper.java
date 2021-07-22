package org.apache.pinot.spark.jobs.preprocess;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.pinot.ingestion.preprocess.DataPreprocessingHelper;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.collection.JavaConverters;


public class SparkAvroDataPreprocessingHelper extends SparkDataPreprocessingHelper {
  public SparkAvroDataPreprocessingHelper(DataPreprocessingHelper dataPreprocessingHelper) {
    super(dataPreprocessingHelper);
  }

  @Override
  public void setUpAndExecuteJob(SparkSession sparkSession) {

    JavaConverters.asJavaCollectionConverter(convertPathsToStrings(_dataPreprocessingHelper._inputDataPaths))
    sparkSession.read().format("avro").load(convertPathsToStrings(_dataPreprocessingHelper._inputDataPaths));

  }

  @Override
  public void setUpAndExecuteJob(JavaSparkContext sparkContext) {
    JavaRDD<String> inputRDD = sparkContext.textFile(_dataPreprocessingHelper._inputDataPaths.toString(), _dataPreprocessingHelper._numOutputFiles);
  }

  private Iterable<String> convertPathsToStrings(List<Path> paths) {
    List<String> stringList = new ArrayList<>();
    for (Path path : paths) {
      stringList.add(path.toString());
    }
    stringList.it
    return stringList.iterator();
  }
}
