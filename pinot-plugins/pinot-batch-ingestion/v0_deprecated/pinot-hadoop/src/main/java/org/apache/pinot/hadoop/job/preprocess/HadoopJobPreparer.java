package org.apache.pinot.hadoop.job.preprocess;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Job;


public interface HadoopJobPreparer {

  void setUpMapperReducerConfigs(Job job) throws IOException;
}
