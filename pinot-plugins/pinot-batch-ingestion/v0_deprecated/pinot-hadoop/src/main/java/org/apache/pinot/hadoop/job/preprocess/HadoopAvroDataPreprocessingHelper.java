package org.apache.pinot.hadoop.job.preprocess;

import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.pinot.ingestion.preprocess.DataPreprocessingHelper;
import org.apache.pinot.ingestion.preprocess.mappers.AvroDataPreprocessingMapper;
import org.apache.pinot.ingestion.preprocess.reducers.AvroDataPreprocessingReducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HadoopAvroDataPreprocessingHelper extends HadoopDataPreprocessingHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(HadoopAvroDataPreprocessingHelper.class);

  public HadoopAvroDataPreprocessingHelper(DataPreprocessingHelper dataPreprocessingHelper) {
    super(dataPreprocessingHelper);
  }

  @Override
  public void setUpMapperReducerConfigs(Job job)
      throws IOException {
    Schema avroSchema = (Schema) getSchema(_dataPreprocessingHelper._sampleRawDataPath);
    LOGGER.info("Avro schema is: {}", avroSchema.toString(true));
    validateConfigsAgainstSchema(avroSchema);

    job.setInputFormatClass(AvroKeyInputFormat.class);
    job.setMapperClass(AvroDataPreprocessingMapper.class);

    job.setReducerClass(AvroDataPreprocessingReducer.class);
    AvroMultipleOutputs.addNamedOutput(job, "avro", AvroKeyOutputFormat.class, avroSchema);
    AvroMultipleOutputs.setCountersEnabled(job, true);
    // Use LazyOutputFormat to avoid creating empty files.
    LazyOutputFormat.setOutputFormatClass(job, AvroKeyOutputFormat.class);
    job.setOutputKeyClass(AvroKey.class);
    job.setOutputValueClass(NullWritable.class);

    AvroJob.setInputKeySchema(job, avroSchema);
    AvroJob.setMapOutputValueSchema(job, avroSchema);
    AvroJob.setOutputKeySchema(job, avroSchema);
  }
}
