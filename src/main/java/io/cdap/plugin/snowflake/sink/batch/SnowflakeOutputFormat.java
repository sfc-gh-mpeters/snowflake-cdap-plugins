/*
 * Copyright © 2020 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.cdap.plugin.snowflake.sink.batch;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * An OutputFormat that sends the output of a Hadoop job to the Snowflake record writer, also
 * it defines the output committer.
 */
public class SnowflakeOutputFormat extends OutputFormat<NullWritable, CSVRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(SnowflakeOutputFormat.class);
  private static final Gson GSON = new GsonBuilder().create();

  @Override
  public RecordWriter<NullWritable, CSVRecord> getRecordWriter(TaskAttemptContext taskAttemptContext)
    throws IOException {
      return new SnowflakeRecordWriter(taskAttemptContext);
  }

  @Override
  public void checkOutputSpecs(JobContext jobContext) {
    //no-op
  }

  /**
   * Used to start Snowflake job when Mapreduce job is started,
   * and to close Snowflake job when Mapreduce job is finished.
   */
  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) {
    return new OutputCommitter() {
      @Override
      public void setupJob(JobContext jobContext) {
      }

      @Override
      public void commitJob(JobContext jobContext) throws IOException {
        Configuration conf = jobContext.getConfiguration();
        String configJson = conf.get(
          SnowflakeOutputFormatProvider.PROPERTY_CONFIG_JSON);
        SnowflakeSinkConfig config = GSON.fromJson(
          configJson, SnowflakeSinkConfig.class);

        SnowflakeSinkAccessor snowflakeAccessor = new SnowflakeSinkAccessor(config);
        snowflakeAccessor.populateTable();
      }

      @Override
      public void setupTask(TaskAttemptContext taskAttemptContext) {

      }

      @Override
      public boolean needsTaskCommit(TaskAttemptContext taskAttemptContext) {
        return true;
      }

      @Override
      public void commitTask(TaskAttemptContext taskAttemptContext) {

      }

      @Override
      public void abortTask(TaskAttemptContext taskAttemptContext) {

      }
    };
  }
}
