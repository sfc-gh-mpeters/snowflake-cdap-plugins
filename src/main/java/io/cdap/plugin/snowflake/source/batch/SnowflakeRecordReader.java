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

package io.cdap.plugin.snowflake.source.batch;

import au.com.bytecode.opencsv.CSVReader;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * RecordReader implementation, which reads object from Snowflake.
 */
public class SnowflakeRecordReader extends RecordReader<NullWritable, Map<String, String>> {
  private static final Logger LOG = LoggerFactory.getLogger(SnowflakeRecordReader.class);

  private final String stageSplit;
  private final SnowflakeSourceAccessor snowflakeAccessor;
  private CSVReader csvReader;
  private String[] headers;
  private String[] nextLine;
  private long totalReadTime = 0;
  private long totalTransformTime = 0;

  public SnowflakeRecordReader(String stageSplit, SnowflakeSourceAccessor snowflakeAccessor) {
    this.stageSplit = stageSplit;
    this.snowflakeAccessor = snowflakeAccessor;
  }

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException {
    LOG.info(String.format("[%s] initA", stageSplit));
    this.csvReader = snowflakeAccessor.buildCsvReader(stageSplit);
    this.headers = csvReader.readNext();
    LOG.info(String.format("[%s] initB", stageSplit));
  }

  @Override
  public boolean nextKeyValue() throws IOException {
    long l = System.currentTimeMillis();
    nextLine = csvReader.readNext();
    long time = System.currentTimeMillis() - l;
    totalReadTime += time;

    return nextLine != null;
  }

  @Override
  public NullWritable getCurrentKey() {
    return NullWritable.get();
  }

  @Override
  public Map<String, String> getCurrentValue() {
    long l = System.currentTimeMillis();
    Map<String, String> result = new HashMap<>();
    for (int i = 0; i < headers.length; i++) {
      result.put(headers[i], nextLine[i]);
    }
    long time = System.currentTimeMillis() - l;
    totalTransformTime += time;
    return result;
  }

  @Override
  public float getProgress() {
    return 0;
  }

  @Override
  public void close() throws IOException {
    LOG.info(String.format("[%s] So/Close read:%d tr:%d", stageSplit, totalReadTime, totalTransformTime));
    if (csvReader != null) {
      csvReader.close();
    }
    LOG.info(String.format("%s [So/Close] a", stageSplit));
    snowflakeAccessor.removeStageFile(stageSplit);
    LOG.info(String.format("%s [So/Close] b", stageSplit));
  }
}
