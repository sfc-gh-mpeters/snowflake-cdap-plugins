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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.snowflake.common.util.SchemaHelper;
import org.apache.hadoop.io.NullWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Source plugin to read data from Snowflake.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(SnowflakeBatchSource.NAME)
@Description("Read data from Snowflake.")
public class SnowflakeBatchSource extends BatchSource<NullWritable, Map<String, String>, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(SnowflakeBatchSource.class);

  public static final String NAME = "Snowflake";

  private final SnowflakeBatchSourceConfig config;
  private SnowflakeMapToRecordTransformer transformer;

  public SnowflakeBatchSource(SnowflakeBatchSourceConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    FailureCollector failureCollector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    config.validate(failureCollector);

    SnowflakeSourceAccessor snowflakeAccessor = new SnowflakeSourceAccessor(config);
    Schema schema = SchemaHelper.getSchema(snowflakeAccessor, config.getSchema(),
                                           failureCollector, config.getImportQuery());
    failureCollector.getOrThrowException();

    pipelineConfigurer.getStageConfigurer().setOutputSchema(schema);
  }

  @Override
  public void prepareRun(BatchSourceContext context) {
    FailureCollector failureCollector = context.getFailureCollector();
    config.validate(failureCollector);

    SnowflakeSourceAccessor snowflakeAccessor = new SnowflakeSourceAccessor(config);
    Schema schema = SchemaHelper.getSchema(snowflakeAccessor, config.getSchema(),
                                           failureCollector, config.getImportQuery());
    failureCollector.getOrThrowException();

    LineageRecorder lineageRecorder = new LineageRecorder(context, config.getReferenceName());
    lineageRecorder.createExternalDataset(schema);
    if (schema.getFields() != null && !schema.getFields().isEmpty()) {
      lineageRecorder.recordRead("Read", "Read from Snowflake",
                                 schema.getFields().stream()
                                   .map(Schema.Field::getName)
                                   .collect(Collectors.toList()));
    }

    context.setInput(Input.of(config.getReferenceName(), new SnowflakeInputFormatProvider(config)));
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    SnowflakeSourceAccessor snowflakeAccessor = new SnowflakeSourceAccessor(config);
    Schema schema = SchemaHelper.getSchema(snowflakeAccessor, config.getImportQuery());
    this.transformer = new SnowflakeMapToRecordTransformer(schema);
  }

  private long totalTime = 0;
  private long number = 0;

  @Override
  public void transform(KeyValue<NullWritable, Map<String, String>> input,
                        Emitter<StructuredRecord> emitter) {
    long l = System.currentTimeMillis();
    StructuredRecord record = transformer.transform(input.getValue());
    emitter.emit(record);
    long time = System.currentTimeMillis() - l;
    totalTime += time;
    checkTime();
  }
  private void checkTime() {
    number++;
    if (number >= 100_000) {
      LOG.info(String.format("[So/TR] time: %d", totalTime));
      number = 0;
      totalTime = 0;
    }
  }
}
