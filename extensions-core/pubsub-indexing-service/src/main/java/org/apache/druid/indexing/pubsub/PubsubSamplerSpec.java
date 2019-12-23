/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.indexing.pubsub;

import com.google.common.base.Preconditions;
import org.apache.druid.data.input.ByteBufferInputRowParser;
import org.apache.druid.data.input.FiniteFirehoseFactory;
import org.apache.druid.data.input.Firehose;
import org.apache.druid.data.input.FirehoseFactoryToInputSourceAdaptor;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.indexing.overlord.sampler.SamplerSpec;
import org.apache.druid.indexing.overlord.sampler.InputSourceSampler;
import org.apache.druid.indexing.overlord.sampler.SamplerConfig;
import org.apache.druid.indexing.pubsub.supervisor.PubsubSupervisorIOConfig;
import org.apache.druid.indexing.pubsub.supervisor.PubsubSupervisorSpec;
import org.apache.druid.indexing.pubsub.supervisor.PubsubSupervisorTuningConfig;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.SplitHintSpec;
import org.apache.druid.indexing.overlord.sampler.SamplerResponse;
import org.apache.druid.java.util.common.parsers.CloseableIterator;

import java.util.HashMap;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.stream.Stream;


public class PubsubSamplerSpec implements SamplerSpec
{
  static final long POLL_TIMEOUT_MS = 100;
  private final ObjectMapper objectMapper;

  @Nullable
  private final DataSchema dataSchema;
  private final InputSourceSampler inputSourceSampler;

  protected final PubsubSupervisorIOConfig ioConfig;
  @Nullable
  protected final PubsubSupervisorTuningConfig tuningConfig;
  protected final SamplerConfig samplerConfig;

  public PubsubSamplerSpec(
      final PubsubSupervisorSpec ingestionSpec,
      @Nullable final SamplerConfig samplerConfig,
      final InputSourceSampler inputSourceSampler
  )
  {
    this.dataSchema = Preconditions.checkNotNull(ingestionSpec, "[spec] is required").getDataSchema();
    this.ioConfig = Preconditions.checkNotNull(ingestionSpec.getIoConfig(), "[spec.ioConfig] is required");
    this.tuningConfig = ingestionSpec.getTuningConfig();
    this.samplerConfig = samplerConfig == null ? SamplerConfig.empty() : samplerConfig;
    this.inputSourceSampler = inputSourceSampler;
  }


  @JsonCreator
  public PubsubSamplerSpec(
      @JsonProperty("spec") final PubsubSupervisorSpec ingestionSpec,
      @JsonProperty("samplerConfig") @Nullable final SamplerConfig samplerConfig,
      @JacksonInject InputSourceSampler inputSourceSampler,
      @JacksonInject ObjectMapper objectMapper
  )
  {
    this(ingestionSpec, samplerConfig, inputSourceSampler);

    this.objectMapper = objectMapper;
  }

  @Override
  public SamplerResponse sample()
  {
    final InputSource inputSource;
    final InputFormat inputFormat;
    if (dataSchema.getParser() != null) {
      inputSource = new FirehoseFactoryToInputSourceAdaptor(
          new PubsubSamplerFirehoseFactory(),
          dataSchema.getParser()
      );
      inputFormat = null;
    } else {
      inputSource = new RecordSupplierInputSource<>(
          ioConfig.getStream(),
          createRecordSupplier(),
          ioConfig.isUseEarliestSequenceNumber()
      );
      inputFormat = Preconditions.checkNotNull(
          ioConfig.getInputFormat(null),
          "[spec.ioConfig.inputFormat] is required"
      );
    }

    return inputSourceSampler.sample(inputSource, inputFormat, dataSchema, samplerConfig);
  }

  private class PubsubSamplerFirehoseFactory implements FiniteFirehoseFactory<ByteBufferInputRowParser, Object>
  {
    @Override
    public Firehose connect(ByteBufferInputRowParser parser, @Nullable File temporaryDirectory)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public Firehose connectForSampler(ByteBufferInputRowParser parser, @Nullable File temporaryDirectory)
    {
      return new PubsubSamplerFirehose(parser);
    }

    @Override
    public boolean isSplittable()
    {
      return false;
    }

    @Override
    public Stream<InputSplit<Object>> getSplits(@Nullable SplitHintSpec splitHintSpec)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getNumSplits(@Nullable SplitHintSpec splitHintSpec)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public FiniteFirehoseFactory withSplit(InputSplit split)
    {
      throw new UnsupportedOperationException();
    }
  }

  private class PubsubSamplerFirehose implements Firehose
  {
    private final InputRowParser parser;
    private final CloseableIterator<InputEntity> entityIterator;

    protected PubsubSamplerFirehose(InputRowParser parser)
    {
      this.parser = parser;
      if (parser instanceof StringInputRowParser) {
        ((StringInputRowParser) parser).startFileFromBeginning();
      }

      RecordSupplierInputSource<PartitionIdType, SequenceOffsetType> inputSource = new RecordSupplierInputSource<>(
          ioConfig.getStream(),
          createRecordSupplier(),
          ioConfig.isUseEarliestSequenceNumber()
      );
      this.entityIterator = inputSource.createEntityIterator();
    }

    @Override
    public boolean hasMore()
    {
      return true;
    }

    @Override
    public InputRow nextRow()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public InputRowListPlusRawValues nextRowWithRaw()
    {
      final ByteBuffer bb = ((ByteEntity) entityIterator.next()).getBuffer();

      final Map<String, Object> rawColumns;
      try {
        if (parser instanceof StringInputRowParser) {
          rawColumns = ((StringInputRowParser) parser).buildStringKeyMap(bb);
        } else {
          rawColumns = null;
        }
      }
      catch (ParseException e) {
        return InputRowListPlusRawValues.of(null, e);
      }

      try {
        final List<InputRow> rows = parser.parseBatch(bb);
        return InputRowListPlusRawValues.of(rows.isEmpty() ? null : rows, rawColumns);
      }
      catch (ParseException e) {
        return InputRowListPlusRawValues.of(rawColumns, e);
      }
    }

    @Override
    public void close() throws IOException
    {
      entityIterator.close();
    }
  }

  @Override
  protected PubsubRecordSupplier createRecordSupplier()
  {
    ClassLoader currCtxCl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

      final Map<String, Object> props = new HashMap<>(((PubsubSupervisorIOConfig) ioConfig).getConsumerProperties());

      props.put("enable.auto.commit", "false");
      props.put("auto.offset.reset", "none");
      props.put("request.timeout.ms", Integer.toString(samplerConfig.getTimeoutMs()));

      return new PubsubRecordSupplier(props, objectMapper);
    }
    finally {
      Thread.currentThread().setContextClassLoader(currCtxCl);
    }
  }
}
