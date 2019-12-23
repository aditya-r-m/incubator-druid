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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.base.Optional;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.data.input.InputFormat;
import org.joda.time.DateTime;
import org.apache.druid.segment.indexing.IOConfig;

import javax.annotation.Nullable;
import java.util.Map;

public class PubsubIndexTaskIOConfig implements IOConfig
{
  @Nullable
  private final Integer taskGroupId;
  private final Map<String, Object> consumerProperties;
  private final long pollTimeout;
  private final DateTime minimumMessageTime;
  private final DateTime maximumMessageTime;
  private final InputFormat inputFormat;

  @JsonCreator
  public PubsubIndexTaskIOConfig(
      @JsonProperty("taskGroupId") @Nullable Integer taskGroupId, // can be null for backward compabitility
      @JsonProperty("consumerProperties") Map<String, Object> consumerProperties,
      @JsonProperty("pollTimeout") Long pollTimeout,
      @JsonProperty("minimumMessageTime") DateTime minimumMessageTime,
      @JsonProperty("maximumMessageTime") DateTime maximumMessageTime,
      @JsonProperty("inputFormat") @Nullable InputFormat inputFormat
  )
  {
    this.taskGroupId = taskGroupId;
    this.consumerProperties = Preconditions.checkNotNull(consumerProperties, "consumerProperties");
    this.pollTimeout = pollTimeout != null ? pollTimeout : KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS;
    this.minimumMessageTime = minimumMessageTime;
    this.maximumMessageTime = maximumMessageTime;
    this.inputFormat = inputFormat;
  }

  @Nullable
  @JsonProperty
  public Integer getTaskGroupId()
  {
    return taskGroupId;
  }

  @JsonProperty
  public Map<String, Object> getConsumerProperties()
  {
    return consumerProperties;
  }

  @JsonProperty
  public Optional<DateTime> getMaximumMessageTime()
  {
    return maximumMessageTime;
  }

  @JsonProperty
  public Optional<DateTime> getMinimumMessageTime()
  {
    return minimumMessageTime;
  }

  @Nullable
  @JsonProperty("inputFormat")
  private InputFormat getGivenInputFormat()
  {
    return inputFormat;
  }

  @Nullable
  @JsonProperty("pollTimeout")
  private long getPollTimeout()
  {
    return pollTimeout;
  }

  @Override
  public String toString()
  {
    return "PubsubIndexTaskIOConfig{" +
           "taskGroupId=" + getTaskGroupId() +
           ", consumerProperties=" + getConsumerProperties() +
           ", pollTimeout=" + getPollTimeout() +
           ", minimumMessageTime=" + getMinimumMessageTime() +
           ", maximumMessageTime=" + getMaximumMessageTime() +
           '}';
  }
}
