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

package org.apache.druid.indexing.pubsub.supervisor;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.indexing.common.stats.RowIngestionMetersFactory;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.supervisor.Supervisor;
import org.apache.druid.indexing.overlord.supervisor.SupervisorSpec;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStateManagerConfig;
import org.apache.druid.indexing.pubsub.PubsubIndexTaskClientFactory;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.server.metrics.DruidMonitorSchedulerConfig;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

public class PubsubSupervisorSpec implements SupervisorSpec
{
  private static final String TASK_TYPE = "pubsub";

  private static PubsubSupervisorIngestionSpec checkIngestionSchema(
      PubsubSupervisorIngestionSpec ingestionSchema
  )
  {
    Preconditions.checkNotNull(ingestionSchema, "ingestionSchema");
    Preconditions.checkNotNull(ingestionSchema.getDataSchema(), "dataSchema");
    Preconditions.checkNotNull(ingestionSchema.getIOConfig(), "ioConfig");
    return ingestionSchema;
  }

  protected final TaskStorage taskStorage;
  protected final TaskMaster taskMaster;
  protected final IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator;
  protected final PubsubIndexTaskClientFactory indexTaskClientFactory;
  protected final ObjectMapper mapper;
  protected final RowIngestionMetersFactory rowIngestionMetersFactory;
  private final PubsubSupervisorIngestionSpec ingestionSchema;
  @Nullable
  private final Map<String, Object> context;
  protected final ServiceEmitter emitter;
  protected final DruidMonitorSchedulerConfig monitorSchedulerConfig;
  private final boolean suspended;
  protected final SupervisorStateManagerConfig supervisorStateManagerConfig;

  @JsonCreator
  public PubsubSupervisorSpec(
      @JsonProperty("spec") final PubsubSupervisorIngestionSpec ingestionSchema,
      @JsonProperty("context") @Nullable Map<String, Object> context,
      @JsonProperty("suspended") Boolean suspended,
      @JacksonInject TaskStorage taskStorage,
      @JacksonInject TaskMaster taskMaster,
      @JacksonInject IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator,
      @JacksonInject PubsubIndexTaskClientFactory indexTaskClientFactory,
      @JacksonInject @Json ObjectMapper mapper,
      @JacksonInject ServiceEmitter emitter,
      @JacksonInject DruidMonitorSchedulerConfig monitorSchedulerConfig,
      @JacksonInject RowIngestionMetersFactory rowIngestionMetersFactory,
      @JacksonInject SupervisorStateManagerConfig supervisorStateManagerConfig
  )
  {
    this.ingestionSchema = checkIngestionSchema(ingestionSchema);
    this.context = context;

    this.taskStorage = taskStorage;
    this.taskMaster = taskMaster;
    this.indexerMetadataStorageCoordinator = indexerMetadataStorageCoordinator;
    this.indexTaskClientFactory = indexTaskClientFactory;
    this.mapper = mapper;
    this.emitter = emitter;
    this.monitorSchedulerConfig = monitorSchedulerConfig;
    this.rowIngestionMetersFactory = rowIngestionMetersFactory;
    this.suspended = suspended != null ? suspended : false;
    this.supervisorStateManagerConfig = supervisorStateManagerConfig;
  }


  @JsonCreator
  public PubsubSupervisorSpec(
      @JsonProperty("spec") @Nullable PubsubSupervisorIngestionSpec ingestionSchema,
      @JsonProperty("dataSchema") @Nullable DataSchema dataSchema,
      @JsonProperty("tuningConfig") @Nullable PubsubSupervisorTuningConfig tuningConfig,
      @JsonProperty("ioConfig") @Nullable PubsubSupervisorIOConfig ioConfig,
      @JsonProperty("context") Map<String, Object> context,
      @JsonProperty("suspended") Boolean suspended,
      @JacksonInject TaskStorage taskStorage,
      @JacksonInject TaskMaster taskMaster,
      @JacksonInject IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator,
      @JacksonInject PubsubIndexTaskClientFactory pubsubIndexTaskClientFactory,
      @JacksonInject @Json ObjectMapper mapper,
      @JacksonInject ServiceEmitter emitter,
      @JacksonInject DruidMonitorSchedulerConfig monitorSchedulerConfig,
      @JacksonInject RowIngestionMetersFactory rowIngestionMetersFactory,
      @JacksonInject SupervisorStateManagerConfig supervisorStateManagerConfig
  )
  {
    this(
        ingestionSchema != null
        ? ingestionSchema
        : new PubsubSupervisorIngestionSpec(
            dataSchema,
            ioConfig,
            tuningConfig != null
            ? tuningConfig
            : PubsubSupervisorTuningConfig.defaultConfig()
        ),
        context,
        suspended,
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        pubsubIndexTaskClientFactory,
        mapper,
        emitter,
        monitorSchedulerConfig,
        rowIngestionMetersFactory,
        supervisorStateManagerConfig
    );
  }

  @Override
  public String getId()
  {
    return ingestionSchema.getDataSchema().getDataSource();
  }

  public DruidMonitorSchedulerConfig getMonitorSchedulerConfig()
  {
    return monitorSchedulerConfig;
  }

  @Override
  public List<String> getDataSources()
  {
    return ImmutableList.of(getDataSchema().getDataSource());
  }

  @Override
  public PubsubSupervisorSpec createSuspendedSpec()
  {
    return toggleSuspend(true);
  }

  @Override
  public PubsubSupervisorSpec createRunningSpec()
  {
    return toggleSuspend(false);
  }

  @Override
  public String getType()
  {
    return TASK_TYPE;
  }

  @Override
  public String getSource()
  {
    return getIoConfig() != null ? getIoConfig().getSubscription() : null;
  }

  @Override
  public Supervisor createSupervisor()
  {
    return new PubsubSupervisor(
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        (PubsubIndexTaskClientFactory) indexTaskClientFactory,
        mapper,
        this,
        rowIngestionMetersFactory
    );
  }

  @Override
  @Deprecated
  @JsonProperty
  public PubsubSupervisorTuningConfig getTuningConfig()
  {
    return (PubsubSupervisorTuningConfig) super.getTuningConfig();
  }

  @Override
  @Deprecated
  @JsonProperty
  public PubsubSupervisorIOConfig getIoConfig()
  {
    return (PubsubSupervisorIOConfig) super.getIoConfig();
  }

  @Override
  @JsonProperty
  public PubsubSupervisorIngestionSpec getSpec()
  {
    return (PubsubSupervisorIngestionSpec) super.getSpec();
  }

  @Override
  protected PubsubSupervisorSpec toggleSuspend(boolean suspend)
  {
    return new PubsubSupervisorSpec(
        getSpec(),
        getDataSchema(),
        getTuningConfig(),
        getIoConfig(),
        getContext(),
        suspend,
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        (PubsubIndexTaskClientFactory) indexTaskClientFactory,
        mapper,
        emitter,
        monitorSchedulerConfig,
        rowIngestionMetersFactory,
        supervisorStateManagerConfig
    );
  }

  @Override
  boolean isSuspended()
  {
    return false;
  }

  @Override
  public String toString()
  {
    return "PubsubSupervisorSpec{" +
           "dataSchema=" + getDataSchema() +
           ", tuningConfig=" + getTuningConfig() +
           ", ioConfig=" + getIoConfig() +
           ", context=" + getContext() +
           ", suspend=" + isSuspended() +
           '}';
  }
}
