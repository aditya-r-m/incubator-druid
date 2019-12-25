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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.pubsub.v1.ReceivedMessage;
import org.apache.druid.data.input.Committer;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.LookupNodeService;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.indexer.IngestionState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.TaskRealtimeMetricsMonitorBuilder;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.SegmentLockAcquireAction;
import org.apache.druid.indexing.common.actions.TimeChunkLockAcquireAction;
import org.apache.druid.indexing.common.stats.RowIngestionMeters;
import org.apache.druid.indexing.common.stats.RowIngestionMetersFactory;
import org.apache.druid.indexing.common.task.RealtimeIndexTask;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.collect.Utils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.indexing.RealtimeIOConfig;
import org.apache.druid.segment.realtime.FireDepartment;
import org.apache.druid.segment.realtime.FireDepartmentMetrics;
import org.apache.druid.segment.realtime.appenderator.Appenderator;
import org.apache.druid.segment.realtime.appenderator.AppenderatorDriverAddResult;
import org.apache.druid.segment.realtime.appenderator.AppenderatorsManager;
import org.apache.druid.segment.realtime.appenderator.SegmentsAndMetadata;
import org.apache.druid.segment.realtime.appenderator.StreamAppenderatorDriver;
import org.apache.druid.segment.realtime.firehose.ChatHandler;
import org.apache.druid.segment.realtime.firehose.ChatHandlerProvider;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.utils.CircularBuffer;
import org.joda.time.DateTime;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * Pubsub indexing task runner supporting incremental segments publishing
 */
public class PubsubIndexTaskRunner implements ChatHandler
{
  private static final EmittingLogger log = new EmittingLogger(PubsubIndexTaskRunner.class);
  protected final AtomicBoolean stopRequested = new AtomicBoolean(false);
  protected final Lock pollRetryLock = new ReentrantLock();
  protected final Condition isAwaitingRetry = pollRetryLock.newCondition();
  private final PubsubIndexTaskIOConfig ioConfig;
  private final PubsubIndexTaskTuningConfig tuningConfig;
  private final PubsubIndexTask task;
  private final InputRowParser<ByteBuffer> parser;
  private final AuthorizerMapper authorizerMapper;
  private final Optional<ChatHandlerProvider> chatHandlerProvider;
  private final CircularBuffer<Throwable> savedParseExceptions;
  private final RowIngestionMetersFactory rowIngestionMetersFactory;
  private final AppenderatorsManager appenderatorsManager;
  private final LockGranularity lockGranularityToUse;
  private final InputRowSchema inputRowSchema;
  private final InputFormat inputFormat;
  private final RowIngestionMeters rowIngestionMeters;
  // The pause lock and associated conditions are to support coordination between the Jetty threads and the main
  // ingestion loop. The goal is to provide callers of the API a guarantee that if pause() returns successfully
  // the ingestion loop has been stopped at the returned sequences and will not ingest any more data until resumed. The
  // fields are used as follows (every step requires acquiring [pauseLock]):
  //   Pausing:
  //   - In pause(), [pauseRequested] is set to true and then execution waits for [status] to change to PAUSED, with the
  //     condition checked when [hasPaused] is signalled.
  //   - In possiblyPause() called from the main loop, if [pauseRequested] is true, [status] is set to PAUSED,
  //     [hasPaused] is signalled, and execution pauses until [pauseRequested] becomes false, either by being set or by
  //     the [pauseMillis] timeout elapsing. [pauseRequested] is checked when [shouldResume] is signalled.
  //   Resuming:
  //   - In resume(), [pauseRequested] is set to false, [shouldResume] is signalled, and execution waits for [status] to
  //     change to something other than PAUSED, with the condition checked when [shouldResume] is signalled.
  //   - In possiblyPause(), when [shouldResume] is signalled, if [pauseRequested] has become false the pause loop ends,
  //     [status] is changed to STARTING and [shouldResume] is signalled.
  private final Lock pauseLock = new ReentrantLock();
  private final Condition hasPaused = pauseLock.newCondition();
  private final Condition shouldResume = pauseLock.newCondition();
  private final AtomicBoolean publishOnStop = new AtomicBoolean(false);
  private final List<ListenableFuture<SegmentsAndMetadata>> publishWaitList = new ArrayList<>();
  private final List<ListenableFuture<SegmentsAndMetadata>> handOffWaitList = new ArrayList<>();
  protected volatile boolean pauseRequested = false;
  private volatile DateTime startTime;
  private volatile Status status = Status.NOT_STARTED; // this is only ever set by the task runner thread (runThread)
  private volatile TaskToolbox toolbox;
  private volatile Thread runThread;
  private volatile Appenderator appenderator;
  private volatile StreamAppenderatorDriver driver;
  private volatile IngestionState ingestionState;

  PubsubIndexTaskRunner(
      PubsubIndexTask task,
      @Nullable InputRowParser<ByteBuffer> parser,
      AuthorizerMapper authorizerMapper,
      Optional<ChatHandlerProvider> chatHandlerProvider,
      CircularBuffer<Throwable> savedParseExceptions,
      RowIngestionMetersFactory rowIngestionMetersFactory,
      AppenderatorsManager appenderatorsManager,
      LockGranularity lockGranularityToUse
  )
  {
    this.task = task;
    this.ioConfig = task.getIOConfig();
    this.tuningConfig = task.getTuningConfig();
    this.parser = parser;
    this.authorizerMapper = authorizerMapper;
    this.chatHandlerProvider = chatHandlerProvider;
    this.savedParseExceptions = savedParseExceptions;
    this.rowIngestionMetersFactory = rowIngestionMetersFactory;
    this.appenderatorsManager = appenderatorsManager;
    this.lockGranularityToUse = lockGranularityToUse;
    this.inputFormat = ioConfig.getInputFormat(parser == null ? null : parser.getParseSpec());
    this.rowIngestionMeters = rowIngestionMetersFactory.createRowIngestionMeters();

    this.inputRowSchema = new InputRowSchema(
        task.getDataSchema().getTimestampSpec(),
        task.getDataSchema().getDimensionsSpec(),
        Arrays.stream(task.getDataSchema().getAggregators())
              .map(AggregatorFactory::getName)
              .collect(Collectors.toList())
    );
  }

  private boolean isPaused()
  {
    return status == Status.PAUSED;
  }

  private void requestPause()
  {
    pauseRequested = true;
  }

  public TaskStatus run(TaskToolbox toolbox)
  {
    try {
      return runInternal(toolbox);
    }
    catch (Exception e) {
      log.error(e, "Encountered exception while running task.");
      final String errorMsg = Throwables.getStackTraceAsString(e);
      // toolbox.getTaskReportFileWriter().write(task.getId(), getTaskCompletionReports(errorMsg));
      return TaskStatus.failure(
          task.getId(),
          errorMsg
      );
    }
  }

  @Nonnull
  protected List<ReceivedMessage> getRecords(
      PubsubRecordSupplier recordSupplier,
      TaskToolbox toolbox
  ) throws Exception
  {
    // Handles OffsetOutOfRangeException, which is thrown if the seeked-to
    // offset is not present in the topic-partition. This can happen if we're asking a task to read from data
    // that has not been written yet (which is totally legitimate). So let's wait for it to show up.
    List<ReceivedMessage> records = new ArrayList<>();
    try {
      records = recordSupplier.poll(task.getIOConfig().getPollTimeout());
    }
    catch (Exception e) {
      log.warn("OffsetOutOfRangeException with message [%s]", e.getMessage());
    }

    return records;
  }

  @VisibleForTesting
  public void setToolbox(TaskToolbox toolbox)
  {
    this.toolbox = toolbox;
  }

  private TaskStatus runInternal(TaskToolbox toolbox) throws Exception
  {
    startTime = DateTimes.nowUtc();
    status = Status.STARTING;

    setToolbox(toolbox);

    if (chatHandlerProvider.isPresent()) {
      log.debug("Found chat handler of class[%s]", chatHandlerProvider.get().getClass().getName());
      chatHandlerProvider.get().register(task.getId(), this, false);
    } else {
      log.warn("No chat handler detected.");
    }

    runThread = Thread.currentThread();

    // Set up FireDepartmentMetrics
    final FireDepartment fireDepartmentForMetrics = new FireDepartment(
        task.getDataSchema(),
        new RealtimeIOConfig(null, null),
        null
    );
    FireDepartmentMetrics fireDepartmentMetrics = fireDepartmentForMetrics.getMetrics();
    toolbox.getMonitorScheduler()
           .addMonitor(TaskRealtimeMetricsMonitorBuilder.build(task, fireDepartmentForMetrics, rowIngestionMeters));

    final String lookupTier = task.getContextValue(RealtimeIndexTask.CTX_KEY_LOOKUP_TIER);
    final LookupNodeService lookupNodeService = lookupTier == null ?
                                                toolbox.getLookupNodeService() :
                                                new LookupNodeService(lookupTier);

    final DiscoveryDruidNode discoveryDruidNode = new DiscoveryDruidNode(
        toolbox.getDruidNode(),
        NodeRole.PEON,
        ImmutableMap.of(
            toolbox.getDataNodeService().getName(), toolbox.getDataNodeService(),
            lookupNodeService.getName(), lookupNodeService
        )
    );

    Throwable caughtExceptionOuter = null;
    try (final PubsubRecordSupplier recordSupplier = task.newTaskRecordSupplier()) {

      if (appenderatorsManager.shouldTaskMakeNodeAnnouncements()) {
        toolbox.getDataSegmentServerAnnouncer().announce();
        toolbox.getDruidNodeAnnouncer().announce(discoveryDruidNode);
      }
      appenderator = task.newAppenderator(fireDepartmentMetrics, toolbox);
      driver = task.newDriver(appenderator, toolbox, fireDepartmentMetrics);

      // Start up, set up initial sequences.
      final Object restoredMetadata = driver.startJob(
          segmentId -> {
            try {
              if (lockGranularityToUse == LockGranularity.SEGMENT) {
                return toolbox.getTaskActionClient().submit(
                    new SegmentLockAcquireAction(
                        TaskLockType.EXCLUSIVE,
                        segmentId.getInterval(),
                        segmentId.getVersion(),
                        segmentId.getShardSpec().getPartitionNum(),
                        1000L
                    )
                ).isOk();
              } else {
                return toolbox.getTaskActionClient().submit(
                    new TimeChunkLockAcquireAction(
                        TaskLockType.EXCLUSIVE,
                        segmentId.getInterval(),
                        1000L
                    )
                ) != null;
              }
            }
            catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
      );
      if (restoredMetadata == null) {
        // no persist has happened so far
        // so either this is a brand new task or replacement of a failed task
      } else {

      }

      // Set up committer.
      final Supplier<Committer> committerSupplier = () -> {
        return new Committer()
        {
          @Override
          public Object getMetadata()
          {
            return null;
          }

          @Override
          public void run()
          {
            // Do nothing.
          }
        };
      };

      ingestionState = IngestionState.BUILD_SEGMENTS;

      // Main loop.
      // Could eventually support leader/follower mode (for keeping replicas more in sync)
      boolean stillReading = true;
      status = Status.READING;
      Throwable caughtExceptionInner = null;

      try {
        while (stillReading) {
          // if stop is requested or task's end sequence is set by call to setEndOffsets method with finish set to true
          if (stopRequested.get()) {
            status = Status.PUBLISHING;
            break;
          }

          //if (backgroundThreadException != null) {
          //  throw new RuntimeException(backgroundThreadException);
          //}

          checkPublishAndHandoffFailure();

          // calling getRecord() ensures that exceptions specific to kafka/kinesis like OffsetOutOfRangeException
          // are handled in the subclasses.
          List<ReceivedMessage> records = getRecords(
              recordSupplier,
              toolbox
          );

          for (ReceivedMessage record : records) {
            final boolean shouldProcess = true;

            if (shouldProcess) {
              try {
                final List<byte[]> valueBytess = Lists.asList(record.getMessage().getData().toByteArray(), null);
                final List<InputRow> rows;
                if (valueBytess == null || valueBytess.isEmpty()) {
                  rows = Utils.nullableListOf((InputRow) null);
                } else {
                  rows = parseBytes(valueBytess);
                }
                boolean isPersistRequired = false;

                for (InputRow row : rows) {
                  if (row != null && task.withinMinMaxRecordTime(row)) {
                    final AppenderatorDriverAddResult addResult = driver.add(
                        row,
                        "todo",
                        committerSupplier,
                        true,
                        // do not allow incremental persists to happen until all the rows from this batch
                        // of rows are indexed
                        false
                    );

                    if (addResult.isOk()) {
                      // If the number of rows in the segment exceeds the threshold after adding a row,
                      // move the segment out from the active segments of BaseAppenderatorDriver to make a new segment.
                      final boolean isPushRequired = addResult.isPushRequired(
                          tuningConfig.getPartitionsSpec().getMaxRowsPerSegment(),
                          tuningConfig.getPartitionsSpec()
                                      .getMaxTotalRowsOr(DynamicPartitionsSpec.DEFAULT_MAX_TOTAL_ROWS)
                      );
                      isPersistRequired |= addResult.isPersistRequired();
                    } else {
                      // Failure to allocate segment puts determinism at risk, bail out to be safe.
                      // May want configurable behavior here at some point.
                      // If we allow continuing, then consider blacklisting the interval for a while to avoid constant checks.
                      throw new ISE("Could not allocate segment for row with timestamp[%s]", row.getTimestamp());
                    }

                    if (addResult.getParseException() != null) {
                      handleParseException(addResult.getParseException(), record);
                    } else {
                      rowIngestionMeters.incrementProcessed();
                    }
                  } else {
                    rowIngestionMeters.incrementThrownAway();
                  }
                }
                if (isPersistRequired) {
                  Futures.addCallback(
                      driver.persistAsync(committerSupplier.get()),
                      new FutureCallback<Object>()
                      {
                        @Override
                        public void onSuccess(@Nullable Object result)
                        {
                          log.debug("Persist completed with metadata: %s", result);
                        }

                        @Override
                        public void onFailure(Throwable t)
                        {
                          log.error("Persist failed, dying");
                          // backgroundThreadException = t;
                        }
                      }
                  );
                }
              }
              catch (ParseException e) {
                handleParseException(e, record);
              }
            }
          }

          if (stillReading) {
            requestPause();
          }
        }
        ingestionState = IngestionState.COMPLETED;
      }
      catch (Exception e) {
        // (1) catch all exceptions while reading from pubsub
        caughtExceptionInner = e;
        log.error(e, "Encountered exception in run() before persisting.");
        throw e;
      }
      finally {
        try {
          driver.persist(committerSupplier.get()); // persist pending data
        }
        catch (Exception e) {
          if (caughtExceptionInner != null) {
            caughtExceptionInner.addSuppressed(e);
          } else {
            throw e;
          }
        }
      }

      /*synchronized (statusLock) {
        if (stopRequested.get() && !publishOnStop.get()) {
          throw new InterruptedException("Stopping without publishing");
        }

        status = Status.PUBLISHING;
      }*/

      // publishAndRegisterHandoff(sequenceMetadata);

      // if (backgroundThreadException != null) {
      //  throw new RuntimeException(backgroundThreadException);
      // }

      // Wait for publish futures to complete.
      // Futures.allAsList(publishWaitList).get();

      // Wait for handoff futures to complete.
      // Note that every publishing task (created by calling AppenderatorDriver.publish()) has a corresponding
      // handoffFuture. handoffFuture can throw an exception if 1) the corresponding publishFuture failed or 2) it
      // failed to persist sequences. It might also return null if handoff failed, but was recoverable.
      // See publishAndRegisterHandoff() for details.
      List<SegmentsAndMetadata> handedOffList = Collections.emptyList();
      /*if (tuningConfig.getHandoffConditionTimeout() == 0) {
        handedOffList = Futures.allAsList(handOffWaitList).get();
      } else {
        try {
          handedOffList = Futures.allAsList(handOffWaitList)
                                 .get(tuningConfig.getHandoffConditionTimeout(), TimeUnit.MILLISECONDS);
        }
        catch (TimeoutException e) {
          // Handoff timeout is not an indexing failure, but coordination failure. We simply ignore timeout exception
          // here.
          log.makeAlert("Timeout waiting for handoff")
             .addData("taskId", task.getId())
             .addData("handoffConditionTimeout", tuningConfig.getHandoffConditionTimeout())
             .emit();
        }
      }*/

      for (SegmentsAndMetadata handedOff : handedOffList) {
        log.info(
            "Handoff complete for segments: %s",
            String.join(", ", Lists.transform(handedOff.getSegments(), DataSegment::toString))
        );
      }

      appenderator.close();
    }
    catch (InterruptedException | RejectedExecutionException e) {
      // (2) catch InterruptedException and RejectedExecutionException thrown for the whole ingestion steps including
      // the final publishing.
      caughtExceptionOuter = e;
      try {
        // Futures.allAsList(publishWaitList).cancel(true);
        // Futures.allAsList(handOffWaitList).cancel(true);
        if (appenderator != null) {
          appenderator.closeNow();
        }
      }
      catch (Exception e2) {
        e.addSuppressed(e2);
      }

      // handle the InterruptedException that gets wrapped in a RejectedExecutionException
      if (e instanceof RejectedExecutionException
          && (e.getCause() == null || !(e.getCause() instanceof InterruptedException))) {
        throw e;
      }

      // if we were interrupted because we were asked to stop, handle the exception and return success, else rethrow
      if (!stopRequested.get()) {
        Thread.currentThread().interrupt();
        throw e;
      }
    }
    catch (Exception e) {
      // (3) catch all other exceptions thrown for the whole ingestion steps including the final publishing.
      caughtExceptionOuter = e;
      /*try {
        Futures.allAsList(publishWaitList).cancel(true);
        Futures.allAsList(handOffWaitList).cancel(true);
        if (appenderator != null) {
          appenderator.closeNow();
        }
      }
      catch (Exception e2) {
        e.addSuppressed(e2);
      }*/
      throw e;
    }
    finally {
      try {

        if (driver != null) {
          driver.close();
        }
        if (chatHandlerProvider.isPresent()) {
          chatHandlerProvider.get().unregister(task.getId());
        }

        if (appenderatorsManager.shouldTaskMakeNodeAnnouncements()) {
          toolbox.getDruidNodeAnnouncer().unannounce(discoveryDruidNode);
          toolbox.getDataSegmentServerAnnouncer().unannounce();
        }
      }
      catch (Throwable e) {
        if (caughtExceptionOuter != null) {
          caughtExceptionOuter.addSuppressed(e);
        } else {
          throw e;
        }
      }
    }

    // toolbox.getTaskReportFileWriter().write(task.getId(), getTaskCompletionReports(null));
    return TaskStatus.success(task.getId());
  }

  /**
   * Checks if the pauseRequested flag was set and if so blocks:
   * a) if pauseMillis == PAUSE_FOREVER, until pauseRequested is cleared
   * b) if pauseMillis != PAUSE_FOREVER, until pauseMillis elapses -or- pauseRequested is cleared
   * <p>
   * If pauseMillis is changed while paused, the new pause timeout will be applied. This allows adjustment of the
   * pause timeout (making a timed pause into an indefinite pause and vice versa is valid) without having to resume
   * and ensures that the loop continues to stay paused without ingesting any new events. You will need to signal
   * shouldResume after adjusting pauseMillis for the new value to take effect.
   * <p>
   * Sets paused = true and signals paused so callers can be notified when the pause command has been accepted.
   * <p>
   * Additionally, pauses if all partitions assignments have been read and pauseAfterRead flag is set.
   *
   * @return true if a pause request was handled, false otherwise
   */
  private boolean possiblyPause() throws InterruptedException
  {
    pauseLock.lockInterruptibly();
    try {
      if (pauseRequested) {
        status = Status.PAUSED;
        hasPaused.signalAll();

        while (pauseRequested) {
          log.debug("Received pause command, pausing ingestion until resumed.");
          shouldResume.await();
        }

        status = Status.READING;
        shouldResume.signalAll();
        log.debug("Received resume command, resuming ingestion.");
        return true;
      }
    }
    finally {
      pauseLock.unlock();
    }

    return false;
  }

  private void checkPublishAndHandoffFailure() throws ExecutionException, InterruptedException
  {
    // Check if any publishFuture failed.
    final List<ListenableFuture<SegmentsAndMetadata>> publishFinished = publishWaitList
        .stream()
        .filter(Future::isDone)
        .collect(Collectors.toList());

    for (ListenableFuture<SegmentsAndMetadata> publishFuture : publishFinished) {
      // If publishFuture failed, the below line will throw an exception and catched by (1), and then (2) or (3).
      publishFuture.get();
    }

    publishWaitList.removeAll(publishFinished);

    // Check if any handoffFuture failed.
    final List<ListenableFuture<SegmentsAndMetadata>> handoffFinished = handOffWaitList
        .stream()
        .filter(Future::isDone)
        .collect(Collectors.toList());

    for (ListenableFuture<SegmentsAndMetadata> handoffFuture : handoffFinished) {
      // If handoffFuture failed, the below line will throw an exception and catched by (1), and then (2) or (3).
      handoffFuture.get();
    }

    handOffWaitList.removeAll(handoffFinished);
  }

  private List<InputRow> parseBytes(List<byte[]> valueBytess) throws IOException
  {
    if (parser != null) {
      return parseWithParser(valueBytess);
    } else {
      return parseWithInputFormat(valueBytess);
    }
  }

  private List<InputRow> parseWithParser(List<byte[]> valueBytess)
  {
    final List<InputRow> rows = new ArrayList<>();
    for (byte[] valueBytes : valueBytess) {
      rows.addAll(parser.parseBatch(ByteBuffer.wrap(valueBytes)));
    }
    return rows;
  }

  private List<InputRow> parseWithInputFormat(List<byte[]> valueBytess) throws IOException
  {
    final List<InputRow> rows = new ArrayList<>();
    for (byte[] valueBytes : valueBytess) {
      final InputEntityReader reader = task.getDataSchema().getTransformSpec().decorate(
          Preconditions.checkNotNull(inputFormat, "inputFormat").createReader(
              inputRowSchema,
              new ByteEntity(valueBytes),
              toolbox.getIndexingTmpDir()
          )
      );
      try (CloseableIterator<InputRow> rowIterator = reader.read()) {
        rowIterator.forEachRemaining(rows::add);
      }
    }
    return rows;
  }

  private void handleParseException(ParseException e, Object record)
  {
    if (e.isFromPartiallyValidRow()) {
      rowIngestionMeters.incrementProcessedWithError();
    } else {
      rowIngestionMeters.incrementUnparseable();
    }

    if (tuningConfig.isLogParseExceptions()) {
    }

    if (savedParseExceptions != null) {
      savedParseExceptions.add(e);
    }

    if (rowIngestionMeters.getUnparseable() + rowIngestionMeters.getProcessedWithError()
        > tuningConfig.getMaxParseExceptions()) {
      throw new RuntimeException("Max parse exceptions exceeded");
    }
  }

  @GET
  @Path("/checkpoints")
  @Produces(MediaType.APPLICATION_JSON)
  public Map<Integer, Object> getCheckpointsHTTP(
      @Context final HttpServletRequest req
  )
  {
    // authorizationCheck(req, Action.READ);
    // return getCheckpoints();
    return null;
  }

  /**
   * Signals the ingestion loop to pause.
   *
   * @return one of the following Responses: 400 Bad Request if the task has started publishing; 202 Accepted if the
   * method has timed out and returned before the task has paused; 200 OK with a map of the current partition sequences
   * in the response body if the task successfully paused
   */
  @POST
  @Path("/pause")
  @Produces(MediaType.APPLICATION_JSON)
  public Response pauseHTTP(
      @Context final HttpServletRequest req
  ) throws InterruptedException
  {
    // authorizationCheck(req, Action.WRITE);
    return pause();
  }

  @VisibleForTesting
  public Response pause() throws InterruptedException
  {
    if (!(status == Status.PAUSED || status == Status.READING)) {
      return Response.status(Response.Status.BAD_REQUEST)
                     .entity(StringUtils.format("Can't pause, task is not in a pausable state (state: [%s])", status))
                     .build();
    }

    pauseLock.lockInterruptibly();
    try {
      pauseRequested = true;

      pollRetryLock.lockInterruptibly();
      try {
        isAwaitingRetry.signalAll();
      }
      finally {
        pollRetryLock.unlock();
      }

      if (isPaused()) {
        shouldResume.signalAll(); // kick the monitor so it re-awaits with the new pauseMillis
      }

      long nanos = TimeUnit.SECONDS.toNanos(2);
      while (!isPaused()) {
        if (nanos <= 0L) {
          return Response.status(Response.Status.ACCEPTED)
                         .entity("Request accepted but task has not yet paused")
                         .build();
        }
        nanos = hasPaused.awaitNanos(nanos);
      }
    }
    finally {
      pauseLock.unlock();
    }

    try {
      return Response.ok().entity(toolbox.getJsonMapper().writeValueAsString("TODO: res")).build();
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @POST
  @Path("/resume")
  public Response resumeHTTP(@Context final HttpServletRequest req) throws InterruptedException
  {
    // authorizationCheck(req, Action.WRITE);
    resume();
    return Response.status(Response.Status.OK).build();
  }

  @VisibleForTesting
  public void resume() throws InterruptedException
  {
    pauseLock.lockInterruptibly();
    try {
      pauseRequested = false;
      shouldResume.signalAll();

      long nanos = TimeUnit.SECONDS.toNanos(5);
      while (isPaused()) {
        if (nanos <= 0L) {
          throw new RuntimeException("Resume command was not accepted within 5 seconds");
        }
        nanos = shouldResume.awaitNanos(nanos);
      }
    }
    finally {
      pauseLock.unlock();
    }
  }

  @GET
  @Path("/time/start")
  @Produces(MediaType.APPLICATION_JSON)
  public DateTime getStartTime(@Context final HttpServletRequest req)
  {
    // authorizationCheck(req, Action.WRITE);
    return startTime;
  }


  public enum Status
  {
    NOT_STARTED,
    STARTING,
    READING,
    PAUSED,
    PUBLISHING
  }
}

