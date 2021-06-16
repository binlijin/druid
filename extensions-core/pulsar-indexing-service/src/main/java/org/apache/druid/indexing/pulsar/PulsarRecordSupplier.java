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

package org.apache.druid.indexing.pulsar;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.pulsar.PulsarRecordEntity;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.indexing.seekablestream.common.StreamException;
import org.apache.druid.indexing.seekablestream.common.StreamPartition;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.naming.TopicName;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class PulsarRecordSupplier
        implements RecordSupplier<String, String, PulsarRecordEntity>
{
  private static final EmittingLogger log = new EmittingLogger(PulsarRecordSupplier.class);
  private boolean closed;
  private final PulsarClient client;
  private final ConcurrentHashMap<StreamPartition<String>, Consumer> consumerHashMap = new ConcurrentHashMap<>();

  private BlockingQueue<Message<byte[]>> received;
  private ScheduledExecutorService scheduledExec;
  private PuslarRecordFetcher fetcher;
  private int maxRecordsInSinglePoll;
  protected volatile boolean checkPartitionsStarted = false;

  public PulsarRecordSupplier(
          Map<String, Object> consumerProperties,
          ObjectMapper sortingMapper
  )
  {
    this.client = getPulsarClient(consumerProperties);
  }

  @VisibleForTesting
  public PulsarRecordSupplier(
          PulsarClient client
  )
  {
    this.client = client;
  }

  public PulsarRecordSupplier(
          Map<String, Object> consumerProperties,
          ObjectMapper sortingMapper,
          int maxRecordsInSinglePoll
  )
  {
    this.client = getPulsarClient(consumerProperties);
    this.maxRecordsInSinglePoll = maxRecordsInSinglePoll;
    this.received = new ArrayBlockingQueue<>(this.maxRecordsInSinglePoll);
    this.fetcher = new PuslarRecordFetcher(consumerHashMap, received);

    scheduledExec = Executors.newScheduledThreadPool(
            2,
            Execs.makeThreadFactory("PulsarRecordSupplier-FetcherWorker-%d")
    );
  }

  @Override
  public void assign(Set<StreamPartition<String>> streamPartitions)
  {
    final Map<String, Object> consumerConfigs = PulsarConsumerConfigs.getConsumerProperties();
    for (StreamPartition<String> streamPartition : streamPartitions) {
      if (consumerHashMap.containsKey(streamPartition)) {
        continue;
      }
      String topicPartition = streamPartition.getPartitionId();
      try {
        Consumer consumer = client.newConsumer()
                .topic(topicPartition)
                .subscriptionName((String) consumerConfigs.get("subscription.name"))
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                //.negativeAckRedeliveryDelay(60, TimeUnit.SECONDS)
                .subscribe();
        consumerHashMap.put(streamPartition, consumer);
      }
      catch (PulsarClientException e) {
        log.error(e, "Assign partitions throw error.");
        throw new StreamException(e);
      }
    }

    for (Iterator<Map.Entry<StreamPartition<String>, Consumer>> i = consumerHashMap.entrySet()
            .iterator(); i.hasNext(); ) {
      Map.Entry<StreamPartition<String>, Consumer> entry = i.next();
      if (!streamPartitions.contains(entry.getKey())) {
        i.remove();
        try {
          entry.getValue().close();
        }
        catch (Exception e) {
          log.warn(e, "close " + entry.getKey() + " Consumer error");
        }
      }
    }
  }

  @Override
  public void seek(StreamPartition<String> partition, String sequenceNumber)
  {
    try {
      Consumer consumer = consumerHashMap.get(partition);
      //consumer.getLastMessageId();
      MessageId messageId = PulsarSequenceNumber.getMessageId(sequenceNumber);
      consumer.seek(messageId);
    }
    catch (PulsarClientException e) {
      log.error(e, "Seek pulsar offset error.");
      throw new StreamException(e);
    }
    checkPartitionsStarted = true;
  }

  @Override
  public void seekToEarliest(Set<StreamPartition<String>> partitions)
  {
    for (StreamPartition<String> partition : partitions) {
      try {
        consumerHashMap.get(partition).seek(MessageId.earliest);
      }
      catch (PulsarClientException e) {
        log.error(e, "Could not seek earliest offset");
        throw new StreamException(e);
      }
    }
  }

  @Override
  public void seekToLatest(Set<StreamPartition<String>> partitions)
  {
    for (StreamPartition<String> partition : partitions) {
      try {
        consumerHashMap.get(partition).seek(MessageId.latest);
      }
      catch (PulsarClientException e) {
        log.error(e, "Could not seek to latest offset.");
        throw new StreamException(e);
      }
    }
  }

  @Override
  public Set<StreamPartition<String>> getAssignment()
  {
    Set<StreamPartition<String>> streamPartitions = new HashSet<>(consumerHashMap.keySet());
    return streamPartitions;
  }

  @Nonnull
  @Override
  public List<OrderedPartitionableRecord<String, String, PulsarRecordEntity>> poll(long timeout)
  {
    if (checkPartitionsStarted) {
      scheduledExec.submit(fetcher);
      checkPartitionsStarted = false;
    }
    List<OrderedPartitionableRecord<String, String, PulsarRecordEntity>> polledRecords = new ArrayList<>();
    try {
      Message<byte[]> item = received.poll(timeout, TimeUnit.MILLISECONDS);
      if (item == null) {
        return polledRecords;
      }

      int numberOfRecords = 0;

      while (item != null) {
        StreamPartition<String> sp = getStreamPartitionFromMessage(item);
        String offset = item.getMessageId().toString();

        polledRecords.add(new OrderedPartitionableRecord<>(
                sp.getStream(),
                sp.getPartitionId(),
                offset,
                ImmutableList.of(new PulsarRecordEntity(item))
        ));

        if (++numberOfRecords >= maxRecordsInSinglePoll) {
          break;
        }

        // Check if we have an item already available
        item = received.poll(0, TimeUnit.MILLISECONDS);
      }

      return polledRecords;
    }
    catch (InterruptedException e) {
      //throw new StreamException(e);
      log.warn(e, "Interrupted while polling");
      return Collections.emptyList();
    }
  }

  private StreamPartition<String> getStreamPartitionFromMessage(Message<byte[]> msg)
  {
    TopicName topic = TopicName.get(msg.getTopicName());
    // topic.getPartitionedTopicName()  persistent://public/default/test-partition
    // msg.getTopicName()  persistent://public/default/test-partition-partition-3
    return new StreamPartition<>(topic.getPartitionedTopicName(), msg.getTopicName());
  }

  @Override
  public String getLatestSequenceNumber(StreamPartition<String> partition)
  {
    try {
      return consumerHashMap.get(partition).getLastMessageId().toString();
    }
    catch (PulsarClientException e) {
      log.error(e, "Could not get latest message ID. ");
      throw new StreamException(e);
    }
  }

  @Override
  public String getEarliestSequenceNumber(StreamPartition<String> partition)
  {
    return MessageId.earliest.toString();
  }

  @Nullable
  @Override
  public String getPosition(StreamPartition<String> partition)
  {
    try {
      Consumer consumer = consumerHashMap.get(partition);
      return consumer.getLastMessageId().toString();
    }
    catch (PulsarClientException e) {
      throw new StreamException(e);
    }
  }

  @Override
  public Set<String> getPartitionIds(String stream)
  {
    try {
      return new HashSet<>(client.getPartitionsForTopic(stream).get());
    }
    catch (InterruptedException e) {
      log.error(e, "Could not get partitions. ");
      throw new StreamException(e);
    }
    catch (ExecutionException e) {
      log.error(e, "Could not get partitions. ");
      throw new StreamException(e);
    }
  }

  @Override
  public void close()
  {
    if (closed) {
      return;
    }
    closed = true;

    if (fetcher != null) {
      fetcher.stop();
    }
    if (scheduledExec != null) {
      scheduledExec.shutdown();
    }
    for (StreamPartition<String> topicPartition : consumerHashMap.keySet()) {
      try {
        consumerHashMap.get(topicPartition).close();
      }
      catch (PulsarClientException e) {
        log.error(e, "Close consumer error. ");
      }
    }
  }

  private static PulsarClient getPulsarClient(Map<String, Object> consumerProperties)
  {
    ClassLoader currCtxCl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(PulsarRecordSupplier.class.getClassLoader());
      try {
        return PulsarClient.builder()
                .serviceUrl((String) consumerProperties.get("serviceUrl"))
                .build();
      }
      catch (PulsarClientException e) {
        log.error(e, "Could not create pulsar client. ");
        throw new StreamException(e);
      }
    }
    finally {
      Thread.currentThread().setContextClassLoader(currCtxCl);
    }
  }
}
