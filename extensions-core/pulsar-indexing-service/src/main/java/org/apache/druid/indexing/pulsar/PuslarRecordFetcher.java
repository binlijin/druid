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

import org.apache.druid.indexing.seekablestream.common.StreamPartition;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class PuslarRecordFetcher
        implements Runnable
{
  private static final EmittingLogger log = new EmittingLogger(PuslarRecordFetcher.class);

  private ConcurrentHashMap<StreamPartition<String>, Reader> readers;
  private BlockingQueue<Message<byte[]>> received;

  private volatile boolean stopRequested;

  public PuslarRecordFetcher(ConcurrentHashMap<StreamPartition<String>, Reader> readers, BlockingQueue<Message<byte[]>> received)
  {
    this.readers = readers;
    this.received = received;
    this.stopRequested = false;
  }

  @Override
  public void run()
  {
    while (true) {
      if (stopRequested) {
        log.info("PuslarRecordFetcher has been stopped");
        return;
      }
      try {
        boolean fetch = false;
        for (Map.Entry<StreamPartition<String>, Reader> entry : readers.entrySet()) {
          Reader<byte[]> reader = entry.getValue();
          Message<byte[]> message = reader.readNext(0, TimeUnit.NANOSECONDS);
          if (message != null) {
            received.put(message);
            //log.info(entry.getKey() + " get MessageId " + message.getMessageId() + ", message = " + message);
            fetch = true;
          }
        }
        if (!fetch) {
          Thread.sleep(100);
        }
      }
      catch (PulsarClientException pce) {
        log.error(pce, "");
      }
      catch (InterruptedException e) {
        return;
      }
    }
  }

  public void stop()
  {
    stopRequested = true;
  }
}
