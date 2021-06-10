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

import org.apache.druid.indexing.seekablestream.common.OrderedSequenceNumber;
import org.apache.druid.indexing.seekablestream.common.StreamException;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;

import javax.validation.constraints.NotNull;

import java.io.IOException;

// OrderedSequenceNumber.equals() should be used instead.
@SuppressWarnings("ComparableImplementedButEqualsNotOverridden")
public class PulsarSequenceNumber extends OrderedSequenceNumber<String>
{
  private static final EmittingLogger log = new EmittingLogger(PulsarSequenceNumber.class);

  private MessageId messageId;

  private PulsarSequenceNumber(String sequenceNumber)
  {
    super(sequenceNumber, false);
    this.messageId = getMessageId(sequenceNumber);
  }

  public MessageId getMessageId()
  {
    return this.messageId;
  }

  public static PulsarSequenceNumber of(String sequenceNumber)
  {
    return new PulsarSequenceNumber(sequenceNumber);
  }

  public static MessageId getMessageId(String sequenceNumber)
  {
    String[] sequenceNumbers = sequenceNumber.split(":");
    if (sequenceNumbers.length == 3) {
      return new MessageIdImpl(
              Long.parseLong(sequenceNumbers[0]),
              Long.parseLong(sequenceNumbers[1]),
              Integer.parseInt(sequenceNumbers[2]));
    } else if (sequenceNumbers.length == 4) {
      return new BatchMessageIdImpl(
              Long.parseLong(sequenceNumbers[0]),
              Long.parseLong(sequenceNumbers[1]),
              Integer.parseInt(sequenceNumbers[2]),
              Integer.parseInt(sequenceNumbers[3]));
    } else {
      log.error(" wrong sequenceNumber = " + sequenceNumber);
      throw new StreamException(new IOException("wrong sequenceNumber " + sequenceNumber));
    }
  }

  @Override
  public int compareTo(
      @NotNull OrderedSequenceNumber<String> o
  )
  {
    if (o instanceof PulsarSequenceNumber) {
      PulsarSequenceNumber other = (PulsarSequenceNumber) o;
      return this.getMessageId().compareTo(other.getMessageId());
    } else {
      throw new IllegalArgumentException("expected PulsarSequenceNumber object. Got instance of " + o.getClass().getName());
    }
  }

}
