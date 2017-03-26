/*
 * Copyright (c) 2017 Arthur Naseef
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.artnaseef.jms.batch.impl;

import com.artnaseef.jms.batch.BatchSender;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.Topic;

/**
 * Executor which sends messages to ActiveMQ and batch commits them on maximum batch size or
 * maximum batch delay limits.
 *
 * Created by art on 3/25/17.
 */
public class BatchSendExecutor implements Runnable, BatchSender {

  public static final long DEFAULT_MAX_BATCH_SIZE = 1000;
  public static final long DEFAULT_MAX_BATCH_DELAY = 1000;

  private static final Logger DEFAULT_LOGGER = LoggerFactory.getLogger(BatchSendExecutor.class);

  private final LinkedBlockingDeque<BatchSenderMessageSend> messages = new LinkedBlockingDeque<>();
  private final Session jmsSession;
  private final MessageProducer jmsProducer;

  private Logger log = DEFAULT_LOGGER;

  private long maxBatchSize = DEFAULT_MAX_BATCH_SIZE;
  private long maxBatchDelay = DEFAULT_MAX_BATCH_DELAY;
  private long lastBatchStart = 0;

  private boolean running = true;

//========================================
// Constructors
//----------------------------------------

  public BatchSendExecutor(Session jmsSession, MessageProducer jmsProducer) throws JMSException {
    if (!jmsSession.getTransacted()) {
      throw new IllegalArgumentException("jms session provided must be transacted");
    }

    this.jmsSession = jmsSession;
    this.jmsProducer = jmsProducer;
  }

//========================================
// Setters and Getters
//----------------------------------------

  public Logger getLog() {
    return log;
  }

  public void setLog(Logger log) {
    this.log = log;
  }

  public long getMaxBatchSize() {
    return maxBatchSize;
  }

  public void setMaxBatchSize(long maxBatchSize) {
    this.maxBatchSize = maxBatchSize;
  }

  public long getMaxBatchDelay() {
    return maxBatchDelay;
  }

  public void setMaxBatchDelay(long maxBatchDelay) {
    this.maxBatchDelay = maxBatchDelay;
  }

//========================================
// Processing
//----------------------------------------

  /**
   * Initiate shutdown of the executor.  Note this does not interrupt the thread running the
   * executor; the caller may do so in order to abort any delay in the executor waiting for more
   * messages to send in the final batch.
   */
  private void initiateShutdown() {
    this.running = true;
  }

  /**
   * Synchronously send the given message using the batch sender.
   */
  @Override
  public void sendMessage(Message message) throws JMSException, InterruptedException {
    BatchSenderMessageSend msgSend = new BatchSenderMessageSend(message);
    synchronized (msgSend) {
      this.messages.add(msgSend);
      msgSend.wait();

      if (msgSend.failureException != null) {
        throw msgSend.failureException;
      }
    }
  }

  /**
   * Main processing loop which waits for messages, sends them to ActiveMQ, and commits them in
   * batches.
   */
  public void run() {
    LinkedList<BatchSenderMessageSend> pendingCommitMessages = new LinkedList<>();

    // Startup log message
    try {
      this.log.info("Starting batch sender; destination={}",
                    this.getDestinationName(this.jmsProducer.getDestination()));
    } catch (JMSException jmsExc) {
      // Extracting the destination name failed
      this.log.info("Starting batch sender; destination name unknown");
      this.log.debug("failed to query destination name", jmsExc);
    }

    //
    // Loop until shutdown time.
    //
    BatchSenderMessageSend msgToSend;
    while (this.running) {
      msgToSend = null;

      //
      // Grab the next message to send, timing out at the timeout of the next batch delay.  If
      //  no batch has started, the timeout is limited to the maximum batch delay to ensure timely
      //  response to shutdown.
      //
      try {
        msgToSend =
            this.messages.pollFirst(this.calculateWaitTime(), TimeUnit.MILLISECONDS);
      } catch (InterruptedException intExc) {
        this.log.warn("interrupted while waiting for more messages to process; continuing");
      }

      // If a message was dequeued, fire it off to the JMS producer now.
      if (msgToSend != null) {
        try {
          this.log.debug("sending message to JMS provider");
          this.jmsProducer.send(msgToSend.message);

          // Remember this send instruction for later notification of success or failure at batch
          //  commit time.
          pendingCommitMessages.add(msgToSend);
        } catch (JMSException jmsExc) {
          this.log.info("failed to send message to JMS provider", jmsExc);
          this.notifyMessageSendFailure(msgToSend, jmsExc);
        }
      }

      // Commit the batch now, if it's time.
      if (this.timeToCommit(pendingCommitMessages.size())) {
        this.log.debug("about to commit batch messages; count={}", pendingCommitMessages.size());
        this.executeCommit(pendingCommitMessages);
      }
    }

    //
    // Process Loop termination
    //
    this.log.info("Shutting down batch sender");

    // Attempt to commit the final batch at shutdown
    if (pendingCommitMessages.size() > 0) {
      this.log.info("Commit final batch on shutdown; size={}", pendingCommitMessages.size());
      this.executeCommit(pendingCommitMessages);
    }
  }

//========================================
// Internals
//----------------------------------------

  private void executeCommit(LinkedList<BatchSenderMessageSend> pendingCommitMessages) {
    try {
      this.jmsSession.commit();
    } catch (JMSException jmsExc) {
      pendingCommitMessages.stream()
          .forEach((msgSend) -> notifyMessageSendFailure(msgSend, jmsExc));
      pendingCommitMessages.clear();
      return;
    }

    pendingCommitMessages.stream().forEach(this::notifyMessageSendSuccess);
    pendingCommitMessages.clear();
  }

  private void notifyMessageSendFailure(BatchSenderMessageSend msgSend, JMSException jmsExc) {
    msgSend.failureException = jmsExc;
    synchronized (msgSend) {
      msgSend.notifyAll();
    }
  }

  private void notifyMessageSendSuccess(BatchSenderMessageSend msgSend) {
    synchronized (msgSend) {
      msgSend.notifyAll();
    }
  }

  private boolean timeToCommit(long pendingMessageCount) {
    if (pendingMessageCount >= this.maxBatchSize) {
      return true;
    }

    if (this.calculateWaitTime() == 0) {
      return true;
    }

    return false;
  }

  /**
   * Calcluate the amount of time to wait for a new message to add to the batch so that the maximum
   * batch delay is not exceeded for any batch.  If no batch is pending, the maximum batch delay is
   * returned.
   *
   * @return milliseconds to wait until the current batch delay period ends.
   */
  private long calculateWaitTime() {
    // If no batch has started yet, use the maximum delay.
    if (this.lastBatchStart == 0) {
      return this.maxBatchDelay;
    }

    // Use System.nanoTime() as it is unaffected by system clock changes.
    long now = System.nanoTime();

    long elapsedNs = now - this.lastBatchStart;
    long elapsedMs = elapsedNs / 1000000L;

    // Some time left?
    if (elapsedMs < this.maxBatchDelay) {
      // Return the remaining time until the end of the batch delay period, in millisecond.
      return this.maxBatchDelay - elapsedMs;
    }

    // Time is up
    return 0;
  }

  /**
   * Utility to extract the name of the given JMS destination.
   *
   * @param destination JMS destination for which to extract the name.
   * @return name of the destination, if successfully extracted; "unknown" otherwise.
   * @throws JMSException on failure to extract the JMS destination name.
   */
  private String getDestinationName(Destination destination) throws JMSException {
    if (destination == null) {
      return "unknown";
    }

    if (destination instanceof TemporaryQueue) {
      return "temp-queue://" + ((TemporaryQueue) destination).getQueueName();
    } else if (destination instanceof TemporaryTopic) {
      return "temp-topic://" + ((Topic) destination).getTopicName();
    } else if (destination instanceof Queue) {
      return "queue://" + ((Queue) destination).getQueueName();
    } else if (destination instanceof Topic) {
      return "topic://" + ((Topic) destination).getTopicName();
    }

    return "unknown";
  }

}
