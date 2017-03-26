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

package com.artnaseef.jms.batch;

import com.artnaseef.jms.batch.impl.BatchSendExecutor;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by art on 3/25/17.
 */
public class BatchSendIT {

  private static final Logger log = LoggerFactory.getLogger(BatchSendIT.class);

  private long nextMessageNum = 1;
  private long numMessageReceived = 0;

  @Test(timeout = 30000)
  public void testBatchSend() throws Exception {
    //
    // Setup test
    //
    Connection connection = ActiveMQConnection.makeConnection("failover://(tcp://127.0.0.1:61616)");
    Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
    Destination testQueue = new ActiveMQQueue("test.queue");
    MessageProducer producer = session.createProducer(new ActiveMQQueue("test.queue"));

    BatchSendExecutor batchSendExecutor = new BatchSendExecutor(session, producer);
    new Thread(batchSendExecutor).start();

    batchSendExecutor.setMaxBatchSize(3);
    batchSendExecutor.setMaxBatchDelay(60000);

    this.startMessageReceiver(session, testQueue);
    connection.start();

    //
    // Execute and Verify test
    //
    this.log.info("Verify initial message count is 0");
    this.verifyMessagesUnsent(0, 1000);


    this.asyncSendNextMessage(batchSendExecutor);
    this.log.info("Verify initial message count is still 0 after queueing 1 message");
    this.verifyMessagesUnsent(0, 500);

    this.asyncSendNextMessage(batchSendExecutor);
    this.log.info("Verify initial message count is still 0 after queueing 2 messages");
    this.verifyMessagesUnsent(0, 500);

    this.asyncSendNextMessage(batchSendExecutor);
    this.log.info("Verify initial message count is 3 after queueing 3 messages");
    this.waitMessagesReceived(3, 3000);


    this.asyncSendNextMessage(batchSendExecutor);
    this.log.info("Verify initial message count is still 3 after queueing 4 messages");
    this.verifyMessagesUnsent(3, 500);

    this.asyncSendNextMessage(batchSendExecutor);
    this.log.info("Verify initial message count is still 3 after queueing 5 messages");
    this.verifyMessagesUnsent(3, 500);

    this.asyncSendNextMessage(batchSendExecutor);
    this.log.info("Verify initial message count is 6 after queueing 6 messages");
    this.waitMessagesReceived(6, 3000);
  }

  public void startMessageReceiver(Session session, Destination destination) throws Exception {
    MessageConsumer consumer = session.createConsumer(destination);
    consumer.setMessageListener(new MessageReceiver());
  }

  public void asyncSendNextMessage(BatchSender batchSender) throws Exception {
    long msgNum = this.nextMessageNum;
    this.nextMessageNum++;

    new Thread(() -> {
      try {
        sendNextMessage(batchSender, msgNum);
      } catch (Exception exc) {
        this.log.warn("send failed", exc);
      }
    }).start();
  }

  public void sendNextMessage(BatchSender batchSender, long msgNum) throws Exception {
    TextMessage msg = new ActiveMQTextMessage();
    msg.setText("MESSAGE #" + msgNum);

    this.log.debug("Sending message #" + msgNum);
    batchSender.sendMessage(msg);
    this.log.debug("Done sending message #" + msgNum);
  }

  public void onMessageReceived(Message message) {
    this.log.debug("received message " + message);
    this.numMessageReceived++;
  }

  private void waitMessagesReceived(long count, long timeout) throws Exception {
    long now = System.nanoTime();
    long expires = System.nanoTime() + ( timeout * 1000000L );

    while ((numMessageReceived < count) && (now < expires)) {
      long delay = ( expires - now ) / 1000000L;
      if (delay > 100) {
        delay = 100;
      }

      Thread.sleep(delay);
      now = System.nanoTime();
    }

    assertEquals(count, numMessageReceived);
  }

  private void verifyMessagesUnsent(long numAlreadyReceived, long timeout) throws Exception {
    long now = System.nanoTime();
    long expires = System.nanoTime() + ( timeout * 1000000L );

    while ((numMessageReceived == numAlreadyReceived) && (now < expires)) {
      long delay = ( expires - now ) / 1000000L;
      if (delay > 100) {
        delay = 100;
      }

      Thread.sleep(delay);
      now = System.nanoTime();
    }

    assertTrue(numMessageReceived == numAlreadyReceived);
  }

  private class MessageReceiver implements MessageListener {

    @Override
    public void onMessage(Message message) {
      BatchSendIT.this.onMessageReceived(message);
    }
  }
}
