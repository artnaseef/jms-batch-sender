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

import javax.jms.JMSException;

/**
 * Interface for a batch sender, which provides a synchronous API for sending messages to a JMS
 * provider using batching.
 *
 * Created by art on 3/25/17.
 */
public interface BatchSender {

  /**
   * Send this message to the JMS provider and wait for the send to complete, using batching.
   *
   * @param message JMS message to send.
   * @throws JMSException cause of failure, when the message fails to send successfully.
   */
  void sendMessage(javax.jms.Message message) throws JMSException, InterruptedException;
}
