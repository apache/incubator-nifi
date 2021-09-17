/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.standard.relp.frame;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.nifi.event.transport.netty.channel.ByteArrayMessageChannelHandler;
import org.apache.nifi.processors.standard.relp.event.RELPNettyEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;

/**
 * Decode data received into a RELPNettyEvent
 */
@ChannelHandler.Sharable
public class RELPNettyEventChannelHandler extends SimpleChannelInboundHandler<RELPNettyEvent> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ByteArrayMessageChannelHandler.class);
    private final BlockingQueue<RELPNettyEvent> events;

    public RELPNettyEventChannelHandler(BlockingQueue<RELPNettyEvent> events) {
        this.events = events;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RELPNettyEvent msg) {
        LOGGER.debug("RELP Message Received Length [{}] Remote Address [{}] ", msg.getData().length, msg.getSender());
        events.offer(msg);
    }
}
