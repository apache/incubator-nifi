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
package org.apache.nifi.event.transport.netty;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.util.Objects;
import java.util.concurrent.ThreadFactory;

/**
 * Event Loop Group Factory for standardized instance creation
 */
class EventLoopGroupFactory {
    private static final String DEFAULT_THREAD_NAME_PREFIX = "NettyEventLoopGroup";

    private static final boolean DAEMON_THREAD_ENABLED = true;

    private String threadNamePrefix = DEFAULT_THREAD_NAME_PREFIX;

    private int workerThreads;

    private final NettyTransports.NettyTransport nettyTransport;

    public EventLoopGroupFactory() {
        this.nettyTransport = NettyTransports.getDefaultNettyTransport();
    }

    /**
     * Set Thread Name Prefix used in Netty NioEventLoopGroup defaults to NettyChannel
     *
     * @param threadNamePrefix Thread Name Prefix
     */
    public void setThreadNamePrefix(final String threadNamePrefix) {
        this.threadNamePrefix = Objects.requireNonNull(threadNamePrefix, "Thread Name Prefix required");
    }

    /**
     * Set Worker Threads used in Netty NioEventLoopGroup with 0 interpreted as the default based on available processors
     *
     * @param workerThreads NioEventLoopGroup Worker Threads
     */
    public void setWorkerThreads(final int workerThreads) {
        this.workerThreads = workerThreads;
    }

    protected EventLoopGroup getEventLoopGroup() {
        return this.nettyTransport.createEventLoopGroup(workerThreads, getThreadFactory());
    }

    protected Class<? extends SocketChannel> getSocketChannelClass() {
       return this.nettyTransport.socketChannelClass();
    }

    protected Class<? extends ServerSocketChannel> getServerSocketChannelClass() {
        return this.nettyTransport.serverSocketChannelClass();
    }

    protected Class<? extends DatagramChannel> getDatagramChannelClass() {
        return this.nettyTransport.datagramChannelClass();
    }

    private ThreadFactory getThreadFactory() {
        return new DefaultThreadFactory(threadNamePrefix, DAEMON_THREAD_ENABLED);
    }
}
