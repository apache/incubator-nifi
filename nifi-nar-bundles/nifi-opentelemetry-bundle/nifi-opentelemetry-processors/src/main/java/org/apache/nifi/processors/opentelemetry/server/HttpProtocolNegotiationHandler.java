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
package org.apache.nifi.processors.opentelemetry.server;

import com.google.protobuf.Message;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpServerExpectContinueHandler;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.ApplicationProtocolNegotiationHandler;
import io.netty.handler.ssl.SslHandler;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processors.opentelemetry.io.RequestContentListener;
import org.apache.nifi.processors.opentelemetry.io.StandardRequestContentListener;

import javax.net.ssl.SSLEngine;
import java.util.concurrent.BlockingQueue;

/**
 * HTTP Protocol Negotiation Handler configures Channel Pipeline based on HTTP/2 or HTTP/1.1
 */
public class HttpProtocolNegotiationHandler extends ApplicationProtocolNegotiationHandler {
    private static final String DEFAULT_APPLICATION_PROTOCOL = ApplicationProtocolNames.HTTP_1_1;

    private final ComponentLog log;

    private final BlockingQueue<Message> messages;

    /**
     * HTTP Protocol Negotiation Handler defaults to HTTP/1.1 when clients do not indicate supported Application Protocols
     *
     * @param log Component Log
     * @param messages Queue of Telemetry Messages
     */
    public HttpProtocolNegotiationHandler(final ComponentLog log, final BlockingQueue<Message> messages) {
        super(DEFAULT_APPLICATION_PROTOCOL);
        this.log = log;
        this.messages = messages;
    }

    /**
     * Configure Channel Pipeline based on negotiated Application Layer Protocol
     *
     * @param channelHandlerContext Channel Handler Context
     * @param protocol Negotiated Protocol ignored in favor of SSLEngine.getApplicationProtocol()
     */
    @Override
    protected void configurePipeline(final ChannelHandlerContext channelHandlerContext, final String protocol) {
        final ChannelPipeline pipeline = channelHandlerContext.pipeline();
        final RequestContentListener requestContentListener = new StandardRequestContentListener(log, messages);
        final String applicationProtocol = getApplicationProtocol(channelHandlerContext);

        if (ApplicationProtocolNames.HTTP_2.equals(applicationProtocol)) {
            final Http2RequestHandlerBuilder handlerBuilder = new Http2RequestHandlerBuilder(log, requestContentListener);
            final Http2RequestHandler handler = handlerBuilder.build();
            pipeline.addLast(handler);
        } else if (ApplicationProtocolNames.HTTP_1_1.equals(applicationProtocol)) {
            pipeline.addLast(
                    new HttpServerCodec(),
                    new HttpContentCompressor(),
                    new HttpServerExpectContinueHandler(),
                    new HttpRequestHandler(log, requestContentListener)
            );
        } else {
            throw new IllegalStateException(String.format("Application Protocol [%s] not supported", applicationProtocol));
        }
    }

    /**
     * Get Application Protocol for SSLEngine because Netty SslHandler does not handle standard SSLEngine methods
     *
     * @param channelHandlerContext Channel Handler Context containing SslHandler
     * @return Application Protocol defaults to HTTP/1.1 when not provided
     */
    private String getApplicationProtocol(final ChannelHandlerContext channelHandlerContext) {
        final SslHandler sslHandler = channelHandlerContext.pipeline().get(SslHandler.class);
        final SSLEngine sslEngine = sslHandler.engine();
        final String negotiatedApplicationProtocol = sslEngine.getApplicationProtocol();

        final String applicationProtocol;
        if (negotiatedApplicationProtocol == null || negotiatedApplicationProtocol.isEmpty()) {
            applicationProtocol = DEFAULT_APPLICATION_PROTOCOL;
        } else {
            applicationProtocol = negotiatedApplicationProtocol;
        }
        return applicationProtocol;
    }
}
