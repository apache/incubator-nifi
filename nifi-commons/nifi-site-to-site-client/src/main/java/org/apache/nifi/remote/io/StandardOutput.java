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
package org.apache.nifi.remote.io;

import org.apache.nifi.remote.protocol.CommunicationsOutput;
import org.apache.nifi.stream.io.ByteCountingOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;

public class StandardOutput implements CommunicationsOutput {

    private static final Logger LOG = LoggerFactory.getLogger(StandardOutput.class);

    private final ByteCountingOutputStream countingOut;
    private final OutputStream bufferedOut;
    private final InterruptableOutputStream interruptableOut;

    public StandardOutput(final OutputStream outputStream) {
        countingOut = new ByteCountingOutputStream(outputStream);
        bufferedOut = new BufferedOutputStream(countingOut);
        interruptableOut = new InterruptableOutputStream(bufferedOut);
    }

    @Override
    public OutputStream getOutputStream() throws IOException {
        return interruptableOut;
    }

    public void setTimeout(final int timeout) {
    }

    @Override
    public long getBytesWritten() {
        return countingOut.getBytesWritten();
    }

    public void interrupt() {
        interruptableOut.interrupt();
    }
}
