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
package org.apache.nifi.questdb.util;

import java.time.Instant;
import java.util.Objects;

/**
 * This class exists for test purposes
 */
public class Event {
    private Instant capturedAt;
    private String subject;
    private long value;

    public Event() {}

    public Event(final Instant capturedAt, final String subject, final long value) {
        this.capturedAt = capturedAt;
        this.subject = subject;
        this.value = value;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(final String subject) {
        this.subject = subject;
    }

    public long getValue() {
        return value;
    }

    public void setValue(final long value) {
        this.value = value;
    }

    public Instant getCapturedAt() {
        return capturedAt;
    }

    public void setCapturedAt(final Instant capturedAt) {
        this.capturedAt = capturedAt;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final Event event = (Event) o;
        return value == event.value && Objects.equals(capturedAt, event.capturedAt) && Objects.equals(subject, event.subject);
    }

    @Override
    public int hashCode() {
        return Objects.hash(capturedAt, subject, value);
    }

    @Override
    public String toString() {
        return "com.github.simonbence.questdb7.Event{" +
                "capturedAt=" + capturedAt +
                ", subject='" + subject + '\'' +
                ", value=" + value +
                '}';
    }
}
