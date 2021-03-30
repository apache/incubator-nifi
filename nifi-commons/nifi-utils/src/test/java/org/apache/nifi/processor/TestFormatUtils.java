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
package org.apache.nifi.processor;

import org.apache.nifi.util.FormatUtils;
import org.junit.Test;

import java.text.DecimalFormatSymbols;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class TestFormatUtils {

    private static String NEW_YORK_TIME_ZONE_ID = "America/New_York";
    private static String KIEV_TIME_ZONE_ID = "Europe/Kiev";

    @Test
    public void testParse() {
        assertEquals(3, FormatUtils.getTimeDuration("3000 ms", TimeUnit.SECONDS));
        assertEquals(3000, FormatUtils.getTimeDuration("3000 s", TimeUnit.SECONDS));
        assertEquals(0, FormatUtils.getTimeDuration("999 millis", TimeUnit.SECONDS));
        assertEquals(4L * 24L * 60L * 60L * 1000000000L, FormatUtils.getTimeDuration("4 days", TimeUnit.NANOSECONDS));
        assertEquals(24, FormatUtils.getTimeDuration("1 DAY", TimeUnit.HOURS));
        assertEquals(60, FormatUtils.getTimeDuration("1 hr", TimeUnit.MINUTES));
        assertEquals(60, FormatUtils.getTimeDuration("1 Hrs", TimeUnit.MINUTES));
    }

    @Test
    public void testFormatTime() throws Exception {
        assertEquals("00:00:00.000", FormatUtils.formatHoursMinutesSeconds(0, TimeUnit.DAYS));
        assertEquals("01:00:00.000", FormatUtils.formatHoursMinutesSeconds(1, TimeUnit.HOURS));
        assertEquals("02:00:00.000", FormatUtils.formatHoursMinutesSeconds(2, TimeUnit.HOURS));
        assertEquals("00:01:00.000", FormatUtils.formatHoursMinutesSeconds(1, TimeUnit.MINUTES));
        assertEquals("00:00:10.000", FormatUtils.formatHoursMinutesSeconds(10, TimeUnit.SECONDS));
        assertEquals("00:00:00.777", FormatUtils.formatHoursMinutesSeconds(777, TimeUnit.MILLISECONDS));
        assertEquals("00:00:07.777", FormatUtils.formatHoursMinutesSeconds(7777, TimeUnit.MILLISECONDS));

        assertEquals("20:11:36.897", FormatUtils.formatHoursMinutesSeconds(TimeUnit.MILLISECONDS.convert(20, TimeUnit.HOURS)
                + TimeUnit.MILLISECONDS.convert(11, TimeUnit.MINUTES)
                + TimeUnit.MILLISECONDS.convert(36, TimeUnit.SECONDS)
                + TimeUnit.MILLISECONDS.convert(897, TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS));

        assertEquals("1000:01:01.001", FormatUtils.formatHoursMinutesSeconds(TimeUnit.MILLISECONDS.convert(999, TimeUnit.HOURS)
                + TimeUnit.MILLISECONDS.convert(60, TimeUnit.MINUTES)
                + TimeUnit.MILLISECONDS.convert(60, TimeUnit.SECONDS)
                + TimeUnit.MILLISECONDS.convert(1001, TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS));
    }


    @Test
    public void testFormatNanos() {
        assertEquals("0 nanos", FormatUtils.formatNanos(0L, false));
        assertEquals("0 nanos (0 nanos)", FormatUtils.formatNanos(0L, true));

        assertEquals("1 millis, 0 nanos", FormatUtils.formatNanos(1_000_000L, false));
        assertEquals("1 millis, 0 nanos (1000000 nanos)", FormatUtils.formatNanos(1_000_000L, true));

        assertEquals("1 millis, 1 nanos", FormatUtils.formatNanos(1_000_001L, false));
        assertEquals("1 millis, 1 nanos (1000001 nanos)", FormatUtils.formatNanos(1_000_001L, true));

        assertEquals("1 seconds, 0 millis, 0 nanos", FormatUtils.formatNanos(1_000_000_000L, false));
        assertEquals(
            "1 seconds, 0 millis, 0 nanos (1000000000 nanos)",
            FormatUtils.formatNanos(1_000_000_000L, true));

        assertEquals("1 seconds, 1 millis, 0 nanos", FormatUtils.formatNanos(1_001_000_000L, false));
        assertEquals(
            "1 seconds, 1 millis, 0 nanos (1001000000 nanos)",
            FormatUtils.formatNanos(1_001_000_000L, true));

        assertEquals("1 seconds, 1 millis, 1 nanos", FormatUtils.formatNanos(1_001_000_001L, false));
        assertEquals(
            "1 seconds, 1 millis, 1 nanos (1001000001 nanos)",
            FormatUtils.formatNanos(1_001_000_001L, true));
    }

    @Test
    public void testFormatDataSize() {
        DecimalFormatSymbols decimalFormatSymbols = DecimalFormatSymbols.getInstance();
        assertEquals("0 bytes", FormatUtils.formatDataSize(0d));
        assertEquals(String.format("10%s4 bytes", decimalFormatSymbols.getDecimalSeparator()), FormatUtils.formatDataSize(10.4d));
        assertEquals(String.format("1%s024 bytes", decimalFormatSymbols.getGroupingSeparator()), FormatUtils.formatDataSize(1024d));

        assertEquals("1 KB", FormatUtils.formatDataSize(1025d));
        assertEquals(String.format("1%s95 KB", decimalFormatSymbols.getDecimalSeparator()), FormatUtils.formatDataSize(2000d));
        assertEquals(String.format("195%s31 KB", decimalFormatSymbols.getDecimalSeparator()), FormatUtils.formatDataSize(200_000d));

        assertEquals(String.format("190%s73 MB", decimalFormatSymbols.getDecimalSeparator()), FormatUtils.formatDataSize(200_000_000d));

        assertEquals(String.format("186%s26 GB", decimalFormatSymbols.getDecimalSeparator()), FormatUtils.formatDataSize(200_000_000_000d));

        assertEquals(String.format("181%s9 TB", decimalFormatSymbols.getDecimalSeparator()), FormatUtils.formatDataSize(200_000_000_000_000d));
    }

    @Test
    public void testParseToInstantUsingFormatterWithoutZones() throws Exception {
        // GMT-
        checkSameResultsWithSimpleDateFormat("yyyy-MM-dd HH:mm:ss", "2020-01-01 02:00:00", NEW_YORK_TIME_ZONE_ID, null, "2020-01-01T07:00:00");
        checkSameResultsWithSimpleDateFormat("yyyy-MM-dd", "2020-01-01", NEW_YORK_TIME_ZONE_ID, null, "2020-01-01T05:00:00");
        checkSameResultsWithSimpleDateFormat("HH:mm:ss", "03:00:00", NEW_YORK_TIME_ZONE_ID, null, "1970-01-01T08:00:00");
        checkSameResultsWithSimpleDateFormat("yyyy-MMM-dd", "2020-may-01", NEW_YORK_TIME_ZONE_ID, null, "2020-05-01T04:00:00");

        // GMT+
        checkSameResultsWithSimpleDateFormat("yyyy-MM-dd HH:mm:ss", "2020-01-01 02:00:00", KIEV_TIME_ZONE_ID, null, "2020-01-01T00:00:00");
        checkSameResultsWithSimpleDateFormat("yyyy-MM-dd", "2020-01-01", KIEV_TIME_ZONE_ID, null, "2019-12-31T22:00:00");
        checkSameResultsWithSimpleDateFormat("HH:mm:ss", "03:00:00", KIEV_TIME_ZONE_ID, null, "1970-01-01T00:00:00");
        checkSameResultsWithSimpleDateFormat("yyyy-MMM-dd", "2020-may-01", KIEV_TIME_ZONE_ID, null, "2020-04-30T21:00:00");

        // UTC
        checkSameResultsWithSimpleDateFormat("yyyy-MM-dd HH:mm:ss", "2020-01-01 02:00:00", ZoneOffset.UTC.getId(), null, "2020-01-01T02:00:00");
        checkSameResultsWithSimpleDateFormat("yyyy-MM-dd", "2020-01-01", ZoneOffset.UTC.getId(), null, "2020-01-01T00:00:00");
        checkSameResultsWithSimpleDateFormat("HH:mm:ss", "03:00:00", ZoneOffset.UTC.getId(), null, "1970-01-01T03:00:00");
        checkSameResultsWithSimpleDateFormat("yyyy-MMM-dd", "2020-may-01", ZoneOffset.UTC.getId(), null, "2020-05-01T00:00:00");
    }

    @Test
    public void testParseToInstantUsingFormatterWithZone() throws Exception {
        for (String systemDefaultZoneId : new String[]{ NEW_YORK_TIME_ZONE_ID, KIEV_TIME_ZONE_ID, ZoneOffset.UTC.getId()}) {
            checkSameResultsWithSimpleDateFormat("yyyy-MM-dd HH:mm:ss", "2020-01-01 02:00:00", systemDefaultZoneId, NEW_YORK_TIME_ZONE_ID, "2020-01-01T07:00:00");
            checkSameResultsWithSimpleDateFormat("yyyy-MM-dd HH:mm:ss", "2020-01-01 02:00:00", systemDefaultZoneId, KIEV_TIME_ZONE_ID, "2020-01-01T00:00:00");
            checkSameResultsWithSimpleDateFormat("yyyy-MM-dd HH:mm:ss", "2020-01-01 02:00:00", systemDefaultZoneId, ZoneOffset.UTC.getId(), "2020-01-01T02:00:00");
        }
    }

    @Test
    public void testParseToInstantWithZonePassedInText() throws Exception {
        for (String systemDefaultZoneId : new String[]{ NEW_YORK_TIME_ZONE_ID, KIEV_TIME_ZONE_ID, ZoneOffset.UTC.getId()}) {
            checkSameResultsWithSimpleDateFormat("yyyy-MM-dd HH:mm:ss Z", "2020-01-01 02:00:00 -0100", systemDefaultZoneId, null, "2020-01-01T03:00:00");
            checkSameResultsWithSimpleDateFormat("yyyy-MM-dd HH:mm:ss Z", "2020-01-01 02:00:00 +0100", systemDefaultZoneId, null, "2020-01-01T01:00:00");
            checkSameResultsWithSimpleDateFormat("yyyy-MM-dd HH:mm:ss Z", "2020-01-01 02:00:00 +0000", systemDefaultZoneId, null, "2020-01-01T02:00:00");
        }
    }

    private void checkSameResultsWithSimpleDateFormat(String pattern, String parsedDateTime, String systemDefaultZoneId, String formatZoneId, String expectedUtcDateTime) throws Exception {
        TimeZone current = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone(systemDefaultZoneId));
        try {
            checkSameResultsWithSimpleDateFormat(pattern, parsedDateTime, formatZoneId, expectedUtcDateTime);
        } finally {
            TimeZone.setDefault(current);
        }
    }

    private void checkSameResultsWithSimpleDateFormat(String pattern, String parsedDateTime, String formatterZoneId, String expectedUtcDateTime) throws Exception {
        Instant expectedInstant = LocalDateTime.parse(expectedUtcDateTime).atZone(ZoneOffset.UTC).toInstant();

        // reference implementation
        SimpleDateFormat sdf = new SimpleDateFormat(pattern, Locale.US);
        if (formatterZoneId != null) {
            sdf.setTimeZone(TimeZone.getTimeZone(formatterZoneId));
        }
        Instant simpleDateFormatResult = sdf.parse(parsedDateTime).toInstant();
        assertEquals(expectedInstant, simpleDateFormatResult);

        // current implementation
        DateTimeFormatter dtf = FormatUtils.prepareLenientCaseInsensitiveDateTimeFormatter(pattern);
        if (formatterZoneId != null) {
            dtf = dtf.withZone(ZoneId.of(formatterZoneId));
        }
        Instant result = FormatUtils.parseToInstant(dtf, parsedDateTime);
        assertEquals(expectedInstant, result);
    }

}
