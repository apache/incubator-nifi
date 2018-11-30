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
package org.apache.nifi.util


import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.util.concurrent.TimeUnit

@RunWith(JUnit4.class)
class TestFormatUtilsGroovy extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(TestFormatUtilsGroovy.class)

    @BeforeClass
    static void setUpOnce() throws Exception {
        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }
    }

    @Before
    void setUp() throws Exception {

    }

    @After
    void tearDown() throws Exception {

    }

    /**
     * New feature test
     */
    @Test
    void testShouldConvertWeeks() {
        // Arrange
        final List WEEKS = ["1 week", "1 wk", "1 w", "1 wks", "1 weeks"]
        final long EXPECTED_DAYS = 7L

        // Act
        List days = WEEKS.collect { String week ->
            FormatUtils.getTimeDuration(week, TimeUnit.DAYS)
        }
        logger.converted(days)

        // Assert
        assert days.every { it == EXPECTED_DAYS }
    }


    @Test
    void testShouldHandleNegativeWeeks() {
        // Arrange
        final List WEEKS = ["-1 week", "-1 wk", "-1 w", "-1 weeks", "- 1 week"]

        // Act
        List msgs = WEEKS.collect { String week ->
            shouldFail(IllegalArgumentException) {
                FormatUtils.getTimeDuration(week, TimeUnit.DAYS)
            }
        }

        // Assert
        assert msgs.every { it =~ /Value '.*' is not a valid Time Duration/ }
    }

    /**
     * Regression test
     */
    @Test
    void testShouldHandleInvalidAbbreviations() {
        // Arrange
        final List WEEKS = ["1 work", "1 wek", "1 k"]

        // Act
        List msgs = WEEKS.collect { String week ->
            shouldFail(IllegalArgumentException) {
                FormatUtils.getTimeDuration(week, TimeUnit.DAYS)
            }
        }

        // Assert
        assert msgs.every { it =~ /Value '.*' is not a valid Time Duration/ }

    }

    /**
     * New feature test
     */
    @Test
    void testShouldHandleNoSpaceInInput() {
        // Arrange
        final List WEEKS = ["1week", "1wk", "1w", "1wks", "1weeks"]
        final long EXPECTED_DAYS = 7L

        // Act
        List days = WEEKS.collect { String week ->
            FormatUtils.getTimeDuration(week, TimeUnit.DAYS)
        }
        logger.converted(days)

        // Assert
        assert days.every { it == EXPECTED_DAYS }
    }

    /**
     * New feature test
     */
    @Test
    void testShouldHandleDecimalValues() {
        // Arrange
        final List WHOLE_NUMBERS = ["10 ms", "10 millis", "10 milliseconds"]
        final List DECIMAL_NUMBERS = ["0.010 s", "0.010 seconds"]
        final long EXPECTED_MILLIS = 10

        // Act
        List parsedWholeMillis = WHOLE_NUMBERS.collect { String whole ->
            FormatUtils.getTimeDuration(whole, TimeUnit.MILLISECONDS)
        }
        logger.converted(parsedWholeMillis)

        List parsedDecimalMillis = DECIMAL_NUMBERS.collect { String decimal ->
            FormatUtils.getTimeDuration(decimal, TimeUnit.MILLISECONDS)
        }
        logger.converted(parsedDecimalMillis)

        // Assert
        assert parsedWholeMillis.every { it == EXPECTED_MILLIS }
        assert parsedDecimalMillis.every { it == EXPECTED_MILLIS }
    }

    /**
     * New feature test
     */
    @Test
    void testGetPreciseTimeDurationShouldHandleDecimalValues() {
        // Arrange
        final List WHOLE_NUMBERS = ["10 ms", "10 millis", "10 milliseconds"]
        final List DECIMAL_NUMBERS = ["0.010 s", "0.010 seconds"]
        final float EXPECTED_MILLIS = 10.0

        // Act
        List parsedWholeMillis = WHOLE_NUMBERS.collect { String whole ->
            FormatUtils.getPreciseTimeDuration(whole, TimeUnit.MILLISECONDS)
        }
        logger.converted(parsedWholeMillis)

        List parsedDecimalMillis = DECIMAL_NUMBERS.collect { String decimal ->
            FormatUtils.getPreciseTimeDuration(decimal, TimeUnit.MILLISECONDS)
        }
        logger.converted(parsedDecimalMillis)

        // Assert
        assert parsedWholeMillis.every { it == EXPECTED_MILLIS }
        assert parsedDecimalMillis.every { it == EXPECTED_MILLIS }
    }

    /**
     * Positive flow test for decimal inputs that can be converted (all equal values)
     */
    @Test
    void testShouldMakeWholeNumberTime() {
        // Arrange
        final List DECIMAL_TIMES = [
                [0.000_000_010, TimeUnit.SECONDS],
                [0.000_010, TimeUnit.MILLISECONDS],
                [0.010, TimeUnit.MICROSECONDS]
        ]
        final long EXPECTED_NANOS = 10L

        // Act
        List parsedWholeNanos = DECIMAL_TIMES.collect { List it ->
            FormatUtils.makeWholeNumberTime(it[0] as float, it[1] as TimeUnit)
        }
        logger.converted(parsedWholeNanos)

        // Assert
        assert parsedWholeNanos.every { it == [EXPECTED_NANOS, TimeUnit.NANOSECONDS] }
    }

    // TODO: Non-metric units (i.e. days to hours, hours to minutes, minutes to seconds)

    /**
     * Positive flow test for whole inputs
     */
    @Test
    void testMakeWholeNumberTimeShouldHandleWholeNumbers() {
        // Arrange
        final List WHOLE_TIMES = [
                [10.0, TimeUnit.DAYS],
                [10.0, TimeUnit.HOURS],
                [10.0, TimeUnit.MINUTES],
                [10.0, TimeUnit.SECONDS],
                [10.0, TimeUnit.MILLISECONDS],
                [10.0, TimeUnit.MICROSECONDS],
                [10.0, TimeUnit.NANOSECONDS],
        ]

        // Act
        List parsedWholeTimes = WHOLE_TIMES.collect { List it ->
            FormatUtils.makeWholeNumberTime(it[0] as float, it[1] as TimeUnit)
        }
        logger.converted(parsedWholeTimes)

        // Assert
        parsedWholeTimes.eachWithIndex { List elements, int i ->
            assert elements[0] instanceof Long
            assert elements[0] == 10L
            assert elements[1] == WHOLE_TIMES[i][1]
        }
    }

    /**
     * Negative flow test for nanosecond inputs (regardless of value, the unit cannot be converted)
     */
    @Test
    void testMakeWholeNumberTimeShouldHandleNanoseconds() {
        // Arrange
        final List WHOLE_TIMES = [
                [1100.0, TimeUnit.NANOSECONDS],
                [2.1, TimeUnit.NANOSECONDS],
                [1.0, TimeUnit.NANOSECONDS],
                [0.1, TimeUnit.NANOSECONDS],
        ]

        final List EXPECTED_TIMES = [
                [1100L, TimeUnit.NANOSECONDS],
                [2L, TimeUnit.NANOSECONDS],
                [1L, TimeUnit.NANOSECONDS],
                [1L, TimeUnit.NANOSECONDS],
        ]

        // Act
        List parsedWholeTimes = WHOLE_TIMES.collect { List it ->
            FormatUtils.makeWholeNumberTime(it[0] as float, it[1] as TimeUnit)
        }
        logger.converted(parsedWholeTimes)

        // Assert
        assert parsedWholeTimes == EXPECTED_TIMES
    }

    /**
     * Positive flow test for whole inputs
     */
    @Test
    void testShouldGetSmallerTimeUnit() {
        // Arrange
        final List UNITS = TimeUnit.values() as List

        // Act
        def nullMsg = shouldFail(IllegalArgumentException) {
            FormatUtils.getSmallerTimeUnit(null)
        }
        logger.expected(nullMsg)

        def nanosMsg = shouldFail(IllegalArgumentException) {
            FormatUtils.getSmallerTimeUnit(TimeUnit.NANOSECONDS)
        }
        logger.expected(nanosMsg)

        List smallerTimeUnits = UNITS[1..-1].collect { TimeUnit unit ->
            FormatUtils.getSmallerTimeUnit(unit)
        }
        logger.converted(smallerTimeUnits)

        // Assert
        assert nullMsg == "Cannot determine a smaller time unit than 'null'"
        assert nanosMsg == "Cannot determine a smaller time unit than 'NANOSECONDS'"
        assert smallerTimeUnits == UNITS[0..<-1]
    }

    /**
     * Positive flow test for multipliers based on valid time units
     */
    @Test
    void testShouldCalculateMultiplier() {
        // Arrange
        final Map SCENARIOS = [
                "allUnits"      : [original: TimeUnit.DAYS, destination: TimeUnit.NANOSECONDS, expectedMultiplier: (long) 24 * 60 * 60 * 1_000_000_000],
                "microsToNanos" : [original: TimeUnit.MICROSECONDS, destination: TimeUnit.NANOSECONDS, expectedMultiplier: 1_000],
                "millisToNanos" : [original: TimeUnit.MILLISECONDS, destination: TimeUnit.NANOSECONDS, expectedMultiplier: 1_000_000],
                "millisToMicros": [original: TimeUnit.MILLISECONDS, destination: TimeUnit.MICROSECONDS, expectedMultiplier: 1_000],
                "daysToHours"   : [original: TimeUnit.DAYS, destination: TimeUnit.HOURS, expectedMultiplier: 24],
                "daysToSeconds" : [original: TimeUnit.DAYS, destination: TimeUnit.SECONDS, expectedMultiplier: 24 * 60 * 60],
        ]

        // Act
        Map results = SCENARIOS.collectEntries { String k, Map values ->
            logger.debug("Evaluating ${k}: ${values}")
            [k, FormatUtils.calculateMultiplier(values.original, values.destination)]
        }
        logger.converted(results)

        // Assert
        results.every { String key, long value ->
            assert value == SCENARIOS[key].expectedMultiplier
        }
    }

    /**
     * Negative flow test for multipliers based on incorrectly-ordered time units
     */
    @Test
    void testCalculateMultiplierShouldHandleIncorrectUnits() {
        // Arrange
        final Map SCENARIOS = [
                "allUnits"      : [original: TimeUnit.NANOSECONDS, destination: TimeUnit.DAYS],
                "nanosToMicros" : [original: TimeUnit.NANOSECONDS, destination: TimeUnit.MICROSECONDS],
                "hoursToDays"   : [original: TimeUnit.HOURS, destination: TimeUnit.DAYS],
        ]

        // Act
        Map results = SCENARIOS.collectEntries { String k, Map values ->
            logger.debug("Evaluating ${k}: ${values}")
            def msg = shouldFail(IllegalArgumentException) {
                FormatUtils.calculateMultiplier(values.original, values.destination)
            }
            logger.expected(msg)
            [k, msg]
        }

        // Assert
        results.every { String key, String value ->
            assert value =~ "The original time unit '.*' must be larger than the new time unit '.*'"
        }
    }

    // TODO: Microsecond parsing
}