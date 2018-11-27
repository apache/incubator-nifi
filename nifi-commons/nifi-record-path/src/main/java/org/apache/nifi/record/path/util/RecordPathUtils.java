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

package org.apache.nifi.record.path.util;

import java.util.Optional;

import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPathEvaluationContext;
import org.apache.nifi.record.path.paths.RecordPathSegment;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

public class RecordPathUtils {

    public static String getFirstStringValue(final RecordPathSegment segment, final RecordPathEvaluationContext context) {
        final Optional<FieldValue> stringFieldValue = segment.evaluate(context).findFirst();
        if (!stringFieldValue.isPresent()) {
            return null;
        }

        final String stringValue = DataTypeUtils.toString(stringFieldValue.get().getValue(), (String) null);
        if (stringValue == null) {
            return null;
        }

        return stringValue;
    }

    /**
     * This method handles backslash sequences after ANTLR parser converts all backslash into double ones
     * with exception for \t, \r and \n. See
     * <a href="file:../../../../../../../../../src/main/antlr3/org/apache/nifi/record/path/RecordPathParser.g">org/apache/nifi/record/path/RecordPathParser.g</a>
     *
     * @param value to be handled
     * @return transformed string from given value.
     */
    public static String unescapeBackslash(String value) {
        if (value == null || value.isEmpty()) {
            return value;
        }
        // need to escape characters after backslashes
        final StringBuilder sb = new StringBuilder();
        boolean lastCharIsBackslash = false;
        for (int i = 0; i < value.length(); i++) {
            final char c = value.charAt(i);

            if (lastCharIsBackslash) {
                switch (c) {
                case 'n':
                    sb.append("\n");
                    break;
                case 'r':
                    sb.append("\r");
                    break;
                case '\\':
                    sb.append("\\");
                    break;
                case 't':
                    sb.append("\\t");
                    break;
                default:
                    sb.append("\\").append(c);
                    break;
                }

                lastCharIsBackslash = false;
            } else if (c == '\\') {
                lastCharIsBackslash = true;
            } else {
                sb.append(c);
            }
        }

        return sb.toString();
    }
}
