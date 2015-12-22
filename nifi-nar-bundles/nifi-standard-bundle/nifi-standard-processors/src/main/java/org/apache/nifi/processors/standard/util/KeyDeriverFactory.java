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
package org.apache.nifi.processors.standard.util;

import org.apache.nifi.security.util.KeyDerivationFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class KeyDeriverFactory {
    private static final Logger logger = LoggerFactory.getLogger(KeyDeriverFactory.class);

    private static Map<KeyDerivationFunction, Class<? extends KeyDeriver>> registeredKeyDerivers;

    public static KeyDeriver getDeriver(KeyDerivationFunction kdf) {
        logger.debug("{} KDFs registered", registeredKeyDerivers.size());

//        if (registeredKeyDerivers.containsKey(kdf)) {
//            Class<? extends KeyDeriver> clazz = registeredKeyDerivers.get(kdf);
//            return clazz.newInstance();
//        }

        return null;
    }
}
