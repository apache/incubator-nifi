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
package org.apache.nifi.processor.util.listen;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;

import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.InetAddress;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;

/**
 * Shared properties.
 */
public class ListenerProperties {

    private static final Set<String> ipSet = new HashSet<>();

    static {
        try {
            final Enumeration<NetworkInterface> interfaceEnum = NetworkInterface.getNetworkInterfaces();
            while (interfaceEnum.hasMoreElements()) {
                final NetworkInterface ifc = interfaceEnum.nextElement();

                final Enumeration<InetAddress> ipEnum = ifc.getInetAddresses();
                while (ipEnum.hasMoreElements()) {
                    final InetAddress ip = ipEnum.nextElement();
                    ipSet.add(ip.getHostAddress());
                }
            }
        } catch (SocketException e) {
        }
    }

    public static final PropertyDescriptor LOCAL_IP_ADDRESS = new PropertyDescriptor.Builder()
            .name("Local IP Address")
            .description("The IP address of a local network interface to be used to restrict listening to a specific LAN.")
            .addValidator(new Validator() {
                @Override
                public ValidationResult validate(String subject, String input, ValidationContext context) {
                    ValidationResult result = new ValidationResult.Builder()
                            .subject("Local IP Address").valid(true).input(input).build();
                    if (ipSet.contains(input.toLowerCase())) {
                        return result;
                    }

                    String message;
                    String realValue = input;
                    try {
                        if (context.isExpressionLanguagePresent(input)) {
                            AttributeExpression ae = context.newExpressionLanguageCompiler().compile(input);
                            realValue = ae.evaluate();
                        }

                        if (ipSet.contains(realValue.toLowerCase())) {
                            return result;
                        }

                        message = realValue + " is not a valid IP address. Valid addresses are " + ipSet.toString();

                    } catch (IllegalArgumentException e) {
                        message = "Not a valid AttributeExpression: " + e.getMessage();
                    }
                    result = new ValidationResult.Builder().subject("Local IP Address")
                            .valid(false).input(input).explanation(message).build();

                    return result;
                }
            })
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

}
