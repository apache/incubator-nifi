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

package org.apache.nifi.processors.cipher;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.nifi.processors.cipher.VerifyContentMAC.Encoding.BASE64;
import static org.apache.nifi.processors.cipher.VerifyContentMAC.Encoding.HEXADECIMAL;
import static org.apache.nifi.processors.cipher.VerifyContentMAC.Encoding.UTF8;

import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.bouncycastle.util.encoders.Hex;

@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"Authentication", "Signing", "MAC", "HMAC"})
@CapabilityDescription("Calculates a Message Authentication Code using the provided Secret Key and compares it with the provided MAC property")
@WritesAttributes({
        @WritesAttribute(attribute = VerifyContentMAC.MAC_CALCULATED_ATTRIBUTE, description = "Calculated Message Authentication Code encoded by the selected encoding"),
        @WritesAttribute(attribute = VerifyContentMAC.MAC_ENCODING_ATTRIBUTE, description = "The Encoding of the Hashed Message Authentication Code"),
        @WritesAttribute(attribute = VerifyContentMAC.MAC_ALGORITHM_ATTRIBUTE, description = "Hashed Message Authentication Code Algorithm")
})
public class VerifyContentMAC extends AbstractProcessor {

    protected static final String HMAC_SHA256 = "HmacSHA256";
    protected static final String HMAC_SHA512 = "HmacSHA512";

    protected static final PropertyDescriptor MAC_ALGORITHM = new PropertyDescriptor.Builder()
            .name("mac-algorithm")
            .displayName("Message Authentication Code Algorithm")
            .description("Hashed Message Authentication Code Function")
            .allowableValues(HMAC_SHA256, HMAC_SHA512)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor MAC_ENCODING = new PropertyDescriptor.Builder()
            .name("message-authentication-code-encoding")
            .displayName("Message Authentication Code Encoding")
            .description("Encoding of the Message Authentication Code")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues(HEXADECIMAL.name(), BASE64.name())
            .defaultValue(HEXADECIMAL.name())
            .build();

    protected static final PropertyDescriptor MAC = new PropertyDescriptor.Builder()
            .name("message-authentication-code")
            .displayName("Message Authentication Code")
            .description("The MAC to compare with the calculated value")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    protected static final PropertyDescriptor SECRET_KEY_ENCODING = new PropertyDescriptor.Builder()
            .name("secret-key-encoding")
            .displayName("Secret Key Encoding")
            .description("Encoding of the Secret Key")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues(UTF8.name(), HEXADECIMAL.name(), BASE64.name())
            .defaultValue(HEXADECIMAL.name())
            .build();

    protected static final PropertyDescriptor SECRET_KEY = new PropertyDescriptor.Builder()
            .name("secret-key")
            .displayName("Secret Key")
            .description("Secret key to calculate the hash")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();

    protected static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Signature Verification Failed")
            .build();

    protected static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Signature Verification Succeeded")
            .build();

    protected static final String MAC_CALCULATED_ATTRIBUTE = "mac.calculated";
    protected static final String MAC_ALGORITHM_ATTRIBUTE = "mac.algorithm";
    protected static final String MAC_ENCODING_ATTRIBUTE = "mac.encoding";

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
                    MAC_ALGORITHM,
                    MAC_ENCODING,
                    MAC,
                    SECRET_KEY_ENCODING,
                    SECRET_KEY
    );

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            SUCCESS,
            FAILURE
    );

    private static final int BUFFER_SIZE = 512000;

    private SecretKeySpec secretKeySpec;
    private String macAlgorithm;
    private String macEncoding;

    @OnScheduled
    public void setUp(ProcessContext context) {
        macAlgorithm = context.getProperty(MAC_ALGORITHM).getValue();
        macEncoding = context.getProperty(MAC_ENCODING).getValue();
        String secretKeyEncoding = context.getProperty(SECRET_KEY_ENCODING).getValue();
        String inputSecretKey = context.getProperty(SECRET_KEY).getValue();

        byte[] secretKey = Encoding.valueOf(secretKeyEncoding).decode(inputSecretKey);

        secretKeySpec = new SecretKeySpec(secretKey, macAlgorithm);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String macEncoded = context.getProperty(MAC).evaluateAttributeExpressions(flowFile).getValue();

        try {
            final byte[] macDecoded = Encoding.valueOf(macEncoding).decode(macEncoded);
            final byte[] macCalculated = getCalculatedMac(session, flowFile);

            flowFile = setFlowFileAttributes(session, flowFile, macCalculated);

            if (MessageDigest.isEqual(macDecoded, macCalculated)) {
                session.transfer(flowFile, SUCCESS);
            } else {
                getLogger().info("Verification Failed with Message Authentication Code Algorithm [{}]", macAlgorithm);
                session.transfer(flowFile, FAILURE);
            }
        } catch (final Exception e) {
            getLogger().error("Processing Failed with Message Authentication Code Algorithm [{}]", macAlgorithm, e);
            session.transfer(flowFile, FAILURE);
        }
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>(super.customValidate(validationContext));

        final String secretKeyEncoding = validationContext.getProperty(SECRET_KEY_ENCODING).getValue();
        final String encodedSecretKey = validationContext.getProperty(SECRET_KEY).getValue();

        try {
            Encoding.valueOf(secretKeyEncoding).decode(encodedSecretKey);
        } catch (Exception e) {
            results.add(new ValidationResult.Builder()
                    .valid(false)
                    .subject(SECRET_KEY.getDisplayName())
                    .explanation("The provided Secret Key is not a valid " + secretKeyEncoding + " value")
                    .build());
        }

        return results;
    }

    private FlowFile setFlowFileAttributes(ProcessSession session, FlowFile flowFile, byte[] calculatedMac) {
        Map<String, String> attributes = new HashMap<>();
        attributes.put(MAC_ALGORITHM_ATTRIBUTE, macAlgorithm);
        attributes.put(MAC_ENCODING_ATTRIBUTE, macEncoding);
        attributes.put(MAC_CALCULATED_ATTRIBUTE, Encoding.valueOf(macEncoding).encode(calculatedMac));
        return session.putAllAttributes(flowFile, attributes);
    }

    private Mac getInitializedMac() {
        try {
            Mac mac = Mac.getInstance(macAlgorithm);
            mac.init(secretKeySpec);
            return mac;
        } catch (final NoSuchAlgorithmException | InvalidKeyException e) {
            throw new ProcessException("HMAC initialization failed", e);
        }
    }

    private byte[] getCalculatedMac(ProcessSession session, FlowFile flowFile) {
        Mac mac = getInitializedMac();

        byte[] contents = new byte[BUFFER_SIZE];
        int readSize;

        try (InputStream is = session.read(flowFile)) {
            while ((readSize = is.read(contents)) != -1) {
                mac.update(contents, 0, readSize);
            }
        } catch (final IOException e) {
            throw new ProcessException("File processing failed", e);
        }

        return mac.doFinal();
    }

    enum Encoding {
        HEXADECIMAL(Hex::toHexString, Hex::decode),
        BASE64(value -> Base64.getEncoder().encodeToString(value), value -> Base64.getDecoder().decode(value)),
        UTF8(value -> new String(value, UTF_8), value -> value.getBytes(UTF_8));

        private final Function<byte[], String> encodeFunction;
        private final Function<String, byte[]> decodeFunction;

        Encoding(Function<byte[], String> encodeFunction, Function<String, byte[]> decodeFunction) {
            this.decodeFunction = decodeFunction;
            this.encodeFunction = encodeFunction;
        }

        public byte[] decode(String value) {
            return decodeFunction.apply(value);
        }

        public String encode(byte[] value) {
            return encodeFunction.apply(value);
        }
    }
}
