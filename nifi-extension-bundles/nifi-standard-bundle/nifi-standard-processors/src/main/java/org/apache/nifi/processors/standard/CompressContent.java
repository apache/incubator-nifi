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
package org.apache.nifi.processors.standard;

import com.aayushatharva.brotli4j.Brotli4jLoader;
import com.aayushatharva.brotli4j.decoder.BrotliInputStream;
import com.aayushatharva.brotli4j.encoder.BrotliOutputStream;
import com.aayushatharva.brotli4j.encoder.Encoder;
import lzma.sdk.lzma.Decoder;
import lzma.streams.LzmaInputStream;
import lzma.streams.LzmaOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorInputStream;
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorInputStream;
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorOutputStream;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.MultiProcessorUseCase;
import org.apache.nifi.annotation.documentation.ProcessorConfiguration;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.documentation.UseCase;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.stream.io.GZIPOutputStream;
import org.apache.nifi.util.StopWatch;
import org.tukaani.xz.LZMA2Options;
import org.tukaani.xz.XZInputStream;
import org.tukaani.xz.XZOutputStream;
import org.xerial.snappy.SnappyFramedInputStream;
import org.xerial.snappy.SnappyFramedOutputStream;
import org.xerial.snappy.SnappyHadoopCompatibleOutputStream;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"content", "compress", "decompress", "gzip", "bzip2", "lzma", "xz-lzma2", "snappy", "snappy-hadoop", "snappy framed", "lz4-framed", "deflate", "zstd", "brotli"})
@CapabilityDescription("Compresses or decompresses the contents of FlowFiles using a user-specified compression algorithm and updates the mime.type "
    + "attribute as appropriate. A common idiom is to precede CompressContent with IdentifyMimeType and configure Mode='decompress' AND Compression Format='use mime.type attribute'. "
    + "When used in this manner, the MIME type is automatically detected and the data is decompressed, if necessary. "
    + "If decompression is unnecessary, the data is passed through to the 'success' relationship."
    + " This processor operates in a very memory efficient way so very large objects well beyond the heap size are generally fine to process.")
@ReadsAttribute(attribute = "mime.type", description = "If the Compression Format is set to use mime.type attribute, this attribute is used to "
    + "determine the compression type. Otherwise, this attribute is ignored.")
@WritesAttribute(attribute = "mime.type", description = "If the Mode property is set to compress, the appropriate MIME Type is set. If the Mode "
    + "property is set to decompress and the file is successfully decompressed, this attribute is removed, as the MIME Type is no longer known.")
@SystemResourceConsideration(resource = SystemResource.CPU)
@SystemResourceConsideration(resource = SystemResource.MEMORY)
@UseCase(
    description = "Compress the contents of a FlowFile",
    configuration = """
        "Mode" = "compress"
        "Compression Format" should be set to whichever compression algorithm should be used."""
)
@UseCase(
    description = "Decompress the contents of a FlowFile",
    configuration = """
        "Mode" = "decompress"
        "Compression Format" should be set to whichever compression algorithm was used to compress the data previously."""
)
@MultiProcessorUseCase(
    description = "Check whether or not a FlowFile is compressed and if so, decompress it.",
    notes = "If IdentifyMimeType determines that the content is not compressed, CompressContent will pass the FlowFile " +
        "along to the 'success' relationship without attempting to decompress it.",
    keywords = {"auto", "detect", "mime type", "compress", "decompress", "gzip", "bzip2"},
    configurations = {
        @ProcessorConfiguration(
            processorClass = IdentifyMimeType.class,
            configuration = """
                Default property values are sufficient.
                Connect the 'success' relationship to CompressContent.
                """
        ),
        @ProcessorConfiguration(
            processorClass = CompressContent.class,
            configuration = """
                "Mode" = "decompress"
                "Compression Format" = "use mime.type attribute"
                """
        )
    }
)
public class CompressContent extends AbstractProcessor {

    public static final String COMPRESSION_FORMAT_ATTRIBUTE = "use mime.type attribute";
    public static final String COMPRESSION_FORMAT_GZIP = "gzip";
    public static final String COMPRESSION_FORMAT_DEFLATE = "deflate";
    public static final String COMPRESSION_FORMAT_BZIP2 = "bzip2";
    public static final String COMPRESSION_FORMAT_XZ_LZMA2 = "xz-lzma2";
    public static final String COMPRESSION_FORMAT_LZMA = "lzma";
    public static final String COMPRESSION_FORMAT_SNAPPY = "snappy";
    public static final String COMPRESSION_FORMAT_SNAPPY_HADOOP = "snappy-hadoop";
    public static final String COMPRESSION_FORMAT_SNAPPY_FRAMED = "snappy framed";
    public static final String COMPRESSION_FORMAT_LZ4_FRAMED = "lz4-framed";
    public static final String COMPRESSION_FORMAT_ZSTD = "zstd";
    public static final String COMPRESSION_FORMAT_BROTLI = "brotli";

    public static final String MODE_COMPRESS = "compress";
    public static final String MODE_DECOMPRESS = "decompress";

    public static final PropertyDescriptor COMPRESSION_FORMAT = new PropertyDescriptor.Builder()
        .name("Compression Format")
        .description("The compression format to use. Valid values are: GZIP, Deflate, ZSTD, BZIP2, XZ-LZMA2, LZMA, Brotli, Snappy, Snappy Hadoop, Snappy Framed, and LZ4-Framed")
        .allowableValues(COMPRESSION_FORMAT_ATTRIBUTE, COMPRESSION_FORMAT_GZIP, COMPRESSION_FORMAT_DEFLATE, COMPRESSION_FORMAT_BZIP2,
            COMPRESSION_FORMAT_XZ_LZMA2, COMPRESSION_FORMAT_LZMA, COMPRESSION_FORMAT_SNAPPY, COMPRESSION_FORMAT_SNAPPY_HADOOP, COMPRESSION_FORMAT_SNAPPY_FRAMED,
            COMPRESSION_FORMAT_LZ4_FRAMED, COMPRESSION_FORMAT_ZSTD, COMPRESSION_FORMAT_BROTLI)
        .defaultValue(COMPRESSION_FORMAT_ATTRIBUTE)
        .required(true)
        .build();
    public static final PropertyDescriptor MODE = new PropertyDescriptor.Builder()
        .name("Mode")
        .description("Indicates whether the processor should compress content or decompress content. Must be either 'compress' or 'decompress'")
        .allowableValues(MODE_COMPRESS, MODE_DECOMPRESS)
        .defaultValue(MODE_COMPRESS)
        .required(true)
        .build();
    public static final PropertyDescriptor COMPRESSION_LEVEL = new PropertyDescriptor.Builder()
        .name("Compression Level")
        .description("The compression level to use; this is valid only when using gzip, deflate or xz-lzma2 compression. A lower value results in faster processing "
            + "but less compression; a value of 0 indicates no (that is, simple archiving) for gzip or minimal for xz-lzma2 compression."
            + " Higher levels can mean much larger memory usage such as the case with levels 7-9 for xz-lzma/2 so be careful relative to heap size.")
        .defaultValue("1")
        .required(true)
        .allowableValues("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
        .dependsOn(COMPRESSION_FORMAT, COMPRESSION_FORMAT_ATTRIBUTE, COMPRESSION_FORMAT_GZIP, COMPRESSION_FORMAT_DEFLATE,
                   COMPRESSION_FORMAT_XZ_LZMA2, COMPRESSION_FORMAT_ZSTD, COMPRESSION_FORMAT_BROTLI)
        .dependsOn(MODE, MODE_COMPRESS)
        .build();

    public static final PropertyDescriptor UPDATE_FILENAME = new PropertyDescriptor.Builder()
        .name("Update Filename")
        .description("If true, will remove the filename extension when decompressing data (only if the extension indicates the appropriate "
            + "compression format) and add the appropriate extension when compressing data")
        .required(true)
        .allowableValues("true", "false")
        .defaultValue("false")
        .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            MODE,
            COMPRESSION_FORMAT,
            COMPRESSION_LEVEL,
            UPDATE_FILENAME
    );

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("FlowFiles will be transferred to the success relationship after successfully being compressed or decompressed")
        .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("FlowFiles will be transferred to the failure relationship if they fail to compress/decompress")
        .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE
    );

    private final Map<String, String> compressionFormatMimeTypeMap = Map.ofEntries(
        Map.entry("application/gzip", COMPRESSION_FORMAT_GZIP),
        Map.entry("application/x-gzip", COMPRESSION_FORMAT_GZIP),
        Map.entry("application/deflate", COMPRESSION_FORMAT_DEFLATE),
        Map.entry("application/x-deflate", COMPRESSION_FORMAT_DEFLATE),
        Map.entry("application/bzip2", COMPRESSION_FORMAT_BZIP2),
        Map.entry("application/x-bzip2", COMPRESSION_FORMAT_BZIP2),
        Map.entry("application/x-lzma", COMPRESSION_FORMAT_LZMA),
        Map.entry("application/x-snappy", COMPRESSION_FORMAT_SNAPPY),
        Map.entry("application/x-snappy-hadoop", COMPRESSION_FORMAT_SNAPPY_HADOOP),
        Map.entry("application/x-snappy-framed", COMPRESSION_FORMAT_SNAPPY_FRAMED),
        Map.entry("application/x-lz4-framed", COMPRESSION_FORMAT_LZ4_FRAMED),
        Map.entry("application/zstd", COMPRESSION_FORMAT_ZSTD),
        Map.entry("application/x-brotli", COMPRESSION_FORMAT_BROTLI));


    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final List<ValidationResult> validationResults = new ArrayList<>(super.customValidate(context));

        if (context.getProperty(COMPRESSION_FORMAT).getValue().equalsIgnoreCase(COMPRESSION_FORMAT_SNAPPY_HADOOP)
            && context.getProperty(MODE).getValue().equalsIgnoreCase(MODE_DECOMPRESS)) {

            validationResults.add(new ValidationResult.Builder()
                .subject(COMPRESSION_FORMAT.getName())
                .explanation("<Compression Format> set to <snappy-hadoop> and <MODE> set to <decompress> is not permitted. " +
                    "Data that is compressed with Snappy Hadoop can not be decompressed using this processor.")
                .valid(false)
                .build());
        }

        return validationResults;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();
        final long sizeBeforeCompression = flowFile.getSize();
        final String compressionMode = context.getProperty(MODE).getValue();

        String compressionFormatValue = context.getProperty(COMPRESSION_FORMAT).getValue();
        if (compressionFormatValue.equals(COMPRESSION_FORMAT_ATTRIBUTE)) {
            final String mimeType = flowFile.getAttribute(CoreAttributes.MIME_TYPE.key());
            if (mimeType == null) {
                logger.error("No {} attribute exists for {}; routing to failure", CoreAttributes.MIME_TYPE.key(), flowFile);
                session.transfer(flowFile, REL_FAILURE);
                return;
            }

            compressionFormatValue = compressionFormatMimeTypeMap.get(mimeType);
            if (compressionFormatValue == null) {
                logger.info("Mime Type of {} is '{}', which does not indicate a supported Compression Format; routing to success without decompressing", flowFile, mimeType);
                session.transfer(flowFile, REL_SUCCESS);
                return;
            }
        }

        final String compressionFormat = compressionFormatValue;
        final AtomicReference<String> mimeTypeRef = new AtomicReference<>(null);
        final StopWatch stopWatch = new StopWatch(true);

        final String fileExtension = switch (compressionFormat.toLowerCase()) {
            case COMPRESSION_FORMAT_GZIP -> ".gz";
            case COMPRESSION_FORMAT_DEFLATE -> ".zlib";
            case COMPRESSION_FORMAT_LZMA -> ".lzma";
            case COMPRESSION_FORMAT_XZ_LZMA2 -> ".xz";
            case COMPRESSION_FORMAT_BZIP2 -> ".bz2";
            case COMPRESSION_FORMAT_SNAPPY -> ".snappy";
            case COMPRESSION_FORMAT_SNAPPY_HADOOP -> ".snappy";
            case COMPRESSION_FORMAT_SNAPPY_FRAMED -> ".sz";
            case COMPRESSION_FORMAT_LZ4_FRAMED -> ".lz4";
            case COMPRESSION_FORMAT_ZSTD -> ".zst";
            case COMPRESSION_FORMAT_BROTLI -> ".br";
            default -> "";
        };

        try {
            flowFile = session.write(flowFile, (rawIn, rawOut) -> {
                final OutputStream compressionOut;
                final InputStream compressionIn;

                final OutputStream bufferedOut = new BufferedOutputStream(rawOut, 65536);
                final InputStream bufferedIn = new BufferedInputStream(rawIn, 65536);

                try {
                    if (MODE_COMPRESS.equalsIgnoreCase(compressionMode)) {
                        compressionIn = bufferedIn;

                        switch (compressionFormat.toLowerCase()) {
                            case COMPRESSION_FORMAT_GZIP: {
                                int compressionLevel = context.getProperty(COMPRESSION_LEVEL).asInteger();
                                compressionOut = new GZIPOutputStream(bufferedOut, compressionLevel);
                                mimeTypeRef.set("application/gzip");
                                break;
                            }
                            case COMPRESSION_FORMAT_DEFLATE: {
                                final int compressionLevel = context.getProperty(COMPRESSION_LEVEL).asInteger();
                                compressionOut = new DeflaterOutputStream(bufferedOut, new Deflater(compressionLevel));
                                mimeTypeRef.set("application/gzip");
                                break;
                            }
                            case COMPRESSION_FORMAT_LZMA:
                                compressionOut = new LzmaOutputStream.Builder(bufferedOut).build();
                                mimeTypeRef.set("application/x-lzma");
                                break;
                            case COMPRESSION_FORMAT_XZ_LZMA2:
                                final int xzCompressionLevel = context.getProperty(COMPRESSION_LEVEL).asInteger();
                                compressionOut = new XZOutputStream(bufferedOut, new LZMA2Options(xzCompressionLevel));
                                mimeTypeRef.set("application/x-xz");
                                break;
                            case COMPRESSION_FORMAT_SNAPPY:
                                compressionOut = new SnappyOutputStream(bufferedOut);
                                mimeTypeRef.set("application/x-snappy");
                                break;
                            case COMPRESSION_FORMAT_SNAPPY_HADOOP:
                                compressionOut = new SnappyHadoopCompatibleOutputStream(bufferedOut);
                                mimeTypeRef.set("application/x-snappy-hadoop");
                                break;
                            case COMPRESSION_FORMAT_SNAPPY_FRAMED:
                                compressionOut = new SnappyFramedOutputStream(bufferedOut);
                                mimeTypeRef.set("application/x-snappy-framed");
                                break;
                            case COMPRESSION_FORMAT_LZ4_FRAMED:
                                mimeTypeRef.set("application/x-lz4-framed");
                                compressionOut = new CompressorStreamFactory().createCompressorOutputStream(compressionFormat.toLowerCase(), bufferedOut);
                                break;
                            case COMPRESSION_FORMAT_ZSTD:
                                final int zstdCompressionLevel = context.getProperty(COMPRESSION_LEVEL).asInteger() * 2;
                                compressionOut = new ZstdCompressorOutputStream(bufferedOut, zstdCompressionLevel);
                                mimeTypeRef.set("application/zstd");
                                break;
                            case COMPRESSION_FORMAT_BROTLI: {
                                Brotli4jLoader.ensureAvailability();
                                final int compressionLevel = context.getProperty(COMPRESSION_LEVEL).asInteger();
                                Encoder.Parameters params = new Encoder.Parameters().setQuality(compressionLevel);
                                compressionOut = new BrotliOutputStream(bufferedOut, params);
                                mimeTypeRef.set("application/x-brotli");
                                break;
                            }
                            case COMPRESSION_FORMAT_BZIP2:
                            default:
                                mimeTypeRef.set("application/x-bzip2");
                                compressionOut = new CompressorStreamFactory().createCompressorOutputStream(compressionFormat.toLowerCase(), bufferedOut);
                                break;
                        }
                    } else {
                        compressionOut = bufferedOut;
                        compressionIn = switch (compressionFormat.toLowerCase()) {
                            case COMPRESSION_FORMAT_LZMA -> new LzmaInputStream(bufferedIn, new Decoder());
                            case COMPRESSION_FORMAT_XZ_LZMA2 -> new XZInputStream(bufferedIn);
                            case COMPRESSION_FORMAT_BZIP2 ->
                                // need this two-arg constructor to support concatenated streams
                                new BZip2CompressorInputStream(bufferedIn, true);
                            case COMPRESSION_FORMAT_GZIP -> new GzipCompressorInputStream(bufferedIn, true);
                            case COMPRESSION_FORMAT_DEFLATE -> new InflaterInputStream(bufferedIn);
                            case COMPRESSION_FORMAT_SNAPPY -> new SnappyInputStream(bufferedIn);
                            case COMPRESSION_FORMAT_SNAPPY_HADOOP -> throw new Exception("Cannot decompress snappy-hadoop.");
                            case COMPRESSION_FORMAT_SNAPPY_FRAMED -> new SnappyFramedInputStream(bufferedIn);
                            case COMPRESSION_FORMAT_LZ4_FRAMED -> new FramedLZ4CompressorInputStream(bufferedIn, true);
                            case COMPRESSION_FORMAT_ZSTD -> new ZstdCompressorInputStream(bufferedIn);
                            case COMPRESSION_FORMAT_BROTLI -> {
                                Brotli4jLoader.ensureAvailability();
                                yield new BrotliInputStream(bufferedIn);
                            }
                            default -> new CompressorStreamFactory().createCompressorInputStream(compressionFormat.toLowerCase(), bufferedIn);
                        };
                    }
                } catch (final Exception e) {
                    closeQuietly(bufferedOut);
                    throw new IOException(e);
                }

                try (final InputStream in = compressionIn;
                    final OutputStream out = compressionOut) {
                    final byte[] buffer = new byte[8192];
                    int len;
                    while ((len = in.read(buffer)) > 0) {
                        out.write(buffer, 0, len);
                    }
                    out.flush();
                }
            });
            stopWatch.stop();

            final long sizeAfterCompression = flowFile.getSize();
            if (MODE_DECOMPRESS.equalsIgnoreCase(compressionMode)) {
                flowFile = session.removeAttribute(flowFile, CoreAttributes.MIME_TYPE.key());

                if (context.getProperty(UPDATE_FILENAME).asBoolean()) {
                    final String filename = flowFile.getAttribute(CoreAttributes.FILENAME.key());
                    if (filename.toLowerCase().endsWith(fileExtension)) {
                        flowFile = session.putAttribute(flowFile, CoreAttributes.FILENAME.key(), filename.substring(0, filename.length() - fileExtension.length()));
                    }
                }
            } else {
                flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), mimeTypeRef.get());

                if (context.getProperty(UPDATE_FILENAME).asBoolean()) {
                    final String filename = flowFile.getAttribute(CoreAttributes.FILENAME.key());
                    flowFile = session.putAttribute(flowFile, CoreAttributes.FILENAME.key(), filename + fileExtension);
                }
            }

            logger.info("Successfully {}ed {} using {} compression format; size changed from {} to {} bytes",
                compressionMode.toLowerCase(), flowFile, compressionFormat, sizeBeforeCompression, sizeAfterCompression);
            session.getProvenanceReporter().modifyContent(flowFile, stopWatch.getDuration(TimeUnit.MILLISECONDS));
            session.transfer(flowFile, REL_SUCCESS);
        } catch (final ProcessException e) {
            logger.error("Unable to {} {} using {} compression format due to {}; routing to failure", compressionMode.toLowerCase(), flowFile, compressionFormat, e, e);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private void closeQuietly(final Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (final Exception ignored) {
            }
        }
    }
}
