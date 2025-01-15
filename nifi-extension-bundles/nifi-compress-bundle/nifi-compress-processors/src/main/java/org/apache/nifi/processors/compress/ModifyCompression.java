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
package org.apache.nifi.processors.compress;

import com.aayushatharva.brotli4j.Brotli4jLoader;
import com.aayushatharva.brotli4j.decoder.BrotliInputStream;
import com.aayushatharva.brotli4j.encoder.BrotliOutputStream;
import com.aayushatharva.brotli4j.encoder.Encoder;
import lzma.sdk.lzma.Decoder;
import lzma.streams.LzmaInputStream;
import lzma.streams.LzmaOutputStream;
import org.apache.commons.compress.compressors.CompressorException;
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
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processors.compress.property.CompressionStrategy;
import org.apache.nifi.processors.compress.property.FilenameStrategy;
import org.apache.nifi.stream.io.GZIPOutputStream;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.util.StringUtils;
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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
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
@Tags({"content", "compress", "recompress", "gzip", "bzip2", "lzma", "xz-lzma2", "snappy", "snappy-hadoop", "snappy framed", "lz4-framed", "deflate", "zstd", "brotli"})
@CapabilityDescription("Changes the compression algorithm used to compress the contents of a FlowFile by decompressing the contents of FlowFiles using a user-specified compression algorithm and " +
    "recompressing the contents using the specified compression format properties. This processor operates in a very memory efficient way so very large objects well beyond " +
    "the heap size are generally fine to process")
@ReadsAttribute(attribute = "mime.type", description = "If the Decompression Format is set to 'use mime.type attribute', this attribute is used to "
        + "determine the decompression type. Otherwise, this attribute is ignored.")
@WritesAttribute(attribute = "mime.type", description = "The appropriate MIME Type is set based on the value of the Compression Format property. If the Compression Format is 'no compression' this "
        + "attribute is removed as the MIME Type is no longer known.")
@SystemResourceConsideration(resource = SystemResource.CPU)
@SystemResourceConsideration(resource = SystemResource.MEMORY)
public class ModifyCompression extends AbstractProcessor {

    public static final PropertyDescriptor INPUT_COMPRESSION_STRATEGY = new PropertyDescriptor.Builder()
            .name("Input Compression Strategy")
            .displayName("Input Compression Strategy")
            .description("The strategy to use for decompressing input FlowFiles")
            .allowableValues(EnumSet.complementOf(EnumSet.of(CompressionStrategy.SNAPPY_HADOOP)))
            .defaultValue(CompressionStrategy.NONE)
            .required(true)
            .build();

    public static final PropertyDescriptor OUTPUT_COMPRESSION_STRATEGY = new PropertyDescriptor.Builder()
            .name("Output Compression Strategy")
            .name("Output Compression Strategy")
            .description("The strategy to use for compressing output FlowFiles")
            .allowableValues(EnumSet.complementOf(EnumSet.of(CompressionStrategy.MIME_TYPE_ATTRIBUTE)))
            .defaultValue(CompressionStrategy.NONE)
            .required(true)
            .build();

    public static final PropertyDescriptor OUTPUT_COMPRESSION_LEVEL = new PropertyDescriptor.Builder()
            .name("Output Compression Level")
            .displayName("Output Compression Level")
            .description("The compression level for output FlowFiles for supported formats. A lower value results in faster processing "
                    + "but less compression; a value of 0 indicates no (that is, simple archiving) for gzip or minimal for xz-lzma2 compression."
                    + " Higher levels can mean much larger memory usage such as the case with levels 7-9 for xz-lzma/2 so be careful relative to heap size.")
            .defaultValue("1")
            .required(true)
            .allowableValues("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
            .dependsOn(OUTPUT_COMPRESSION_STRATEGY,
                    CompressionStrategy.MIME_TYPE_ATTRIBUTE,
                    CompressionStrategy.GZIP,
                    CompressionStrategy.DEFLATE,
                    CompressionStrategy.XZ_LZMA2,
                    CompressionStrategy.ZSTD,
                    CompressionStrategy.BROTLI)
            .build();

    public static final PropertyDescriptor OUTPUT_FILENAME_STRATEGY = new PropertyDescriptor.Builder()
            .name("Output Filename Strategy")
            .displayName("Output Filename Strategy")
            .description("Processing strategy for filename attribute on output FlowFiles")
            .required(true)
            .allowableValues(FilenameStrategy.class)
            .defaultValue(FilenameStrategy.UPDATED)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles will be transferred to the success relationship on compression modification success")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles will be transferred to the failure relationship on compression modification errors")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            INPUT_COMPRESSION_STRATEGY,
            OUTPUT_COMPRESSION_STRATEGY,
            OUTPUT_COMPRESSION_LEVEL,
            OUTPUT_FILENAME_STRATEGY
    );

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE
    );

    private static final Map<String, CompressionStrategy> compressionFormatMimeTypeMap;

    private final static int STREAM_BUFFER_SIZE = 65536;

    static {
        final Map<String, CompressionStrategy> mimeTypeMap = new HashMap<>();
        for (final CompressionStrategy compressionStrategy : CompressionStrategy.values()) {
            String[] mimeTypes = compressionStrategy.getMimeTypes();
            if (mimeTypes == null) {
                continue;
            }
            for (final String mimeType : mimeTypes) {
                mimeTypeMap.put(mimeType, compressionStrategy);
            }
        }

        compressionFormatMimeTypeMap = Collections.unmodifiableMap(mimeTypeMap);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final CompressionStrategy inputCompressionStrategy;
        final CompressionStrategy configuredInputCompressionStrategy = context.getProperty(INPUT_COMPRESSION_STRATEGY).asAllowableValue(CompressionStrategy.class);
        if (CompressionStrategy.MIME_TYPE_ATTRIBUTE == configuredInputCompressionStrategy) {
            final String mimeType = flowFile.getAttribute(CoreAttributes.MIME_TYPE.key());
            if (mimeType == null) {
                getLogger().error("Required FlowFile Attribute [{}] not found {}", CoreAttributes.MIME_TYPE.key(), flowFile);
                session.transfer(flowFile, REL_FAILURE);
                return;
            }

            inputCompressionStrategy = compressionFormatMimeTypeMap.get(mimeType);
            if (inputCompressionStrategy == null) {
                getLogger().info("Compression Strategy not found for MIME Type [{}] {}", mimeType, flowFile);
                session.transfer(flowFile, REL_FAILURE);
                return;
            }
        } else {
            inputCompressionStrategy = configuredInputCompressionStrategy;
        }

        final CompressionStrategy outputCompressionStrategy = context.getProperty(OUTPUT_COMPRESSION_STRATEGY).asAllowableValue(CompressionStrategy.class);
        final AtomicReference<String> mimeTypeRef = new AtomicReference<>(null);
        final StopWatch stopWatch = new StopWatch(true);
        final long inputFileSize = flowFile.getSize();
        final int outputCompressionLevel = context.getProperty(OUTPUT_COMPRESSION_LEVEL).asInteger();
        try {
            flowFile = session.write(flowFile, (flowFileInputStream, flowFileOutputStream) -> {
                try (
                        final BufferedInputStream bufferedInputStream = new BufferedInputStream(flowFileInputStream, STREAM_BUFFER_SIZE);
                        final InputStream inputStream = getCompressionInputStream(inputCompressionStrategy, bufferedInputStream);
                        final BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(flowFileOutputStream, STREAM_BUFFER_SIZE);
                        final OutputStream outputStream = getCompressionOutputStream(outputCompressionStrategy, outputCompressionLevel, mimeTypeRef, bufferedOutputStream)
                        ) {
                    StreamUtils.copy(inputStream, outputStream);
                }
            });
            stopWatch.stop();

            final String outputMimeType = mimeTypeRef.get();
            if (StringUtils.isEmpty(outputMimeType)) {
                flowFile = session.removeAttribute(flowFile, CoreAttributes.MIME_TYPE.key());
            } else {
                flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), outputMimeType);
            }

            final FilenameStrategy filenameStrategy = FilenameStrategy.valueOf(context.getProperty(OUTPUT_FILENAME_STRATEGY).getValue());
            if (FilenameStrategy.UPDATED == filenameStrategy) {
                final String updatedFilename = getUpdatedFilename(flowFile, inputCompressionStrategy, outputCompressionStrategy);
                flowFile = session.putAttribute(flowFile, CoreAttributes.FILENAME.key(), updatedFilename);
            }

            getLogger().info("Input Compression [{}] Size [{}] Output Compression [{}] Size [{}] Completed {}",
                    inputCompressionStrategy, inputFileSize, outputCompressionStrategy, flowFile.getSize(), flowFile);
            session.getProvenanceReporter().modifyContent(flowFile, stopWatch.getDuration(TimeUnit.MILLISECONDS));
            session.transfer(flowFile, REL_SUCCESS);
        } catch (final RuntimeException e) {
            getLogger().error("Input Compression [{}] Size [{}] Output Compression [{}] Failed {}",
                    inputCompressionStrategy, inputFileSize, outputCompressionStrategy, flowFile, e);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private InputStream getCompressionInputStream(final CompressionStrategy compressionFormat, final InputStream parentInputStream) throws IOException {
        return switch (compressionFormat) {
            case LZMA -> new LzmaInputStream(parentInputStream, new Decoder());
            case XZ_LZMA2 -> new XZInputStream(parentInputStream);
            case BZIP2 -> {
                // need this two-arg constructor to support concatenated streams
                yield new BZip2CompressorInputStream(parentInputStream, true);
            }
            case GZIP -> new GzipCompressorInputStream(parentInputStream, true);
            case DEFLATE -> new InflaterInputStream(parentInputStream);
            case SNAPPY -> new SnappyInputStream(parentInputStream);
            case SNAPPY_HADOOP -> throw new IOException("Cannot decompress snappy-hadoop");
            case SNAPPY_FRAMED -> new SnappyFramedInputStream(parentInputStream);
            case LZ4_FRAMED -> new FramedLZ4CompressorInputStream(parentInputStream, true);
            case ZSTD -> new ZstdCompressorInputStream(parentInputStream);
            case BROTLI -> {
                Brotli4jLoader.ensureAvailability();
                yield new BrotliInputStream(parentInputStream);
            }
            case NONE -> parentInputStream;
            default -> {
                final String compressorStreamFormat = compressionFormat.getValue().toLowerCase();
                try {
                    yield new CompressorStreamFactory().createCompressorInputStream(compressorStreamFormat, parentInputStream);
                } catch (final CompressorException e) {
                    throw new IOException(String.format("Compressor Stream Format [%s] creation failed", compressorStreamFormat), e);
                }
            }
        };
    }

    private OutputStream getCompressionOutputStream(
            final CompressionStrategy compressionFormat,
            final int compressionLevel,
            final AtomicReference<String> mimeTypeRef,
            final OutputStream parentOutputStream
    ) throws IOException {
        final OutputStream compressionOut;
        switch (compressionFormat) {
            case GZIP -> {
                compressionOut = new GZIPOutputStream(parentOutputStream, compressionLevel);
                mimeTypeRef.set(CompressionStrategy.GZIP.getMimeTypes()[0]);
            }
            case DEFLATE -> {
                compressionOut = new DeflaterOutputStream(parentOutputStream, new Deflater(compressionLevel));
                mimeTypeRef.set(CompressionStrategy.GZIP.getMimeTypes()[0]);
            }
            case LZMA -> {
                compressionOut = new LzmaOutputStream.Builder(parentOutputStream).build();
                mimeTypeRef.set(CompressionStrategy.LZMA.getMimeTypes()[0]);
            }
            case XZ_LZMA2 -> {
                compressionOut = new XZOutputStream(parentOutputStream, new LZMA2Options(compressionLevel));
                mimeTypeRef.set(CompressionStrategy.XZ_LZMA2.getMimeTypes()[0]);
            }
            case SNAPPY -> {
                compressionOut = new SnappyOutputStream(parentOutputStream);
                mimeTypeRef.set(CompressionStrategy.SNAPPY.getMimeTypes()[0]);
            }
            case SNAPPY_HADOOP -> {
                compressionOut = new SnappyHadoopCompatibleOutputStream(parentOutputStream);
                mimeTypeRef.set(CompressionStrategy.SNAPPY_HADOOP.getMimeTypes()[0]);
            }
            case SNAPPY_FRAMED -> {
                compressionOut = new SnappyFramedOutputStream(parentOutputStream);
                mimeTypeRef.set(CompressionStrategy.SNAPPY_FRAMED.getMimeTypes()[0]);
            }
            case LZ4_FRAMED -> {
                final String compressorStreamFormat = compressionFormat.getValue().toLowerCase();
                try {
                    compressionOut = new CompressorStreamFactory().createCompressorOutputStream(compressorStreamFormat, parentOutputStream);
                } catch (final CompressorException e) {
                    throw new IOException(String.format("Compressor Stream Format [%s] creation failed", compressorStreamFormat), e);
                }
                mimeTypeRef.set(CompressionStrategy.LZ4_FRAMED.getMimeTypes()[0]);
            }
            case ZSTD -> {
                final int outputCompressionLevel = compressionLevel * 2;
                compressionOut = new ZstdCompressorOutputStream(parentOutputStream, outputCompressionLevel);
                mimeTypeRef.set(CompressionStrategy.ZSTD.getMimeTypes()[0]);
            }
            case BROTLI -> {
                Brotli4jLoader.ensureAvailability();
                Encoder.Parameters params = new Encoder.Parameters().setQuality(compressionLevel);
                compressionOut = new BrotliOutputStream(parentOutputStream, params);
                mimeTypeRef.set(CompressionStrategy.BROTLI.getMimeTypes()[0]);
            }
            case BZIP2 -> {
                final String compressorStreamFormat = compressionFormat.getValue().toLowerCase();
                try {
                    compressionOut = new CompressorStreamFactory().createCompressorOutputStream(compressorStreamFormat, parentOutputStream);
                } catch (final CompressorException e) {
                    throw new IOException(String.format("Compressor Stream Format [%s] creation failed", compressorStreamFormat), e);
                }
                mimeTypeRef.set(CompressionStrategy.BZIP2.getMimeTypes()[0]);
            }
            case null, default -> compressionOut = parentOutputStream;
        }
        return compressionOut;
    }

    private String getUpdatedFilename(final FlowFile flowFile, final CompressionStrategy inputCompressionStrategy, final CompressionStrategy outputCompressionStrategy) {
        final String inputFilename = flowFile.getAttribute(CoreAttributes.FILENAME.key());
        // Remove the input file extension if necessary
        final String inputFileExtension = inputCompressionStrategy.getFileExtension();
        final String truncatedFilename;
        if (inputFilename.toLowerCase().endsWith(inputFileExtension)) {
            truncatedFilename = inputFilename.substring(0, inputFilename.length() - inputFileExtension.length());
        } else {
            truncatedFilename = inputFilename;
        }
        return truncatedFilename + outputCompressionStrategy.getFileExtension();
    }
}
