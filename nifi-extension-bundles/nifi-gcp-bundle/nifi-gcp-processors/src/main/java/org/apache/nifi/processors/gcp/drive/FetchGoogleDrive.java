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
package org.apache.nifi.processors.gcp.drive;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.DriveScopes;
import com.google.api.services.drive.model.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.MultiProcessorUseCase;
import org.apache.nifi.annotation.documentation.ProcessorConfiguration;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.gcp.ProxyAwareTransportFactory;
import org.apache.nifi.processors.gcp.util.GoogleUtils;
import org.apache.nifi.proxy.ProxyConfiguration;

import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.ERROR_CODE;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.ERROR_CODE_DESC;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.ERROR_MESSAGE;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.ERROR_MESSAGE_DESC;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.FILENAME_DESC;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.ID;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.ID_DESC;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.MIME_TYPE_DESC;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.SIZE;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.SIZE_DESC;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.TIMESTAMP;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.TIMESTAMP_DESC;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"google", "drive", "storage", "fetch"})
@CapabilityDescription("Fetches files from a Google Drive Folder. Designed to be used in tandem with ListGoogleDrive. " +
    "Please see Additional Details to set up access to Google Drive.")
@SeeAlso({ListGoogleDrive.class, PutGoogleDrive.class})
@ReadsAttribute(attribute = ID, description = ID_DESC)
@WritesAttributes({
        @WritesAttribute(attribute = ID, description = ID_DESC),
        @WritesAttribute(attribute = "filename", description = FILENAME_DESC),
        @WritesAttribute(attribute = "mime.type", description = MIME_TYPE_DESC),
        @WritesAttribute(attribute = SIZE, description = SIZE_DESC),
        @WritesAttribute(attribute = TIMESTAMP, description = TIMESTAMP_DESC),
        @WritesAttribute(attribute = ERROR_CODE, description = ERROR_CODE_DESC),
        @WritesAttribute(attribute = ERROR_MESSAGE, description = ERROR_MESSAGE_DESC)
})
@MultiProcessorUseCase(
    description = "Retrieve all files in a Google Drive folder",
    keywords = {"google", "drive", "google cloud", "state", "retrieve", "fetch", "all", "stream"},
    configurations = {
        @ProcessorConfiguration(
            processorClass = ListGoogleDrive.class,
            configuration = """
                The "Folder ID" property should be set to the ID of the Google Drive folder that files reside in. \
                    See processor documentation / additional details for more information on how to determine a Google Drive folder's ID.
                    If the flow being built is to be reused elsewhere, it's a good idea to parameterize \
                    this property by setting it to something like `#{GOOGLE_DRIVE_FOLDER_ID}`.

                The "GCP Credentials Provider Service" property should specify an instance of the GCPCredentialsService in order to provide credentials for accessing the folder.

                The 'success' Relationship of this Processor is then connected to FetchGoogleDrive.
                """
        ),
        @ProcessorConfiguration(
            processorClass = FetchGoogleDrive.class,
            configuration = """
                "File ID" = "${drive.id}"

                The "GCP Credentials Provider Service" property should specify an instance of the GCPCredentialsService in order to provide credentials for accessing the bucket.
                """
        )
    }
)
public class FetchGoogleDrive extends AbstractProcessor implements GoogleDriveTrait {

    // Google Docs Export Types
    private static final AllowableValue EXPORT_MS_WORD = new AllowableValue("application/vnd.openxmlformats-officedocument.wordprocessingml.document", "Microsoft Word");
    private static final AllowableValue EXPORT_OPEN_DOCUMENT = new AllowableValue("application/vnd.oasis.opendocument.text", "OpenDocument");
    private static final AllowableValue EXPORT_PDF = new AllowableValue("application/pdf", "PDF");
    private static final AllowableValue EXPORT_RICH_TEXT = new AllowableValue("application/rtf", "Rich Text");
    private static final AllowableValue EXPORT_EPUB = new AllowableValue("application/epub+zip", "EPUB");

    // Shared Export Types
    private static final AllowableValue EXPORT_HTML_DOC = new AllowableValue("application/zip", "Web Page (HTML)");
    private static final AllowableValue EXPORT_PLAIN_TEXT = new AllowableValue("text/plain", "Plain Text");

    // Google Spreadsheet Export Types
    private static final AllowableValue EXPORT_MS_EXCEL = new AllowableValue("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "Microsoft Excel");
    private static final AllowableValue EXPORT_OPEN_SPREADSHEET = new AllowableValue("application/x-vnd.oasis.opendocument.spreadsheet", "OpenDocument Spreadsheet");
    private static final AllowableValue EXPORT_PDF_SPREADSHEET = new AllowableValue("application/pdf", "PDF");
    private static final AllowableValue EXPORT_CSV = new AllowableValue("text/csv", "CSV (first sheet only)",
        "Comma-separated values. Only the first sheet will be exported.");
    private static final AllowableValue EXPORT_TSV = new AllowableValue("text/tab-separated-values", "TSV (first sheet only)",
        "Tab-separate values. Only the first sheet will be exported.");
    private static final AllowableValue EXPORT_HTML_SPREADSHEET = new AllowableValue("text/html", "Web Page (HTML)");

    // Google Presentation Export Types
    private static final AllowableValue EXPORT_MS_POWERPOINT = new AllowableValue("application/vnd.openxmlformats-officedocument.presentationml.presentation", "Microsoft PowerPoint");
    private static final AllowableValue EXPORT_OPEN_PRESENTATION = new AllowableValue("application/vnd.oasis.opendocument.presentation", "OpenDocument Presentation");
    private static final AllowableValue EXPORT_PNG = new AllowableValue("image/png", "PNG (first slide only)");
    private static final AllowableValue EXPORT_JPEG = new AllowableValue("image/jpeg", "JPEG (first slide only)");
    private static final AllowableValue EXPORT_SVG = new AllowableValue("image/svg+xml", "SVG (first slide only)",
        "Scalable Vector Graphics. Only the first slide will be exported.");

    // Drawings Export Types
    private static final AllowableValue EXPORT_PNG_DRAWING = new AllowableValue("image/png", "PNG");
    private static final AllowableValue EXPORT_JPEG_DRAWING = new AllowableValue("image/jpeg", "JPEG");
    private static final AllowableValue EXPORT_SVG_DRAWING = new AllowableValue("image/svg+xml", "SVG");

    private static final Map<String, String> fileExtensions = new HashMap<>();
    static {
        fileExtensions.put(EXPORT_MS_WORD.getValue(), ".docx");
        fileExtensions.put(EXPORT_OPEN_DOCUMENT.getValue(), ".odt");
        fileExtensions.put(EXPORT_PDF.getValue(), ".pdf");
        fileExtensions.put(EXPORT_RICH_TEXT.getValue(), ".rtf");
        fileExtensions.put(EXPORT_EPUB.getValue(), ".epub");
        fileExtensions.put(EXPORT_HTML_DOC.getValue(), ".zip");
        fileExtensions.put(EXPORT_PLAIN_TEXT.getValue(), ".txt");
        fileExtensions.put(EXPORT_MS_EXCEL.getValue(), ".xlsx");
        fileExtensions.put(EXPORT_OPEN_SPREADSHEET.getValue(), ".ods");
        fileExtensions.put(EXPORT_CSV.getValue(), ".csv");
        fileExtensions.put(EXPORT_TSV.getValue(), ".tsv");
        fileExtensions.put(EXPORT_MS_POWERPOINT.getValue(), ".pptx");
        fileExtensions.put(EXPORT_OPEN_PRESENTATION.getValue(), ".odp");
        fileExtensions.put(EXPORT_PNG.getValue(), ".png");
        fileExtensions.put(EXPORT_JPEG.getValue(), ".jpg");
        fileExtensions.put(EXPORT_SVG.getValue(), ".svg");
        fileExtensions.put("application/vnd.google-apps.script+json", ".json");
    }


    public static final PropertyDescriptor FILE_ID = new PropertyDescriptor.Builder()
            .name("drive-file-id")
            .displayName("File ID")
            .description("The Drive ID of the File to fetch. Please see Additional Details for information on how to obtain the Drive ID.")
            .required(true)
            .defaultValue("${drive.id}")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor GOOGLE_DOC_EXPORT_TYPE = new PropertyDescriptor.Builder()
        .name("Google Doc Export Type")
        .description("Google Documents cannot be downloaded directly from Google Drive but instead must be exported to a specified MIME Type. In the event " +
            "that the incoming FlowFile's MIME Type indicates that the file is a Google Document, this property specifies the MIME Type to export the document to.")
        .required(true)
        .allowableValues(
            EXPORT_PDF, EXPORT_PLAIN_TEXT, EXPORT_MS_WORD,
            EXPORT_OPEN_DOCUMENT, EXPORT_RICH_TEXT, EXPORT_HTML_DOC, EXPORT_EPUB)
        .defaultValue(EXPORT_PDF.getValue())
        .build();

    public static final PropertyDescriptor GOOGLE_SPREADSHEET_EXPORT_TYPE = new PropertyDescriptor.Builder()
        .name("Google Spreadsheet Export Type")
        .description("Google Spreadsheets cannot be downloaded directly from Google Drive but instead must be exported to a specified MIME Type. In the event " +
            "that the incoming FlowFile's MIME Type indicates that the file is a Google Spreadsheet, this property specifies the MIME Type to export the spreadsheet to.")
        .required(true)
        .allowableValues(
            EXPORT_CSV, EXPORT_MS_EXCEL, EXPORT_PDF_SPREADSHEET,
            EXPORT_TSV, EXPORT_HTML_SPREADSHEET, EXPORT_OPEN_SPREADSHEET)
        .defaultValue(EXPORT_CSV.getValue())
        .build();

    public static final PropertyDescriptor GOOGLE_PRESENTATION_EXPORT_TYPE = new PropertyDescriptor.Builder()
        .name("Google Presentation Export Type")
        .description("Google Presentations cannot be downloaded directly from Google Drive but instead must be exported to a specified MIME Type. In the event " +
            "that the incoming FlowFile's MIME Type indicates that the file is a Google Presentation, this property specifies the MIME Type to export the presentation to.")
        .required(true)
        .allowableValues(
            EXPORT_PDF, EXPORT_MS_POWERPOINT, EXPORT_PLAIN_TEXT, EXPORT_OPEN_PRESENTATION,
            EXPORT_PNG, EXPORT_JPEG, EXPORT_SVG)
        .defaultValue(EXPORT_PDF.getValue())
        .build();

    public static final PropertyDescriptor GOOGLE_DRAWING_EXPORT_TYPE = new PropertyDescriptor.Builder()
        .name("Google Drawing Export Type")
        .description("Google Drawings cannot be downloaded directly from Google Drive but instead must be exported to a specified MIME Type. In the event " +
            "that the incoming FlowFile's MIME Type indicates that the file is a Google Drawing, this property specifies the MIME Type to export the drawing to.")
        .required(true)
        .allowableValues(
            EXPORT_PDF, EXPORT_PNG_DRAWING, EXPORT_JPEG_DRAWING, EXPORT_SVG_DRAWING)
        .defaultValue(EXPORT_PDF.getValue())
        .build();



    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile will be routed here for each successfully fetched File.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile will be routed here for each File for which fetch was attempted but failed.")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
        GoogleUtils.GCP_CREDENTIALS_PROVIDER_SERVICE,
        FILE_ID,
        ProxyConfiguration.createProxyConfigPropertyDescriptor(ProxyAwareTransportFactory.PROXY_SPECS),
        GOOGLE_DOC_EXPORT_TYPE,
        GOOGLE_SPREADSHEET_EXPORT_TYPE,
        GOOGLE_PRESENTATION_EXPORT_TYPE,
        GOOGLE_DRAWING_EXPORT_TYPE
    );

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE
    );

    private volatile Drive driveService;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws IOException {
        final ProxyConfiguration proxyConfiguration = ProxyConfiguration.getConfiguration(context);

        driveService = createDriveService(
                context,
                new ProxyAwareTransportFactory(proxyConfiguration).create(),
                DriveScopes.DRIVE, DriveScopes.DRIVE_FILE
        );
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String fileId = context.getProperty(FILE_ID).evaluateAttributeExpressions(flowFile).getValue();

        final long startNanos = System.nanoTime();
        try {
            final File fileMetadata = fetchFileMetadata(fileId);
            final Map<String, String> attributeMap = createAttributeMap(fileMetadata);

            flowFile = fetchFile(fileId, session, context, flowFile, attributeMap);

            flowFile = session.putAllAttributes(flowFile, attributeMap);

            final String url = DRIVE_URL + fileMetadata.getId();
            final long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            session.getProvenanceReporter().fetch(flowFile, url, transferMillis);
            session.transfer(flowFile, REL_SUCCESS);
        } catch (GoogleJsonResponseException e) {
            handleErrorResponse(session, fileId, flowFile, e);
        } catch (Exception e) {
            handleUnexpectedError(session, flowFile, fileId, e);
        }
    }

    private String getExportType(final String mimeType, final ProcessContext context) {
        if (mimeType == null) {
            return null;
        }

        return switch (mimeType) {
            case "application/vnd.google-apps.document" -> context.getProperty(GOOGLE_DOC_EXPORT_TYPE).getValue();
            case "application/vnd.google-apps.spreadsheet" -> context.getProperty(GOOGLE_SPREADSHEET_EXPORT_TYPE).getValue();
            case "application/vnd.google-apps.presentation" -> context.getProperty(GOOGLE_PRESENTATION_EXPORT_TYPE).getValue();
            case "application/vnd.google-apps.drawing" -> context.getProperty(GOOGLE_DRAWING_EXPORT_TYPE).getValue();
            case "application/vnd.google-apps.script" -> "application/vnd.google-apps.script+json";
            default -> null;
        };
    }

    private FlowFile fetchFile(final String fileId, final ProcessSession session, final ProcessContext context, final FlowFile flowFile, final Map<String, String> attributeMap) throws IOException {
        final String mimeType = flowFile.getAttribute(CoreAttributes.MIME_TYPE.key());
        final String exportType = getExportType(mimeType, context);

        if (exportType == null) {
            return downloadFile(fileId, session, flowFile);
        }

        return exportFile(fileId, exportType, session, flowFile, attributeMap);
    }

    private FlowFile downloadFile(final String fileId, final ProcessSession session, final FlowFile flowFile) throws IOException {
        try (final InputStream driveFileInputStream = driveService
            .files()
            .get(fileId)
            .setSupportsAllDrives(true)
            .executeMediaAsInputStream()) {

            return session.importFrom(driveFileInputStream, flowFile);
        }
    }

    private FlowFile exportFile(final String fileId, final String exportMimeType, final ProcessSession session, final FlowFile flowFile, final Map<String, String> attributeMap) throws IOException {
        attributeMap.put(CoreAttributes.MIME_TYPE.key(), exportMimeType);

        final String fileExtension = fileExtensions.get(exportMimeType);
        if (fileExtension != null) {
            attributeMap.put(CoreAttributes.FILENAME.key(), flowFile.getAttribute(CoreAttributes.FILENAME.key()) + fileExtension);
        }

        try (final InputStream driveFileInputStream = driveService
            .files()
            .export(fileId, exportMimeType)
            .executeMediaAsInputStream()) {

            return session.importFrom(driveFileInputStream, flowFile);
        }
    }


    private File fetchFileMetadata(final String fileId) throws IOException {
        return driveService
                .files()
                .get(fileId)
                .setSupportsAllDrives(true)
                .setFields("id, name, createdTime, mimeType, size")
                .execute();
    }

    private void handleErrorResponse(final ProcessSession session, final String fileId, FlowFile flowFile, final GoogleJsonResponseException e) {
        getLogger().error("Fetching File [{}] failed", fileId, e);

        flowFile = session.putAttribute(flowFile, ERROR_CODE, String.valueOf(e.getStatusCode()));
        flowFile = session.putAttribute(flowFile, ERROR_MESSAGE, e.getMessage());

        flowFile = session.penalize(flowFile);
        session.transfer(flowFile, REL_FAILURE);
    }

    private void handleUnexpectedError(final ProcessSession session, FlowFile flowFile, final String fileId, final Exception e) {
        getLogger().error("Fetching File [{}] failed", fileId, e);

        flowFile = session.putAttribute(flowFile, ERROR_MESSAGE, e.getMessage());

        flowFile = session.penalize(flowFile);
        session.transfer(flowFile, REL_FAILURE);
    }
}
