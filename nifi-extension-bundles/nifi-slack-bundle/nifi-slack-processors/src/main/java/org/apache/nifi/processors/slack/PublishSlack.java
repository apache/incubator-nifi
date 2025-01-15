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

package org.apache.nifi.processors.slack;

import com.slack.api.Slack;
import com.slack.api.SlackConfig;
import com.slack.api.bolt.App;
import com.slack.api.bolt.AppConfig;
import com.slack.api.methods.MethodsClient;
import com.slack.api.methods.request.chat.ChatPostMessageRequest;
import com.slack.api.methods.request.files.FilesUploadV2Request;
import com.slack.api.methods.response.chat.ChatPostMessageResponse;
import com.slack.api.methods.response.files.FilesUploadV2Response;
import com.slack.api.model.File;
import com.slack.api.model.File.ShareDetail;
import com.slack.api.model.File.Shares;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.configuration.DefaultSettings;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.MultiProcessorUseCase;
import org.apache.nifi.annotation.documentation.ProcessorConfiguration;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.documentation.UseCase;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.slack.util.SlackResponseUtil;
import org.apache.nifi.processors.slack.util.ChannelMapper;
import org.apache.nifi.processors.slack.util.RateLimit;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.FormatUtils;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("""
    Posts a message to the specified Slack channel. The content of the message can be either a user-defined message that makes use of Expression Language or
    the contents of the FlowFile can be sent as the message. If sending a user-defined message, the contents of the FlowFile may also be optionally uploaded as
    a file attachment.
    """)
@SeeAlso({ListenSlack.class, ConsumeSlack.class})
@Tags({"slack", "conversation", "chat.postMessage", "social media", "team", "text", "unstructured", "write", "upload", "send", "publish"})
@WritesAttributes({
    @WritesAttribute(attribute = "slack.channel.id", description = "The ID of the Slack Channel from which the messages were retrieved"),
    @WritesAttribute(attribute = "slack.ts", description = "The timestamp of the slack messages that was sent; this is used by Slack as a unique identifier")
})
@DefaultSettings(yieldDuration = "3 sec")
@UseCase(
    description = "Send specific text as a message to Slack, optionally including the FlowFile's contents as an attached file.",
    configuration = """
            Set "Access Token" to the value of your Slack OAuth Access Token.
            Set "Channel" to the ID of the channel or the name of the channel prefixed with the # symbol. For example, "C0123456789" or "#general".
            Set "Publish Strategy" to "Use 'Message Text' Property".
            Set "Message Text" to the text that you would like to send as the Slack message.
            Set "Include FlowFile Content as Attachment" to "true" if the FlowFile's contents should be attached as a file, or "false" to send just the message text without an attachment.
            """
)
@UseCase(
    description = "Send the contents of the FlowFile as a message to Slack.",
    configuration = """
            Set "Access Token" to the value of your Slack OAuth Access Token.
            Set "Channel" to the ID of the channel or the name of the channel prefixed with the # symbol. For example, "C0123456789" or "#general".
            Set "Publish Strategy" to "Send FlowFile Content as Message".
            """
)
@MultiProcessorUseCase(
    description = "Respond to a Slack message in a thread.",
    keywords = {"slack", "respond", "reply", "thread"},
    configurations = {
        @ProcessorConfiguration(
            processorClassName = "org.apache.nifi.processors.standard.EvaluateJsonPath",
            configuration = """
                Set "Destination" to "flowfile-attribute"

                Add a new property named "thread.ts" with a value of `$.threadTs`
                Add a new property named "message.ts" with a value of `$.ts`
                Add a new property named "channel.id" with a value of `$.channel`
                Add a new property named "user.id" with a value of `$.user`

                Connect the "matched" Relationship to PublishSlack.
                """
        ),
        @ProcessorConfiguration(
            processorClass = PublishSlack.class,
            configuration = """
                Set "Access Token" to the value of your Slack OAuth Access Token.
                Set "Channel" to `${'channel.id'}`
                Set "Publish Strategy" to "Use 'Message Text' Property".
                Set "Message Text" to the text that you would like to send as the response. If desired, you can reference the user of the original message by including the text `<@${'user.id'}>`.
                    For example: `Hey, <@${'user.id'}>, thanks for asking...`
                Set "Include FlowFile Content as Attachment" to "false".
                Set "Thread Timestamp" to `${'thread.ts':replaceEmpty( ${'message.ts'} )}`
                """
        )
    }
)
public class PublishSlack extends AbstractProcessor {

    static final AllowableValue PUBLISH_STRATEGY_CONTENT_AS_MESSAGE = new AllowableValue("Send FlowFile Content as Message", "Send FlowFile Content as Message",
        "The contents of the FlowFile will be sent as the message text.");
    static final AllowableValue PUBLISH_STRATEGY_USE_PROPERTY = new AllowableValue("Use 'Message Text' Property", "Use 'Message Text' Property",
        "The value of the Message Text Property will be sent as the message text.");

    static final PropertyDescriptor ACCESS_TOKEN = new PropertyDescriptor.Builder()
        .name("Access Token")
        .description("OAuth Access Token used for authenticating/authorizing the Slack request sent by NiFi. This may be either a User Token or a Bot Token. " +
                     "The token must be granted the chat:write scope. Additionally, in order to upload FlowFile contents as an attachment, it must be granted files:write.")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .required(true)
        .sensitive(true)
        .build();

    static PropertyDescriptor CHANNEL = new PropertyDescriptor.Builder()
        .name("Channel")
        .description("The name or identifier of the channel to send the message to. If using a channel name, it must be prefixed with the # character. " +
                     "For example, #general. This is valid only for public channels. Otherwise, the unique identifier of the channel to publish to must be " +
                     "provided.")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .required(true)
        .build();

    static PropertyDescriptor PUBLISH_STRATEGY = new PropertyDescriptor.Builder()
        .name("Publish Strategy")
        .description("Specifies how the Processor will send the message or file to Slack.")
        .required(true)
        .allowableValues(PUBLISH_STRATEGY_CONTENT_AS_MESSAGE, PUBLISH_STRATEGY_USE_PROPERTY)
        .defaultValue(PUBLISH_STRATEGY_CONTENT_AS_MESSAGE.getValue())
        .build();

    static PropertyDescriptor CHARACTER_SET = new PropertyDescriptor.Builder()
        .name("Character Set")
        .description("Specifies the name of the Character Set used to encode the FlowFile contents.")
        .required(true)
        .defaultValue("UTF-8")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .dependsOn(PUBLISH_STRATEGY, PUBLISH_STRATEGY_CONTENT_AS_MESSAGE)
        .build();

    static PropertyDescriptor MESSAGE_TEXT = new PropertyDescriptor.Builder()
        .name("Message Text")
        .description("The text of the message to send to Slack.")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .required(true)
        .addValidator(Validator.VALID)
        .dependsOn(PUBLISH_STRATEGY, PUBLISH_STRATEGY_USE_PROPERTY)
        .build();

    static PropertyDescriptor SEND_CONTENT_AS_ATTACHMENT = new PropertyDescriptor.Builder()
        .name("Include FlowFile Content as Attachment")
        .description("Specifies whether or not the contents of the FlowFile should be uploaded as an attachment to the Slack message.")
        .allowableValues("true", "false")
        .required(true)
        .dependsOn(PUBLISH_STRATEGY, PUBLISH_STRATEGY_USE_PROPERTY)
        .defaultValue("false")
        .build();

    static PropertyDescriptor MAX_FILE_SIZE = new PropertyDescriptor.Builder()
        .name("Max FlowFile Size")
        .description("The maximum size of a FlowFile that can be sent to Slack. If any FlowFile exceeds this size, it will be routed to failure. " +
                     "This plays an important role because the entire contents of the file must be loaded into NiFi's heap in order to send the data " +
                     "to Slack.")
        .required(true)
        .dependsOn(SEND_CONTENT_AS_ATTACHMENT, "true")
        .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
        .defaultValue("1 MB")
        .build();

    static PropertyDescriptor THREAD_TS = new PropertyDescriptor.Builder()
        .name("Thread Timestamp")
        .description("The Timestamp identifier for the thread that this message is to be a part of. If not specified, the message will be a top-level message instead of " +
                     "being in a thread.")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .required(false)
        .build();

    static PropertyDescriptor METHODS_ENDPOINT_URL_PREFIX = new PropertyDescriptor.Builder()
        .name("Methods Endpoint Url Prefix")
        .description("Customization of the Slack Client. Set the methodsEndpointUrlPrefix. If you need to set a different URL prefix for Slack API Methods calls, " +
                     "you can set the one. Default value: https://slack.com/api/")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .required(false)
        .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(ACCESS_TOKEN,
        CHANNEL,
        PUBLISH_STRATEGY,
        MESSAGE_TEXT,
        CHARACTER_SET,
        SEND_CONTENT_AS_ATTACHMENT,
        MAX_FILE_SIZE,
        THREAD_TS,
        METHODS_ENDPOINT_URL_PREFIX
    );


    public static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("FlowFiles are routed to success after being successfully sent to Slack")
        .build();

    public static final Relationship REL_RATE_LIMITED = new Relationship.Builder()
        .name("rate limited")
        .description("FlowFiles are routed to 'rate limited' if the Rate Limit has been exceeded")
        .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("FlowFiles are routed to 'failure' if unable to be sent to Slack for any other reason")
        .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
        REL_SUCCESS,
        REL_RATE_LIMITED,
        REL_FAILURE);

    private RateLimit rateLimit;

    private volatile ChannelMapper channelMapper;
    private volatile App slackApp;
    private volatile MethodsClient client;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @OnScheduled
    public void setup(final ProcessContext context) {
        rateLimit = new RateLimit(getLogger());
        slackApp = createSlackApp(context);
        client = slackApp.client();

        channelMapper = new ChannelMapper(client);
    }

    @OnStopped
    public void shutdown() {
        channelMapper = null;
        client = null;
        if (slackApp != null) {
            slackApp.stop();
            slackApp = null;
        }
        rateLimit = null;
    }

    private App createSlackApp(final ProcessContext context) {
        final String botToken = context.getProperty(ACCESS_TOKEN).getValue();
        final String methodsEndpointUrlPrefix = context.getProperty(METHODS_ENDPOINT_URL_PREFIX).getValue();

        final SlackConfig slackConfig = new SlackConfig();
        if (context.getProperty(METHODS_ENDPOINT_URL_PREFIX).isSet()) {
            slackConfig.setMethodsEndpointUrlPrefix(methodsEndpointUrlPrefix);
        }
        final AppConfig appConfig = AppConfig.builder()
            .slack(Slack.getInstance(slackConfig))
            .singleTeamBotToken(botToken)
            .build();

        return new App(appConfig);
    }


    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        if (rateLimit.isLimitReached()) {
            getLogger().debug("Will not publish to Slack because rate limit has been reached");
            context.yield();
            return;
        }

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String channelId = getChannelId(flowFile, session, context);
        if (channelId == null) {
            // error will have already been logged
            return;
        }

        // Get the message text
        final String publishStrategy = context.getProperty(PUBLISH_STRATEGY).getValue();
        if (PUBLISH_STRATEGY_CONTENT_AS_MESSAGE.getValue().equalsIgnoreCase(publishStrategy)) {
            publishContentAsMessage(flowFile, channelId, context, session);
        } else if (context.getProperty(SEND_CONTENT_AS_ATTACHMENT).asBoolean()) {
            publishAsFile(flowFile, channelId, context, session);
        } else {
            final String messageText = context.getProperty(MESSAGE_TEXT).evaluateAttributeExpressions(flowFile).getValue();
            publishAsMessage(flowFile, channelId, messageText, context, session);
        }
    }

    private String getChannelId(final FlowFile flowFile, final ProcessSession session, final ProcessContext context) {
        final String channelNameOrId = context.getProperty(CHANNEL).evaluateAttributeExpressions(flowFile).getValue();
        if (channelNameOrId.isEmpty()) {
            getLogger().error("No Channel ID was given for {}; routing to failure", flowFile);
            session.transfer(flowFile, REL_FAILURE);
            return null;
        }

        if (!channelNameOrId.startsWith("#")) {
            return channelNameOrId;
        }

        // Resolve Channel name to an ID
        try {
            final String channelId = channelMapper.lookupChannelId(channelNameOrId);
            if (channelId == null) {
                getLogger().error("Could not find Channel with name {} for {}; routing to failure", channelNameOrId, flowFile);
                session.transfer(flowFile, REL_FAILURE);
                return null;
            }

            return channelId;
        } catch (final Exception e) {
            final Relationship relationship = handleClientException(channelNameOrId, flowFile, session, context, e);
            getLogger().error("Failed to resolve Slack Channel ID for {}; transferring to {}", flowFile, relationship, e);
            return null;
        }
    }

    private void publishContentAsMessage(FlowFile flowFile, final String channelId, final ProcessContext context, final ProcessSession session) {
        // Slack limits the message size to 100,000 characters. We don't have a way to know based on the size of the FlowFile how many characters it will contain,
        // but we can be rather certain that if the size exceeds 500,000 bytes, it will also exceed 100,000 characters. As a result, we pre-emptively route to
        // 'too large' in order to avoid buffering the contents into memory.
        if (flowFile.getSize() > 500_000) {
            getLogger().error("Cannot send contents of FlowFile {} to Slack because its length exceeds 500,000 bytes; routing to 'failure'", flowFile);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        final String charsetName = context.getProperty(CHARACTER_SET).evaluateAttributeExpressions(flowFile).getValue();

        final byte[] buffer = new byte[(int) flowFile.getSize()];
        final String messageText;
        try (final InputStream in = session.read(flowFile)) {
            StreamUtils.fillBuffer(in, buffer, true);
            messageText = new String(buffer, charsetName);
        } catch (final IOException ioe) {
            getLogger().error("Failed to send contents of FlowFile {} to Slack; routing to failure", ioe);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        if (messageText.length() > 100_000) {
            getLogger().error("Cannot send contents of FlowFile {} to Slack because its length exceeds 100,000 characters; routing to 'failure'");
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        publishAsMessage(flowFile, channelId, messageText, context, session);
    }

    private void publishAsMessage(FlowFile flowFile, final String channelId, final String messageText, final ProcessContext context, final ProcessSession session) {
        final String threadTs = context.getProperty(THREAD_TS).evaluateAttributeExpressions(flowFile).getValue();

        final ChatPostMessageRequest request = ChatPostMessageRequest.builder()
            .channel(channelId)
            .text(messageText)
            .threadTs(threadTs)
            .build();

        final ChatPostMessageResponse postMessageResponse;
        try {
            postMessageResponse = client.chatPostMessage(request);
        } catch (final Exception e) {
            final Relationship relationship = handleClientException(channelId, flowFile, session, context, e);
            getLogger().error("Failed to send message to Slack for {}; transferring to {}", flowFile, relationship, e);
            return;
        }

        if (!postMessageResponse.isOk()) {
            final String errorMessage = SlackResponseUtil.getErrorMessage(postMessageResponse.getError(), postMessageResponse.getNeeded(),
                postMessageResponse.getProvided(), postMessageResponse.getWarning());

            getLogger().error("Could not send message to Slack for {} - received error: {}", flowFile, errorMessage);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        final String ts = postMessageResponse.getTs();
        final Map<String, String> attributes = Map.of("slack.ts", ts,
            "slack.channel.id", channelId);
        flowFile = session.putAllAttributes(flowFile, attributes);
        session.getProvenanceReporter().send(flowFile, "https://slack.com/api/chat.postMessage");
        session.transfer(flowFile, REL_SUCCESS);
    }


    private void publishAsFile(FlowFile flowFile, final String channelId, final ProcessContext context, final ProcessSession session) {
        final String filename = flowFile.getAttribute(CoreAttributes.FILENAME.key());
        final long maxSize = context.getProperty(MAX_FILE_SIZE).asDataSize(DataUnit.B).longValue();
        if (flowFile.getSize() > maxSize) {
            getLogger().warn("{} exceeds max allowable file size. Max File Size = {}; FlowFile size = {}; routing to 'failure'",
                flowFile, FormatUtils.formatDataSize(maxSize), FormatUtils.formatDataSize(flowFile.getSize()));
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        final FilesUploadV2Response uploadResponse;
        try {
            final byte[] buffer = new byte[(int) flowFile.getSize()];
            try (final InputStream in = session.read(flowFile)) {
                StreamUtils.fillBuffer(in, buffer, true);
            }

            final String message = context.getProperty(MESSAGE_TEXT).evaluateAttributeExpressions(flowFile).getValue();
            final String threadTs = context.getProperty(THREAD_TS).evaluateAttributeExpressions(flowFile).getValue();

            final FilesUploadV2Request uploadRequest = FilesUploadV2Request.builder()
                .filename(filename)
                .title(filename)
                .initialComment(message)
                .channel(channelId)
                .threadTs(threadTs)
                .fileData(buffer)
                .build();

            uploadResponse = client.filesUploadV2(uploadRequest);
        } catch (final Exception e) {
            final Relationship relationship = handleClientException(channelId, flowFile, session, context, e);
            getLogger().error("Could not upload contents of {} to Slack; routing to {}", flowFile, relationship, e);
            return;
        }

        if (!uploadResponse.isOk()) {
            final String errorMessage = SlackResponseUtil.getErrorMessage(uploadResponse.getError(), uploadResponse.getNeeded(),
                uploadResponse.getProvided(), uploadResponse.getWarning());

            getLogger().error("Could not upload contents of {} to Slack - received error: {}", flowFile, errorMessage);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        // Get timestamp that the file was shared so that we can add as an attribute.
        final File file = uploadResponse.getFile();
        final Shares shares = file.getShares();
        String ts = null;
        if (shares != null) {
            ts = getTs(shares.getPrivateChannels());
            if (ts == null) {
                ts = getTs(shares.getPublicChannels());
            }
        }

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("slack.channel.id", channelId);
        if (ts != null) {
            attributes.put("slack.ts", ts);
        }
        flowFile = session.putAllAttributes(flowFile, attributes);
        session.getProvenanceReporter().send(flowFile, "https://slack.com/api/files.upload");
        session.transfer(flowFile, REL_SUCCESS);
    }

    private Relationship handleClientException(final String channel, final FlowFile flowFile, final ProcessSession session, final ProcessContext context, final Exception cause) {
        final boolean rateLimited = yieldOnRateLimit(cause, channel, context);
        final Relationship relationship = rateLimited ? REL_RATE_LIMITED : REL_FAILURE;
        session.transfer(flowFile, relationship);
        return relationship;
    }

    private boolean yieldOnRateLimit(final Throwable t, final String channelId, final ProcessContext context) {
        final boolean rateLimited = SlackResponseUtil.isRateLimited(t);
        if (rateLimited) {
            getLogger().warn("Slack indicated that the Rate Limit has been exceeded when attempting to publish messages to channel {}", channelId);
        } else {
            getLogger().error("Failed to retrieve messages for channel {}", channelId, t);
        }

        final int retryAfterSeconds = SlackResponseUtil.getRetryAfterSeconds(t);
        rateLimit.retryAfter(Duration.ofSeconds(retryAfterSeconds));
        context.yield();
        return rateLimited;
    }

    private String getTs(final Map<String, List<ShareDetail>> shareDetails) {
        if (shareDetails == null) {
            return null;
        }

        for (final List<ShareDetail> detailList : shareDetails.values()) {
            for (final ShareDetail detail : detailList) {
                final String ts = detail.getTs();
                if (ts != null) {
                    return ts;
                }
            }
        }

        return null;
    }

}
