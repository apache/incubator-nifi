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
package org.apache.nifi.web.api;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.ServletContext;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.StreamingOutput;
import jakarta.ws.rs.core.UriInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authorization.AccessDeniedException;
import org.apache.nifi.authorization.AuthorizableLookup;
import org.apache.nifi.authorization.AuthorizationResult;
import org.apache.nifi.authorization.AuthorizationResult.Result;
import org.apache.nifi.authorization.PublicPortAuthorizable;
import org.apache.nifi.authorization.resource.ResourceType;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.remote.HttpRemoteSiteListener;
import org.apache.nifi.remote.Peer;
import org.apache.nifi.remote.PeerDescription;
import org.apache.nifi.remote.StandardVersionNegotiator;
import org.apache.nifi.remote.VersionNegotiator;
import org.apache.nifi.remote.client.http.TransportProtocolVersionNegotiator;
import org.apache.nifi.remote.exception.BadRequestException;
import org.apache.nifi.remote.exception.HandshakeException;
import org.apache.nifi.remote.exception.NotAuthorizedException;
import org.apache.nifi.remote.exception.RequestExpiredException;
import org.apache.nifi.remote.io.http.HttpCommunicationsSession;
import org.apache.nifi.remote.io.http.HttpOutput;
import org.apache.nifi.remote.io.http.HttpServerCommunicationsSession;
import org.apache.nifi.remote.protocol.HandshakeProperty;
import org.apache.nifi.remote.protocol.ResponseCode;
import org.apache.nifi.remote.protocol.http.HttpFlowFileServerProtocol;
import org.apache.nifi.remote.protocol.http.StandardHttpFlowFileServerProtocol;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.api.entity.TransactionResultEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.nifi.remote.protocol.HandshakeProperty.BATCH_COUNT;
import static org.apache.nifi.remote.protocol.HandshakeProperty.BATCH_DURATION;
import static org.apache.nifi.remote.protocol.HandshakeProperty.BATCH_SIZE;
import static org.apache.nifi.remote.protocol.HandshakeProperty.REQUEST_EXPIRATION_MILLIS;
import static org.apache.nifi.remote.protocol.http.HttpHeaders.HANDSHAKE_PROPERTY_BATCH_COUNT;
import static org.apache.nifi.remote.protocol.http.HttpHeaders.HANDSHAKE_PROPERTY_BATCH_DURATION;
import static org.apache.nifi.remote.protocol.http.HttpHeaders.HANDSHAKE_PROPERTY_BATCH_SIZE;
import static org.apache.nifi.remote.protocol.http.HttpHeaders.HANDSHAKE_PROPERTY_REQUEST_EXPIRATION;
import static org.apache.nifi.remote.protocol.http.HttpHeaders.HANDSHAKE_PROPERTY_USE_COMPRESSION;

/**
 * RESTful endpoint for managing a SiteToSite connection.
 */
@Controller
@Path("/data-transfer")
@Tag(name = "DataTransfer")
public class DataTransferResource extends ApplicationResource {

    private static final Logger logger = LoggerFactory.getLogger(DataTransferResource.class);

    public static final String CHECK_SUM = "checksum";
    public static final String RESPONSE_CODE = "responseCode";


    private static final String PORT_TYPE_INPUT = "input-ports";
    private static final String PORT_TYPE_OUTPUT = "output-ports";

    private NiFiServiceFacade serviceFacade;
    private final ResponseCreator responseCreator = new ResponseCreator();
    private final VersionNegotiator transportProtocolVersionNegotiator = new TransportProtocolVersionNegotiator(1);
    private final HttpRemoteSiteListener transactionManager;
    private final NiFiProperties nifiProperties;

    public DataTransferResource(final NiFiProperties nifiProperties) {
        this.nifiProperties = nifiProperties;
        transactionManager = HttpRemoteSiteListener.getInstance(nifiProperties);
    }

    /**
     * Authorizes access to data transfers.
     * <p>
     * Note: Protected for testing purposes
     */
    protected void authorizeDataTransfer(final AuthorizableLookup lookup, final ResourceType resourceType, final String identifier) {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        // ensure the resource type is correct
        if (!ResourceType.InputPort.equals(resourceType) && !ResourceType.OutputPort.equals(resourceType)) {
            throw new IllegalArgumentException("The resource must be an Input or Output Port.");
        }

        // get the authorizable
        final PublicPortAuthorizable authorizable;
        if (ResourceType.InputPort.equals(resourceType)) {
            authorizable = lookup.getPublicInputPort(identifier);
        } else {
            authorizable = lookup.getPublicOutputPort(identifier);
        }

        // perform the authorization
        final AuthorizationResult authorizationResult = authorizable.checkAuthorization(user);
        if (!Result.Approved.equals(authorizationResult.getResult())) {
            throw new AccessDeniedException(authorizationResult.getExplanation());
        }
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{portType}/{portId}/transactions")
    @Operation(
            summary = "Create a transaction to the specified output port or input port",
            responses = {
                    @ApiResponse(responseCode = "201", content = @Content(schema = @Schema(implementation = TransactionResultEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it."),
                    @ApiResponse(responseCode = "503", description = "NiFi instance is not ready for serving request, or temporarily overloaded. Retrying the same request later may be successful"),
            },
            security = {
                    @SecurityRequirement(name = "Write - /data-transfer/{component-type}/{uuid}")
            }
    )
    public Response createPortTransaction(
            @Parameter(
                    description = "The port type.",
                    required = true
            )
            @PathParam("portType") String portType,
            @PathParam("portId") String portId,
            @Context HttpServletRequest req,
            @Context ServletContext context,
            @Context UriInfo uriInfo,
            InputStream inputStream) {


        if (!PORT_TYPE_INPUT.equals(portType) && !PORT_TYPE_OUTPUT.equals(portType)) {
            return responseCreator.wrongPortTypeResponse(portType, portId);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            authorizeDataTransfer(lookup, PORT_TYPE_INPUT.equals(portType) ? ResourceType.InputPort : ResourceType.OutputPort, portId);
        });

        final ValidateRequestResult validationResult = validateResult(req, portId);
        if (validationResult.errResponse != null) {
            return validationResult.errResponse;
        }

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final String transactionId = transactionManager.createTransaction();
        final Peer peer = constructPeer(req, inputStream, out, portId, transactionId);
        final int transportProtocolVersion = validationResult.transportProtocolVersion;

        try {
            // Execute handshake.
            initiateServerProtocol(req, peer, transportProtocolVersion);

            TransactionResultEntity entity = new TransactionResultEntity();
            entity.setResponseCode(ResponseCode.PROPERTIES_OK.getCode());
            entity.setMessage("Handshake properties are valid, and port is running. A transaction is created:" + transactionId);

            return responseCreator.locationResponse(uriInfo, portType, portId, transactionId, entity, transportProtocolVersion, transactionManager);

        } catch (HandshakeException e) {
            transactionManager.cancelTransaction(transactionId);
            return responseCreator.handshakeExceptionResponse(e);

        } catch (Exception e) {
            transactionManager.cancelTransaction(transactionId);
            return responseCreator.unexpectedErrorResponse(portId, e);
        }
    }

    @POST
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Produces(MediaType.TEXT_PLAIN)
    @Path("input-ports/{portId}/transactions/{transactionId}/flow-files")
    @Operation(
            summary = "Transfer flow files to the input port",
            responses = {
                    @ApiResponse(responseCode = "202", content = @Content(schema = @Schema(implementation = String.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it."),
                    @ApiResponse(responseCode = "503", description = "NiFi instance is not ready for serving request, or temporarily overloaded. Retrying the same request later may be successful"),
            },
            security = {
                    @SecurityRequirement(name = "Write - /data-transfer/input-ports/{uuid}")
            }
    )
    public Response receiveFlowFiles(
            @Parameter(
                    description = "The input port id.",
                    required = true
            )
            @PathParam("portId") String portId,
            @PathParam("transactionId") String transactionId,
            @Context HttpServletRequest req,
            @Context ServletContext context,
            InputStream inputStream) {

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            authorizeDataTransfer(lookup, ResourceType.InputPort, portId);
        });

        final ValidateRequestResult validationResult = validateResult(req, portId, transactionId);
        if (validationResult.errResponse != null) {
            return validationResult.errResponse;
        }

        logger.debug("receiveFlowFiles request: portId={}", portId);

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final Peer peer = constructPeer(req, inputStream, out, portId, transactionId);
        final int transportProtocolVersion = validationResult.transportProtocolVersion;

        try {
            HttpFlowFileServerProtocol serverProtocol = initiateServerProtocol(req, peer, transportProtocolVersion);
            int numOfFlowFiles = serverProtocol.getPort().receiveFlowFiles(peer, serverProtocol);
            logger.debug("finished receiving flow files, numOfFlowFiles={}", numOfFlowFiles);
            if (numOfFlowFiles < 1) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity("Client should send request when there is data to send. There was no flow file sent.").build();
            }
        } catch (HandshakeException e) {
            return responseCreator.handshakeExceptionResponse(e);

        } catch (NotAuthorizedException e) {
            return responseCreator.unauthorizedResponse(e);

        } catch (BadRequestException | RequestExpiredException e) {
            return responseCreator.badRequestResponse(e);

        } catch (Exception e) {
            return responseCreator.unexpectedErrorResponse(portId, e);
        }

        String serverChecksum = ((HttpServerCommunicationsSession) peer.getCommunicationsSession()).getChecksum();
        return responseCreator.acceptedResponse(transactionManager, serverChecksum, transportProtocolVersion);
    }

    private HttpFlowFileServerProtocol initiateServerProtocol(final HttpServletRequest req, final Peer peer,
                                                              final Integer transportProtocolVersion) throws IOException {
        // Switch transaction protocol version based on transport protocol version.
        TransportProtocolVersionNegotiator negotiatedTransportProtocolVersion = new TransportProtocolVersionNegotiator(transportProtocolVersion);
        VersionNegotiator versionNegotiator = new StandardVersionNegotiator(negotiatedTransportProtocolVersion.getTransactionProtocolVersion());

        final String dataTransferUrl = req.getRequestURL().toString();
        ((HttpCommunicationsSession) peer.getCommunicationsSession()).setDataTransferUrl(dataTransferUrl);

        HttpFlowFileServerProtocol serverProtocol = getHttpFlowFileServerProtocol(versionNegotiator);
        HttpRemoteSiteListener.getInstance(nifiProperties).setupServerProtocol(serverProtocol);
        serverProtocol.handshake(peer);
        return serverProtocol;
    }

    HttpFlowFileServerProtocol getHttpFlowFileServerProtocol(final VersionNegotiator versionNegotiator) {
        return new StandardHttpFlowFileServerProtocol(versionNegotiator, nifiProperties);
    }

    private Peer constructPeer(final HttpServletRequest req, final InputStream inputStream,
                               final OutputStream outputStream, final String portId, final String transactionId) {
        String clientHostName = req.getRemoteHost();
        try {
            // req.getRemoteHost returns IP address, try to resolve hostname to be consistent with RAW protocol.
            final InetAddress clientAddress = InetAddress.getByName(clientHostName);
            clientHostName = clientAddress.getHostName();
        } catch (UnknownHostException e) {
            logger.info("Failed to resolve client hostname {}, due to {}", clientHostName, e.getMessage());
        }
        final int clientPort = req.getRemotePort();

        final PeerDescription peerDescription = new PeerDescription(clientHostName, clientPort, req.isSecure());

        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        final String userDn = user == null ? null : user.getIdentity();
        final HttpServerCommunicationsSession commSession = new HttpServerCommunicationsSession(inputStream, outputStream, transactionId, userDn);

        boolean useCompression = false;
        final String useCompressionStr = req.getHeader(HANDSHAKE_PROPERTY_USE_COMPRESSION);
        if (!isEmpty(useCompressionStr) && Boolean.valueOf(useCompressionStr)) {
            useCompression = true;
        }

        final String requestExpiration = req.getHeader(HANDSHAKE_PROPERTY_REQUEST_EXPIRATION);
        final String batchCount = req.getHeader(HANDSHAKE_PROPERTY_BATCH_COUNT);
        final String batchSize = req.getHeader(HANDSHAKE_PROPERTY_BATCH_SIZE);
        final String batchDuration = req.getHeader(HANDSHAKE_PROPERTY_BATCH_DURATION);

        commSession.putHandshakeParam(HandshakeProperty.PORT_IDENTIFIER, portId);
        commSession.putHandshakeParam(HandshakeProperty.GZIP, String.valueOf(useCompression));

        if (!isEmpty(requestExpiration)) {
            commSession.putHandshakeParam(REQUEST_EXPIRATION_MILLIS, requestExpiration);
        }
        if (!isEmpty(batchCount)) {
            commSession.putHandshakeParam(BATCH_COUNT, batchCount);
        }
        if (!isEmpty(batchSize)) {
            commSession.putHandshakeParam(BATCH_SIZE, batchSize);
        }
        if (!isEmpty(batchDuration)) {
            commSession.putHandshakeParam(BATCH_DURATION, batchDuration);
        }

        if (peerDescription.isSecure()) {
            final NiFiUser nifiUser = NiFiUserUtils.getNiFiUser();
            logger.debug("initiating peer, nifiUser={}", nifiUser);
            commSession.setUserDn(nifiUser.getIdentity());
        }

        // TODO: Followed how SocketRemoteSiteListener define peerUrl and clusterUrl, but it can be more meaningful values, especially for clusterUrl.
        final String peerUrl = "nifi://" + clientHostName + ":" + clientPort;
        final String clusterUrl = "nifi://localhost:" + req.getLocalPort();

        return new Peer(peerDescription, commSession, peerUrl, clusterUrl);
    }

    @DELETE
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("output-ports/{portId}/transactions/{transactionId}")
    @Operation(
            summary = "Commit or cancel the specified transaction",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = TransactionResultEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it."),
                    @ApiResponse(responseCode = "503", description = "NiFi instance is not ready for serving request, or temporarily overloaded. Retrying the same request later may be successful"),
            },
            security = {
                    @SecurityRequirement(name = "Write - /data-transfer/output-ports/{uuid}")
            }
    )
    public Response commitOutputPortTransaction(
            @Parameter(
                    description = "The response code. Available values are CONFIRM_TRANSACTION(12) or CANCEL_TRANSACTION(15).",
                    required = true
            )
            @QueryParam(RESPONSE_CODE) Integer responseCode,
            @Parameter(
                    description = "A checksum calculated at client side using CRC32 to check flow file content integrity. It must match with the value calculated at server side.",
                    required = true
            )
            @QueryParam(CHECK_SUM) @DefaultValue(StringUtils.EMPTY) String checksum,
            @Parameter(
                    description = "The output port id.",
                    required = true
            )
            @PathParam("portId") String portId,
            @Parameter(
                    description = "The transaction id.",
                    required = true
            )
            @PathParam("transactionId") String transactionId,
            @Context HttpServletRequest req,
            @Context ServletContext context,
            InputStream inputStream) {

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            authorizeDataTransfer(lookup, ResourceType.OutputPort, portId);
        });

        final ValidateRequestResult validationResult = validateResult(req, portId, transactionId);
        if (validationResult.errResponse != null) {
            return validationResult.errResponse;
        }

        logger.debug("commitOutputPortTransaction request: portId={}, transactionId={}", portId, transactionId);

        final int transportProtocolVersion = validationResult.transportProtocolVersion;
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final Peer peer = constructPeer(req, inputStream, out, portId, transactionId);

        final TransactionResultEntity entity = new TransactionResultEntity();
        try {
            HttpFlowFileServerProtocol serverProtocol = initiateServerProtocol(req, peer, transportProtocolVersion);

            String inputErrMessage = null;
            if (responseCode == null) {
                inputErrMessage = "responseCode is required.";
            } else if (ResponseCode.CONFIRM_TRANSACTION.getCode() != responseCode
                    && ResponseCode.CANCEL_TRANSACTION.getCode() != responseCode) {
                inputErrMessage = "responseCode " + responseCode + " is invalid. ";
            }

            if (inputErrMessage != null) {
                entity.setMessage(inputErrMessage);
                entity.setResponseCode(ResponseCode.ABORT.getCode());
                return Response.status(Response.Status.BAD_REQUEST).entity(entity).build();
            }

            if (ResponseCode.CANCEL_TRANSACTION.getCode() == responseCode) {
                return cancelTransaction(transactionId, entity);
            }

            int flowFileSent = serverProtocol.commitTransferTransaction(peer, checksum);
            entity.setResponseCode(ResponseCode.CONFIRM_TRANSACTION.getCode());
            entity.setFlowFileSent(flowFileSent);

        } catch (HandshakeException e) {
            return responseCreator.handshakeExceptionResponse(e);

        } catch (Exception e) {
            HttpServerCommunicationsSession commsSession = (HttpServerCommunicationsSession) peer.getCommunicationsSession();
            logger.error("Failed to process the request", e);
            if (ResponseCode.BAD_CHECKSUM.equals(commsSession.getResponseCode())) {
                entity.setResponseCode(commsSession.getResponseCode().getCode());
                entity.setMessage(e.getMessage());

                Response.ResponseBuilder builder = Response.status(Response.Status.BAD_REQUEST).entity(entity);
                return noCache(builder).build();
            }

            return responseCreator.unexpectedErrorResponse(portId, transactionId, e);
        }

        return noCache(setCommonHeaders(Response.ok(entity), transportProtocolVersion, transactionManager)).build();
    }


    @DELETE
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("input-ports/{portId}/transactions/{transactionId}")
    @Operation(
            summary = "Commit or cancel the specified transaction",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = TransactionResultEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it."),
                    @ApiResponse(responseCode = "503", description = "NiFi instance is not ready for serving request, or temporarily overloaded. Retrying the same request later may be successful"),
            },
            security = {
                    @SecurityRequirement(name = "Write - /data-transfer/input-ports/{uuid}")
            }
    )
    public Response commitInputPortTransaction(
            @Parameter(
                    description = "The response code. Available values are BAD_CHECKSUM(19), CONFIRM_TRANSACTION(12) or CANCEL_TRANSACTION(15).",
                    required = true
            )
            @QueryParam(RESPONSE_CODE) Integer responseCode,
            @Parameter(
                    description = "The input port id.",
                    required = true
            )
            @PathParam("portId") String portId,
            @Parameter(
                    description = "The transaction id.",
                    required = true
            )
            @PathParam("transactionId") String transactionId,
            @Context HttpServletRequest req,
            @Context ServletContext context,
            InputStream inputStream) {

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            authorizeDataTransfer(lookup, ResourceType.InputPort, portId);
        });

        final ValidateRequestResult validationResult = validateResult(req, portId, transactionId);
        if (validationResult.errResponse != null) {
            return validationResult.errResponse;
        }

        logger.debug("commitInputPortTransaction request: portId={}, transactionId={}, responseCode={}",
                portId, transactionId, responseCode);

        final int transportProtocolVersion = validationResult.transportProtocolVersion;
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final Peer peer = constructPeer(req, inputStream, out, portId, transactionId);

        final TransactionResultEntity entity = new TransactionResultEntity();
        try {
            HttpFlowFileServerProtocol serverProtocol = initiateServerProtocol(req, peer, transportProtocolVersion);
            HttpServerCommunicationsSession commsSession = (HttpServerCommunicationsSession) peer.getCommunicationsSession();
            // Pass the response code sent from the client.
            String inputErrMessage = null;
            if (responseCode == null) {
                inputErrMessage = "responseCode is required.";
            } else if (ResponseCode.BAD_CHECKSUM.getCode() != responseCode
                    && ResponseCode.CONFIRM_TRANSACTION.getCode() != responseCode
                    && ResponseCode.CANCEL_TRANSACTION.getCode() != responseCode) {
                inputErrMessage = "responseCode " + responseCode + " is invalid. ";
            }

            if (inputErrMessage != null) {
                entity.setMessage(inputErrMessage);
                entity.setResponseCode(ResponseCode.ABORT.getCode());
                return Response.status(Response.Status.BAD_REQUEST).entity(entity).build();
            }

            if (ResponseCode.CANCEL_TRANSACTION.getCode() == responseCode) {
                return cancelTransaction(transactionId, entity);
            }

            commsSession.setResponseCode(ResponseCode.fromCode(responseCode));

            try {
                int flowFileSent = serverProtocol.commitReceiveTransaction(peer);
                entity.setResponseCode(commsSession.getResponseCode().getCode());
                entity.setFlowFileSent(flowFileSent);

            } catch (IOException e) {
                if (ResponseCode.BAD_CHECKSUM.getCode() == responseCode && e.getMessage().contains("Received a BadChecksum response")) {
                    // AbstractFlowFileServerProtocol throws IOException after it canceled transaction.
                    // This is a known behavior and if we return 500 with this exception,
                    // it's not clear if there is an issue at server side, or cancel operation has been accomplished.
                    // Above conditions can guarantee this is the latter case, we return 200 OK here.
                    entity.setResponseCode(ResponseCode.CANCEL_TRANSACTION.getCode());
                    return noCache(Response.ok(entity)).build();
                } else {
                    return responseCreator.unexpectedErrorResponse(portId, transactionId, e);
                }
            }

        } catch (HandshakeException e) {
            return responseCreator.handshakeExceptionResponse(e);

        } catch (Exception e) {
            return responseCreator.unexpectedErrorResponse(portId, transactionId, e);
        }

        return noCache(setCommonHeaders(Response.ok(entity), transportProtocolVersion, transactionManager)).build();
    }

    private Response cancelTransaction(String transactionId, TransactionResultEntity entity) {
        transactionManager.cancelTransaction(transactionId);
        entity.setMessage("Transaction has been canceled.");
        entity.setResponseCode(ResponseCode.CANCEL_TRANSACTION.getCode());
        return Response.ok(entity).build();
    }


    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    @Path("output-ports/{portId}/transactions/{transactionId}/flow-files")
    @Operation(
            summary = "Transfer flow files from the output port",
            responses = {
                    @ApiResponse(responseCode = "202", content = @Content(schema = @Schema(implementation = StreamingOutput.class))),
                    @ApiResponse(responseCode = "200", description = "There is no flow file to return."),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it."),
                    @ApiResponse(responseCode = "503", description = "NiFi instance is not ready for serving request, or temporarily overloaded. Retrying the same request later may be successful"),
            },
            security = {
                    @SecurityRequirement(name = "Write - /data-transfer/output-ports/{uuid}")
            }
    )
    public Response transferFlowFiles(
            @Parameter(
                    description = "The output port id.",
                    required = true
            )
            @PathParam("portId") String portId,
            @PathParam("transactionId") String transactionId,
            @Context HttpServletRequest req,
            @Context HttpServletResponse res,
            @Context ServletContext context,
            InputStream inputStream) {

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            authorizeDataTransfer(lookup, ResourceType.OutputPort, portId);
        });

        final ValidateRequestResult validationResult = validateResult(req, portId, transactionId);
        if (validationResult.errResponse != null) {
            return validationResult.errResponse;
        }

        logger.debug("transferFlowFiles request: portId={}", portId);

        // Before opening the real output stream for HTTP response,
        // use this temporary output stream to buffer handshake result.
        final ByteArrayOutputStream tempBos = new ByteArrayOutputStream();
        final Peer peer = constructPeer(req, inputStream, tempBos, portId, transactionId);
        final int transportProtocolVersion = validationResult.transportProtocolVersion;
        try {
            final HttpFlowFileServerProtocol serverProtocol = initiateServerProtocol(req, peer, transportProtocolVersion);

            StreamingOutput flowFileContent = outputStream -> {

                HttpOutput output = (HttpOutput) peer.getCommunicationsSession().getOutput();
                output.setOutputStream(outputStream);

                try {
                    int numOfFlowFiles = serverProtocol.getPort().transferFlowFiles(peer, serverProtocol);
                    logger.debug("finished transferring flow files, numOfFlowFiles={}", numOfFlowFiles);
                    if (numOfFlowFiles < 1) {
                        // There was no flow file to transfer. Throw this exception to stop responding with SEE OTHER.
                        throw new WebApplicationException(Response.Status.OK);
                    }
                } catch (NotAuthorizedException | BadRequestException | RequestExpiredException e) {
                    // Handshake is done outside of write() method, so these exception wouldn't be thrown.
                    throw new IOException("Failed to process the request.", e);
                }
            };

            return responseCreator.acceptedResponse(transactionManager, flowFileContent, transportProtocolVersion);

        } catch (HandshakeException e) {
            return responseCreator.handshakeExceptionResponse(e);

        } catch (Exception e) {
            return responseCreator.unexpectedErrorResponse(portId, e);
        }
    }

    @PUT
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("input-ports/{portId}/transactions/{transactionId}")
    @Operation(
            summary = "Extend transaction TTL",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = TransactionResultEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /data-transfer/input-ports/{uuid}")
            }
    )
    public Response extendInputPortTransactionTTL(
            @PathParam("portId") String portId,
            @PathParam("transactionId") String transactionId,
            @Context HttpServletRequest req,
            @Context HttpServletResponse res,
            @Context ServletContext context,
            @Context UriInfo uriInfo,
            InputStream inputStream) {

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            authorizeDataTransfer(lookup, ResourceType.InputPort, portId);
        });

        return extendPortTransactionTTL(PORT_TYPE_INPUT, portId, transactionId, req, res, context, uriInfo, inputStream);
    }

    @PUT
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("output-ports/{portId}/transactions/{transactionId}")
    @Operation(
            summary = "Extend transaction TTL",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = TransactionResultEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it."),
                    @ApiResponse(responseCode = "503", description = "NiFi instance is not ready for serving request, or temporarily overloaded. Retrying the same request later may be successful"),
            },
            security = {
                    @SecurityRequirement(name = "Write - /data-transfer/output-ports/{uuid}")
            }
    )
    public Response extendOutputPortTransactionTTL(
            @PathParam("portId") String portId,
            @PathParam("transactionId") String transactionId,
            @Context HttpServletRequest req,
            @Context HttpServletResponse res,
            @Context ServletContext context,
            @Context UriInfo uriInfo,
            InputStream inputStream) {

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            authorizeDataTransfer(lookup, ResourceType.OutputPort, portId);
        });

        return extendPortTransactionTTL(PORT_TYPE_OUTPUT, portId, transactionId, req, res, context, uriInfo, inputStream);
    }

    public Response extendPortTransactionTTL(
            String portType,
            String portId,
            String transactionId,
            HttpServletRequest req,
            HttpServletResponse res,
            ServletContext context,
            UriInfo uriInfo,
            InputStream inputStream) {

        final ValidateRequestResult validationResult = validateResult(req, portId, transactionId);
        if (validationResult.errResponse != null) {
            return validationResult.errResponse;
        }

        if (!PORT_TYPE_INPUT.equals(portType) && !PORT_TYPE_OUTPUT.equals(portType)) {
            return responseCreator.wrongPortTypeResponse(portType, portId);
        }

        logger.debug("extendOutputPortTransactionTTL request: portType={}, portId={}, transactionId={}",
                portType, portId, transactionId);

        final int transportProtocolVersion = validationResult.transportProtocolVersion;
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final Peer peer = constructPeer(req, inputStream, out, portId, transactionId);

        try {
            // Do handshake
            initiateServerProtocol(req, peer, transportProtocolVersion);
            transactionManager.extendTransaction(transactionId);

            final TransactionResultEntity entity = new TransactionResultEntity();
            entity.setResponseCode(ResponseCode.CONTINUE_TRANSACTION.getCode());
            entity.setMessage("Extended TTL.");
            return noCache(setCommonHeaders(Response.ok(entity), transportProtocolVersion, transactionManager)).build();

        } catch (HandshakeException e) {
            return responseCreator.handshakeExceptionResponse(e);

        } catch (Exception e) {
            return responseCreator.unexpectedErrorResponse(portId, transactionId, e);
        }

    }

    private class ValidateRequestResult {
        private Integer transportProtocolVersion;
        private Response errResponse;
    }

    private ValidateRequestResult validateResult(HttpServletRequest req, String portId) {
        return validateResult(req, portId, null);
    }

    private ValidateRequestResult validateResult(HttpServletRequest req, String portId, String transactionId) {
        ValidateRequestResult result = new ValidateRequestResult();
        if (!properties.isSiteToSiteHttpEnabled()) {
            result.errResponse = responseCreator.httpSiteToSiteIsNotEnabledResponse();
            return result;
        }

        try {
            result.transportProtocolVersion = negotiateTransportProtocolVersion(req, transportProtocolVersionNegotiator);
        } catch (BadRequestException e) {
            result.errResponse = responseCreator.badRequestResponse(e);
            return result;
        }

        if (!isEmpty(transactionId) && !transactionManager.isTransactionActive(transactionId)) {
            result.errResponse = responseCreator.transactionNotFoundResponse(portId, transactionId);
            return result;
        }

        return result;
    }

    @Autowired
    public void setServiceFacade(NiFiServiceFacade serviceFacade) {
        this.serviceFacade = serviceFacade;
    }
}
