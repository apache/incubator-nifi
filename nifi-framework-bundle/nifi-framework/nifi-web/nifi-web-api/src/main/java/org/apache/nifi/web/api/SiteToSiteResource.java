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
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.node.NodeWorkload;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.remote.HttpRemoteSiteListener;
import org.apache.nifi.remote.PeerDescription;
import org.apache.nifi.remote.PeerDescriptionModifier;
import org.apache.nifi.remote.VersionNegotiator;
import org.apache.nifi.remote.client.http.TransportProtocolVersionNegotiator;
import org.apache.nifi.remote.exception.BadRequestException;
import org.apache.nifi.remote.protocol.SiteToSiteTransportProtocol;
import org.apache.nifi.remote.protocol.http.HttpHeaders;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.api.dto.ControllerDTO;
import org.apache.nifi.web.api.dto.remote.PeerDTO;
import org.apache.nifi.web.api.entity.ControllerEntity;
import org.apache.nifi.web.api.entity.PeersEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.commons.lang3.StringUtils.isEmpty;

/**
 * RESTful endpoint for managing a SiteToSite connection.
 */
@Controller
@Path("/site-to-site")
@Tag(name = "SiteToSite")
public class SiteToSiteResource extends ApplicationResource {

    private static final Logger logger = LoggerFactory.getLogger(SiteToSiteResource.class);

    private NiFiServiceFacade serviceFacade;
    private ClusterCoordinator clusterCoordinator;
    private Authorizer authorizer;

    private final ResponseCreator responseCreator = new ResponseCreator();
    private final VersionNegotiator transportProtocolVersionNegotiator = new TransportProtocolVersionNegotiator(1);
    private final HttpRemoteSiteListener transactionManager;
    private final PeerDescriptionModifier peerDescriptionModifier;

    public SiteToSiteResource(final NiFiProperties nifiProperties) {
        transactionManager = HttpRemoteSiteListener.getInstance(nifiProperties);
        peerDescriptionModifier = new PeerDescriptionModifier(nifiProperties);
    }

    /**
     * Authorizes access to Site To Site details.
     * <p>
     * Note: Protected for testing purposes
     */
    protected void authorizeSiteToSite() {
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable siteToSite = lookup.getSiteToSite();
            siteToSite.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });
    }

    /**
     * Returns the details of this NiFi.
     *
     * @return A controllerEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Returns the details about this NiFi necessary to communicate via site to site",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ControllerEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /site-to-site")
            }
    )
    public Response getSiteToSiteDetails(@Context HttpServletRequest req) {

        authorizeSiteToSite();

        // get the controller dto
        final ControllerDTO controller = serviceFacade.getSiteToSiteDetails();

        // Alter s2s port.
        final boolean modificationNeededRaw = peerDescriptionModifier.isModificationNeeded(SiteToSiteTransportProtocol.RAW);
        final boolean modificationNeededHttp = peerDescriptionModifier.isModificationNeeded(SiteToSiteTransportProtocol.HTTP);
        if (modificationNeededRaw || modificationNeededHttp) {
            final PeerDescription source = getSourcePeerDescription(req);
            final Boolean isSiteToSiteSecure = controller.isSiteToSiteSecure();
            final String siteToSiteHostname = getSiteToSiteHostname(req);
            final Map<String, String> httpHeaders = getHttpHeaders(req);

            if (modificationNeededRaw) {
                final PeerDescription rawTarget = new PeerDescription(siteToSiteHostname, controller.getRemoteSiteListeningPort(), isSiteToSiteSecure);
                final PeerDescription modifiedRawTarget = peerDescriptionModifier.modify(source, rawTarget,
                        SiteToSiteTransportProtocol.RAW, PeerDescriptionModifier.RequestType.SiteToSiteDetail, new HashMap<>(httpHeaders));
                controller.setRemoteSiteListeningPort(modifiedRawTarget.getPort());
            }

            if (modificationNeededHttp) {
                final PeerDescription httpTarget = new PeerDescription(siteToSiteHostname, controller.getRemoteSiteHttpListeningPort(), isSiteToSiteSecure);
                final PeerDescription modifiedHttpTarget = peerDescriptionModifier.modify(source, httpTarget,
                        SiteToSiteTransportProtocol.HTTP, PeerDescriptionModifier.RequestType.SiteToSiteDetail, new HashMap<>(httpHeaders));
                controller.setRemoteSiteHttpListeningPort(modifiedHttpTarget.getPort());
                if (!controller.isSiteToSiteSecure() && modifiedHttpTarget.isSecure()) {
                    // In order to enable TLS terminate at the reverse proxy server, even if NiFi itself is not secured, introduce the endpoint as secure.
                    controller.setSiteToSiteSecure(true);
                }
            }
        }

        // build the response entity
        final ControllerEntity entity = new ControllerEntity();
        entity.setController(controller);

        if (isEmpty(req.getHeader(HttpHeaders.PROTOCOL_VERSION))) {
            // This indicates the client uses older NiFi version,
            // which strictly read JSON properties and fail with unknown properties.
            // Convert result entity so that old version clients can understand.
            logger.debug("Converting result to provide backward compatibility...");
            controller.setRemoteSiteHttpListeningPort(null);
        }

        // generate the response
        return noCache(Response.ok(entity)).build();
    }

    private PeerDescription getSourcePeerDescription(@Context HttpServletRequest req) {
        return new PeerDescription(req.getRemoteHost(), req.getRemotePort(), req.isSecure());
    }

    private Map<String, String> getHttpHeaders(@Context HttpServletRequest req) {
        final Map<String, String> headers = new HashMap<>();
        final Enumeration<String> headerNames = req.getHeaderNames();
        while (headerNames.hasMoreElements()) {
            final String name = headerNames.nextElement();
            headers.put(name, req.getHeader(name));
        }
        return headers;
    }

    /**
     * Returns the available Peers and its status of this NiFi.
     *
     * @return A peersEntity.
     */
    @GET
    @Path("/peers")
    @Consumes(MediaType.WILDCARD)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Operation(
            summary = "Returns the available Peers and its status of this NiFi",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = PeersEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /site-to-site")
            }
    )
    public Response getPeers(@Context HttpServletRequest req) {

        authorizeSiteToSite();

        if (!properties.isSiteToSiteHttpEnabled()) {
            return responseCreator.httpSiteToSiteIsNotEnabledResponse();
        }

        final Integer transportProtocolVersion;
        try {
            transportProtocolVersion = negotiateTransportProtocolVersion(req, transportProtocolVersionNegotiator);
        } catch (BadRequestException e) {
            return responseCreator.badRequestResponse(e);
        }

        final List<PeerDTO> peers = new ArrayList<>();
        final PeerDescription source = getSourcePeerDescription(req);
        final boolean modificationNeeded = peerDescriptionModifier.isModificationNeeded(SiteToSiteTransportProtocol.HTTP);
        final Map<String, String> headers = modificationNeeded ? getHttpHeaders(req) : null;
        if (properties.isNode()) {

            try {
                final Map<NodeIdentifier, NodeWorkload> clusterWorkload = clusterCoordinator.getClusterWorkload();
                clusterWorkload.forEach((nodeId, workload) -> {
                    final String siteToSiteHostname = nodeId.getSiteToSiteAddress() == null ? nodeId.getApiAddress() : nodeId.getSiteToSiteAddress();
                    final int siteToSitePort = nodeId.getSiteToSiteHttpApiPort() == null ? nodeId.getApiPort() : nodeId.getSiteToSiteHttpApiPort();

                    PeerDescription target = new PeerDescription(siteToSiteHostname, siteToSitePort, nodeId.isSiteToSiteSecure());

                    if (modificationNeeded) {
                        target = peerDescriptionModifier.modify(source, target,
                                SiteToSiteTransportProtocol.HTTP, PeerDescriptionModifier.RequestType.Peers, new HashMap<>(headers));
                    }

                    final PeerDTO peer = new PeerDTO();
                    peer.setHostname(target.getHostname());
                    peer.setPort(target.getPort());
                    peer.setSecure(target.isSecure());
                    peer.setFlowFileCount(workload.getFlowFileCount());
                    peers.add(peer);
                });
            } catch (IOException e) {
                throw new RuntimeException("Failed to retrieve cluster workload due to " + e, e);
            }

        } else {
            // Standalone mode.
            final PeerDTO peer = new PeerDTO();
            final String siteToSiteHostname = getSiteToSiteHostname(req);


            PeerDescription target = new PeerDescription(siteToSiteHostname,
                    properties.getRemoteInputHttpPort(), properties.isSiteToSiteSecure());

            if (modificationNeeded) {
                target = peerDescriptionModifier.modify(source, target,
                        SiteToSiteTransportProtocol.HTTP, PeerDescriptionModifier.RequestType.Peers, new HashMap<>(headers));
            }

            peer.setHostname(target.getHostname());
            peer.setPort(target.getPort());
            peer.setSecure(target.isSecure());
            peer.setFlowFileCount(0);  // doesn't matter how many FlowFiles we have, because we're the only host.

            peers.add(peer);
        }

        final PeersEntity entity = new PeersEntity();
        entity.setPeers(peers);

        return noCache(setCommonHeaders(Response.ok(entity), transportProtocolVersion, transactionManager)).build();
    }

    private String getSiteToSiteHostname(final HttpServletRequest req) {
        // Private IP address or hostname may not be accessible from client in some environments.
        // So, use the value defined in nifi.properties instead when it is defined.
        final String remoteInputHost = properties.getRemoteInputHost();
        String localName;
        try {
            // Get local host name using InetAddress if available, same as RAW socket does.
            localName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            if (logger.isDebugEnabled()) {
                logger.debug("Failed to get local host name using InetAddress.", e);
            }
            localName = req.getLocalName();
        }

        return isEmpty(remoteInputHost) ? localName : remoteInputHost;
    }

    @Autowired
    public void setServiceFacade(final NiFiServiceFacade serviceFacade) {
        this.serviceFacade = serviceFacade;
    }

    @Autowired
    public void setAuthorizer(Authorizer authorizer) {
        this.authorizer = authorizer;
    }

    @Autowired(required = false)
    @Override
    public void setClusterCoordinator(final ClusterCoordinator clusterCoordinator) {
        super.setClusterCoordinator(clusterCoordinator);
        this.clusterCoordinator = clusterCoordinator;
    }
}
