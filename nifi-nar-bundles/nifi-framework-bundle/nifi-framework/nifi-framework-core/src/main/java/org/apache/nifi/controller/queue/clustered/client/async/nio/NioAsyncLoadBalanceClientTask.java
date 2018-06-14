package org.apache.nifi.controller.queue.clustered.client.async.nio;

import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.node.NodeConnectionState;
import org.apache.nifi.cluster.coordination.node.NodeConnectionStatus;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NioAsyncLoadBalanceClientTask implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(NioAsyncLoadBalanceClientTask.class);

    private final NioAsyncLoadBalanceClientRegistry clientRegistry;
    private final ClusterCoordinator clusterCoordinator;
    private volatile boolean running = true;

    public NioAsyncLoadBalanceClientTask(final NioAsyncLoadBalanceClientRegistry clientRegistry, final ClusterCoordinator clusterCoordinator) {
        this.clientRegistry = clientRegistry;
        this.clusterCoordinator = clusterCoordinator;
    }

    @Override
    public void run() {
        while (running) {
            try {
                boolean success = false;
                // TODO: Make getAllClients() more efficient by just keeping a set of them.
                for (final NioAsyncLoadBalanceClient client : clientRegistry.getAllClients()) {
                    if (!client.isRunning()) {
                        logger.trace("Client {} is not running so will not communicate with it", client);
                        continue;
                    }

                    if (client.isPenalized()) {
                        logger.trace("Client {} is penalized so will not communicate with it", client);
                        continue;
                    }

                    final NodeIdentifier clientNodeId = client.getNodeIdentifier();
                    final NodeConnectionStatus connectionStatus = clusterCoordinator.getConnectionStatus(clientNodeId);
                    final NodeConnectionState connectionState = connectionStatus.getState();
                    if (connectionState != NodeConnectionState.CONNECTED) {
                        logger.trace("Node {} has a Connection State of {} so will not communicate with it", clientNodeId, connectionState);
                        continue;
                    }

                    // TODO: Do to the nature of how this is used, we need to ensure that client.communicate() doesn't use a synchronized keyword
                    // but instead uses a Lock with tryLock(). This way, we don't wait on another thread but instead just move on.
                    // Continue communicating with the Peer until we block due to the socket buffer being full, waiting on response from Peer, etc.
                    try {
                        while (client.communicate()) {
                            success = true;
                            logger.trace("Client {} was able to make progress communicating with peer. Will continue to communicate with peer.", client);
                        }
                    } catch (final Exception e) {
                        logger.error("Failed to communicate with Peer {} while trying to load balance data across the cluster.", client.getNodeIdentifier(), e);
                    }

                    logger.trace("Client {} was no longer able to make progress communicating with peer. Will move on to the next client", client);
                }

                if (!success) {
                    logger.trace("Was unable to communicate with any client. Will sleep for 1 millisecond.");
                    Thread.sleep(1L);
                }
            } catch (final Exception e) {
                logger.error("Failed to communicate with peer while trying to load balance data across the cluster", e);
            }
        }
    }

    public void stop() {
        running = false;
    }
}
