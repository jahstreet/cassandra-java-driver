/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.internal.core.metadata;

import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.token.TokenFactory;
import com.datastax.oss.driver.internal.core.metadata.token.TokenFactoryRegistry;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The first node list refresh: contact points are not in the metadata yet, we need to copy them
 * over.
 */
@ThreadSafe
class InitialNodeListRefresh extends NodesRefresh {

  private static final Logger LOG = LoggerFactory.getLogger(InitialNodeListRefresh.class);

  @VisibleForTesting final Iterable<NodeInfo> nodeInfos;
  @VisibleForTesting final Set<DefaultNode> contactPoints;

  InitialNodeListRefresh(Iterable<NodeInfo> nodeInfos, Set<DefaultNode> contactPoints) {
    this.nodeInfos = nodeInfos;
    this.contactPoints = contactPoints;
  }

  @Override
  public Result compute(
      DefaultMetadata oldMetadata, boolean tokenMapEnabled, InternalDriverContext context) {

    String logPrefix = context.getSessionName();
    TokenFactoryRegistry tokenFactoryRegistry = context.getTokenFactoryRegistry();

    // Since this is the first refresh, and we've stored contact points separately until now, the
    // metadata is empty.
    assert oldMetadata == DefaultMetadata.EMPTY;
    TokenFactory tokenFactory = null;

    Map<UUID, DefaultNode> newNodes = new HashMap<>();

    for (NodeInfo nodeInfo : nodeInfos) {
      UUID hostId = nodeInfo.getHostId();
      if (newNodes.containsKey(hostId)) {
        LOG.warn(
            "[{}] Found duplicate entries with host_id {} in system.peers, "
                + "keeping only the first one",
            logPrefix,
            hostId);
      } else {
        DefaultNode newNode = new DefaultNode(nodeInfo.getEndPoint(), context);
        LOG.debug("[{}] Adding new node {}", logPrefix, newNode);
        if (tokenMapEnabled && tokenFactory == null && nodeInfo.getPartitioner() != null) {
          tokenFactory = tokenFactoryRegistry.tokenFactoryFor(nodeInfo.getPartitioner());
        }
        copyInfos(newNodeInfo, newNode, context);
        newNodes.put(hostId, newNode);
      }
    }

    ImmutableList.Builder<Object> eventsBuilder = ImmutableList.builder();
    for (DefaultNode newNode : newNodes.values()) {
      eventsBuilder.add(NodeStateEvent.added(newNode));
    }

    // This should trigger control connection reconnet using added nodes, which won't contain contact point endpoints.
    // So now driver would explicitly fail when connecting via LB w/o address translator.
    // Contact points will really become the points to just make the initial connection, then we fully switch to
    // broadcast rpc address and address translated endpoints.
    for (DefaultNode contactPoint : contactPoints) {
      eventsBuilder.add(NodeStateEvent.removed(contactPoint));
    }

    return new Result(
        oldMetadata.withNodes(
            ImmutableMap.copyOf(newNodes), tokenMapEnabled, true, tokenFactory, context),
        eventsBuilder.build());
  }
}
