/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.transport;

import com.google.common.base.Functions;
import com.google.common.collect.Iterables;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.transport.TransportService;
import org.junit.Test;

import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import static org.hamcrest.Matchers.*;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class TransportClientTests extends ElasticsearchIntegrationTest {

    @Test
    public void testPickingUpChangesInDiscoveryNode() {
        String nodeName = internalCluster().startNode();

        TransportClient client = (TransportClient) internalCluster().transportClient();
        assertThat(client.connectedNodes(), hasSize(greaterThanOrEqualTo(1)));
        Iterable<String> nodeNames = Iterables.transform(client.connectedNodes(), Functions.toStringFunction());
        assertThat(nodeNames, hasItem(containsString(nodeName)));
    }

    @Test
    public void testNodeVersionIsUpdated() {
        TransportClient client = (TransportClient) internalCluster().transportClient();
        TransportClientNodesService nodeService = client.nodeService();
        Node node = NodeBuilder.nodeBuilder().data(false).settings(ImmutableSettings.builder()
                .put(internalCluster().getDefaultSettings())
                .put("http.enabled", false)
                .put("index.store.type", "ram")
                .put("config.ignore_system_properties", true) // make sure we get what we set :)
                .put("gateway.type", "none")
                .build()).clusterName(randomAsciiOfLength(10)).build();
        node.start();
        try {
            TransportAddress transportAddress = ((InternalNode) node).injector().getInstance(TransportService.class).boundAddress().publishAddress();
            client.addTransportAddress(transportAddress);
            assertThat(nodeService.connectedNodes().size(), greaterThanOrEqualTo(1)); // since we force transport clients there has to be one node started that we connect to.
            for (DiscoveryNode discoveryNode : nodeService.connectedNodes()) {  // connected nodes have updated version
                assertThat(discoveryNode.getVersion(), equalTo(Version.CURRENT));
            }

            for (DiscoveryNode discoveryNode : nodeService.listedNodes()) {
                assertThat(discoveryNode.id(), startsWith("#transport#-"));
                assertThat(discoveryNode.getVersion(), equalTo(Version.CURRENT.minimumCompatibilityVersion()));
            }

            assertThat(nodeService.filteredNodes().size(), equalTo(1));
            for (DiscoveryNode discoveryNode : nodeService.filteredNodes()) {
                assertThat(discoveryNode.getVersion(), equalTo(Version.CURRENT.minimumCompatibilityVersion()));
            }
        } finally {
            node.stop();
        }
    }
}
