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

package org.apache.flink.kubernetes.operator.utils;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.runtime.dispatcher.NoOpJobGraphListener;
import org.apache.flink.runtime.highavailability.zookeeper.CuratorFrameworkWithUnhandledErrorListener;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.runtime.testutils.ZooKeeperTestUtils;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.runtime.util.ZooKeeperUtils;

import org.apache.flink.shaded.curator5.org.apache.curator.framework.CuratorFramework;

import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.rules.TemporaryFolder;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for Zookeeper functions in FlinkUtils. */
public class FlinkUtilsZookeeperHATest {

    Configuration configuration;
    TestingServer testingServer;
    TemporaryFolder temporaryFolder;
    CuratorFramework curator;
    JobID jobID = JobID.generate();

    public CuratorFrameworkWithUnhandledErrorListener getTestCurator(Configuration configuration) {
        return ZooKeeperUtils.startCuratorFramework(configuration, new TestingFatalErrorHandler());
    }

    @BeforeEach
    public void setupZookeeper() throws Exception {
        // Start a ZK server
        testingServer = ZooKeeperTestUtils.createAndStartZookeeperTestingServer();

        // Generate configuration for the Curator
        configuration = new Configuration();
        configuration.setString(
                HighAvailabilityOptions.HA_MODE, HighAvailabilityMode.ZOOKEEPER.toString());
        configuration.setString(
                HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, testingServer.getConnectString());
        temporaryFolder = new TemporaryFolder();
        temporaryFolder.create();
        configuration.setString(
                HighAvailabilityOptions.HA_STORAGE_PATH,
                temporaryFolder.newFolder().getAbsolutePath());

        // Create the Curator
        curator = getTestCurator(configuration).asCuratorFramework();

        // Populate the HA metadata with a test JobGraph
        var jobGraphStore = ZooKeeperUtils.createJobGraphs(curator, configuration);
        jobGraphStore.start(NoOpJobGraphListener.INSTANCE);
        var jobGraph = JobGraphTestUtils.emptyJobGraph();
        jobGraph.setJobID(jobID);
        jobGraphStore.putJobGraph(jobGraph);
        jobGraphStore.stop();

        // Create jobs znode
        curator.create().forPath(ZooKeeperUtils.getJobsPath());
        curator.create().forPath(ZooKeeperUtils.getLeaderPathForJob(new JobID()));
    }

    @AfterEach
    public void cleanupZookeeper() throws Exception {
        curator.close();
        testingServer.close();
        temporaryFolder.delete();
    }

    @Test
    public void testDeleteZookeeperHAMetadata() throws Exception {
        // First verify that the HA metadata path exists and is not empty
        assertNotNull(curator.checkExists().forPath("/"));
        assertTrue(curator.getChildren().forPath("/").size() != 0);

        // Now delete all data
        FlinkUtils.deleteZookeeperHAMetadata(configuration);

        // Verify that the root path doesn't exist anymore
        assertNull(curator.checkExists().forPath("/"));
    }

    @Test
    public void testDeleteJobGraphInZookeeperHA() throws Exception {
        // First verify that the JobGraph exists in ZK for the test JobID
        var jobGraphPath = configuration.get(HighAvailabilityOptions.HA_ZOOKEEPER_JOBGRAPHS_PATH);
        assertEquals(List.of(jobID.toString()), curator.getChildren().forPath(jobGraphPath));

        // Now delete the JobGraph
        FlinkUtils.deleteJobGraphInZookeeperHA(configuration);

        // Verify the JobGraph path doesn't exist anymore
        assertNull(curator.checkExists().forPath(jobGraphPath));
    }

    @Test
    public void zookeeperHaMetaDataCheckTest() throws Exception {
        // Verify that the HA metadata exists since it was created in setupZookeeper()
        assertTrue(FlinkUtils.isZookeeperHaMetadataAvailable(configuration));

        // Now delete all data
        curator.delete().deletingChildrenIfNeeded().forPath("/");

        // Verify that the HA metadata no longer exists
        assertFalse(FlinkUtils.isZookeeperHaMetadataAvailable(configuration));
    }
}
