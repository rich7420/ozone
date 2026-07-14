/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.service;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Verifies that an OM-internal write request (no RPC/gRPC user, no UserInfo)
 * still succeeds after HDDS-15467 removed the OM-starter-user fallback.
 *
 * <p>The quota repair task is the canonical "OM itself logic" path: it builds
 * a QuotaRepair OMRequest without any UserInfo and submits it straight to the
 * Ratis server, which bypasses {@code preExecute()} (and therefore
 * {@code createUGIForApi()}). This test drives that path end-to-end on a live,
 * ACL-enabled OM and asserts it does not fail with UNAUTHORIZED.
 */
public class TestQuotaRepairInternalUgi {

  private MiniOzoneCluster cluster;
  private OzoneClient client;

  @BeforeEach
  void setup() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OZONE_ACL_ENABLED, true);
    cluster = MiniOzoneCluster.newBuilder(conf).build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();
  }

  @AfterEach
  void teardown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  void internalQuotaRepairSubmitSucceedsWithoutUserInfo() throws Exception {
    // Create some data so there is quota usage to recompute.
    OzoneVolume volume;
    client.getObjectStore().createVolume("vol1");
    volume = client.getObjectStore().getVolume("vol1");
    volume.createBucket("bucket1");
    OzoneBucket bucket = volume.getBucket("bucket1");
    byte[] data = "hello-quota".getBytes(UTF_8);
    try (OzoneOutputStream out = bucket.createKey("key1", data.length)) {
      out.write(data);
    }

    OzoneManager om = cluster.getOzoneManager();
    boolean repaired = new QuotaRepairTask(om).repair().get(60, TimeUnit.SECONDS);
    assertTrue(repaired,
        "OM-internal quota repair write should succeed without a per-user UGI; status="
            + QuotaRepairTask.getStatus());
  }
}
