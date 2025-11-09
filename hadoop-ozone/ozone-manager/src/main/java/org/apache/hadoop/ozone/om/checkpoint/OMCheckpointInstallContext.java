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

package org.apache.hadoop.ozone.om.checkpoint;

import org.apache.hadoop.hdds.ExitManager;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.KeyManager;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ipc.RPC;

/**
 * Context object that encapsulates all dependencies needed for checkpoint
 * installation. This follows the Context Object Pattern to avoid passing
 * too many parameters and to centralize dependency management.
 */
public class OMCheckpointInstallContext {
  private final OMMetadataManager metadataManager;
  private final KeyManager keyManager;
  private final OmSnapshotManager omSnapshotManager;
  private final OzoneManagerRatisServer omRatisServer;
  private final RPC.Server omRpcServer;
  private final OzoneConfiguration configuration;
  private final ExitManager exitManager;
  private final ServiceLifecycleManager serviceLifecycleManager;

  private OMCheckpointInstallContext(Builder builder) {
    this.metadataManager = builder.metadataManager;
    this.keyManager = builder.keyManager;
    this.omSnapshotManager = builder.omSnapshotManager;
    this.omRatisServer = builder.omRatisServer;
    this.omRpcServer = builder.omRpcServer;
    this.configuration = builder.configuration;
    this.exitManager = builder.exitManager;
    this.serviceLifecycleManager = builder.serviceLifecycleManager;
  }

  public OMMetadataManager getMetadataManager() {
    return metadataManager;
  }

  public KeyManager getKeyManager() {
    return keyManager;
  }

  public OmSnapshotManager getOmSnapshotManager() {
    return omSnapshotManager;
  }

  public OzoneManagerRatisServer getOmRatisServer() {
    return omRatisServer;
  }

  public RPC.Server getOmRpcServer() {
    return omRpcServer;
  }

  public OzoneConfiguration getConfiguration() {
    return configuration;
  }

  public ExitManager getExitManager() {
    return exitManager;
  }

  public ServiceLifecycleManager getServiceLifecycleManager() {
    return serviceLifecycleManager;
  }

  public static class Builder {
    private OMMetadataManager metadataManager;
    private KeyManager keyManager;
    private OmSnapshotManager omSnapshotManager;
    private OzoneManagerRatisServer omRatisServer;
    private RPC.Server omRpcServer;
    private OzoneConfiguration configuration;
    private ExitManager exitManager;
    private ServiceLifecycleManager serviceLifecycleManager;

    public Builder setMetadataManager(OMMetadataManager metadataManager) {
      this.metadataManager = metadataManager;
      return this;
    }

    public Builder setKeyManager(KeyManager keyManager) {
      this.keyManager = keyManager;
      return this;
    }

    public Builder setOmSnapshotManager(OmSnapshotManager omSnapshotManager) {
      this.omSnapshotManager = omSnapshotManager;
      return this;
    }

    public Builder setOmRatisServer(OzoneManagerRatisServer omRatisServer) {
      this.omRatisServer = omRatisServer;
      return this;
    }

    public Builder setOmRpcServer(RPC.Server omRpcServer) {
      this.omRpcServer = omRpcServer;
      return this;
    }

    public Builder setConfiguration(OzoneConfiguration configuration) {
      this.configuration = configuration;
      return this;
    }

    public Builder setExitManager(ExitManager exitManager) {
      this.exitManager = exitManager;
      return this;
    }

    public Builder setServiceLifecycleManager(
        ServiceLifecycleManager serviceLifecycleManager) {
      this.serviceLifecycleManager = serviceLifecycleManager;
      return this;
    }

    public OMCheckpointInstallContext build() {
      return new OMCheckpointInstallContext(this);
    }
  }
}


