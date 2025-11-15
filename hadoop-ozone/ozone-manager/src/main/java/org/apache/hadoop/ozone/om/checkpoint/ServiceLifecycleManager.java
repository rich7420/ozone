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

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.ratis.server.protocol.TermIndex;

/**
 * Abstraction over the lifecycle operations required for installing an OM
 * checkpoint. This allows the checkpoint installer to live outside
 * {@link org.apache.hadoop.ozone.om.OzoneManager} while keeping its behaviour
 * identical to the original implementation.
 */
public interface ServiceLifecycleManager {

  File getOldDBLocation();

  void stopKeyManager() throws Exception;

  void startKeyManager() throws Exception;

  void stopSecretManager();

  void startSecretManagerIfNecessary();

  void stopTrashEmptier();

  void startTrashEmptier() throws Exception;

  void invalidateSnapshotCache();

  void stopMetadataManager() throws Exception;

  void reloadOMState() throws IOException;

  void closeSnapshotManager();

  File replaceOMDBWithCheckpoint(long lastAppliedIndex, File oldDBLocation,
      Path checkpointLocation) throws Exception;

  void stopRpcServer();

  void startRpcServer() throws Exception;

  void setOmRpcServerRunning(boolean running);

  OzoneManagerRatisServer getRatisServer();

  void setTransactionInfo(TransactionInfo info);

  void buildDBCheckpointInstallAuditLog(String leaderId, long term,
      long lastAppliedIndex);

  void logCheckpointCannotProceedWarning(TermIndex currentTermIndex,
      TermIndex checkpointTermIndex);

  void logRpcServerStopped(long durationMs);

  void logMetadataManagerStopped(long durationMs);

  void logReloadedOmState(long term, long lastAppliedIndex, long durationMs);

  void logOmDbNotStopped(long term, long lastAppliedIndex);

  void logRpcServerRestarted(long durationMs);

  void logInstallCheckpointCompletion(TermIndex termIndex, long durationMs);

  OzoneConfiguration getConfiguration();

  void failCheckpointInstall(String message, Throwable cause);
}

