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

import java.io.IOException;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;

/**
 * Interface for managing service lifecycle during checkpoint installation.
 * This interface abstracts the operations needed to start/stop services
 * and manage state during checkpoint installation, allowing for better
 * testability and separation of concerns.
 */
public interface ServiceLifecycleManager {

  /**
   * Stop the KeyManager service.
   * @throws Exception if stopping fails
   */
  void stopKeyManager() throws Exception;

  /**
   * Start the KeyManager service.
   * @param conf OzoneConfiguration
   * @throws Exception if starting fails
   */
  void startKeyManager(OzoneConfiguration conf) throws Exception;

  /**
   * Stop the SecretManager service.
   * @throws Exception if stopping fails
   */
  void stopSecretManager() throws Exception;

  /**
   * Start the SecretManager service if necessary.
   * @throws Exception if starting fails
   */
  void startSecretManagerIfNecessary() throws Exception;

  /**
   * Stop the TrashEmptier service.
   */
  void stopTrashEmptier();

  /**
   * Start the TrashEmptier service.
   * @param conf OzoneConfiguration
   */
  void startTrashEmptier(OzoneConfiguration conf);

  /**
   * Reload the OM state from the new checkpoint.
   * @throws IOException if reloading fails
   */
  void reloadOMState() throws IOException;

  /**
   * Set the transaction info.
   * @param txnInfo TransactionInfo to set
   */
  void setTransactionInfo(TransactionInfo txnInfo);

  /**
   * Create a new RPC server instance.
   * @param conf OzoneConfiguration
   * @return RPC.Server instance
   * @throws IOException if creation fails
   */
  RPC.Server createNewRpcServer(OzoneConfiguration conf) throws IOException;

  /**
   * Log checkpoint installation audit information.
   * @param leaderId leader OM ID
   * @param term term number
   * @param index transaction index
   */
  void logCheckpointInstallAudit(String leaderId, long term, long index);

  /**
   * Get the Ratis server instance.
   * @return OzoneManagerRatisServer
   */
  OzoneManagerRatisServer getRatisServer();

  /**
   * Replace the OM DB with checkpoint.
   * @param lastAppliedIndex last applied transaction index
   * @param oldDB old database location
   * @param checkpointPath checkpoint path
   * @return backup directory location
   * @throws IOException if replacement fails
   */
  java.io.File replaceOMDBWithCheckpoint(long lastAppliedIndex,
      java.io.File oldDB, java.nio.file.Path checkpointPath)
      throws IOException;
}


