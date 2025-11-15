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
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.ratis.util.FileUtils;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.ratis.thirdparty.com.google.protobuf.ServiceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class responsible for installing checkpoints in OzoneManager.
 * This class encapsulates the checkpoint installation logic that was previously
 * embedded in the OzoneManager class, improving code organization and maintainability.
 */
public class OMCheckpointInstaller {

  private static final Logger LOG = LoggerFactory.getLogger(OMCheckpointInstaller.class);

  private final ServiceLifecycleManager serviceManager;

  /**
   * Constructor for OMCheckpointInstaller.
   * @param serviceManager the service lifecycle manager to use for service operations
   */
  public OMCheckpointInstaller(ServiceLifecycleManager serviceManager) {
    this.serviceManager = serviceManager;
  }

  /**
   * Install checkpoint. If the checkpoint snapshot index is greater than
   * OM's last applied transaction index, then re-initialize the OM
   * state via this checkpoint. Before re-initializing OM state, the OM Ratis
   * server should be stopped so that no new transactions can be applied.
   *
   * @param leaderId leader OM node ID
   * @param checkpointLocation path to the checkpoint
   * @param checkpointTrxnInfo transaction info from the checkpoint
   * @return TermIndex if successful, null otherwise
   * @throws Exception if installation fails
   */
  public org.apache.ratis.server.protocol.TermIndex install(String leaderId,
      Path checkpointLocation,
      TransactionInfo checkpointTrxnInfo) throws Exception {
    long startTime = org.apache.hadoop.util.Time.monotonicNow();

    // Get current state before making any changes
    File oldDBLocation = serviceManager.getOldDBLocation();

    try {
      // Stop Background services
      serviceManager.stopKeyManager();
      serviceManager.stopSecretManager();
      serviceManager.stopTrashEmptier();
      // Note: omSnapshotManager.invalidateCache() should be handled by serviceManager

      // Pause the State Machine so that no new transactions can be applied.
      // This action also clears the OM Double Buffer so that if there are any
      // pending transactions in the buffer, they are discarded.
      serviceManager.getRatisServer().getOmStateMachine().pause();
    } catch (Exception e) {
      LOG.error("Failed to stop/ pause the services. Cannot proceed with " +
          "installing the new checkpoint.");
      // Stop the checkpoint install process and restart the services.
      try {
        serviceManager.startKeyManager(serviceManager.getConfiguration());
        serviceManager.startSecretManagerIfNecessary();
        serviceManager.startTrashEmptier(serviceManager.getConfiguration());
      } catch (Exception restartEx) {
        LOG.error("Failed to restart services after initial failure", restartEx);
      }
      throw e;
    }

    File dbBackup = null;
    org.apache.ratis.server.protocol.TermIndex termIndex = serviceManager.getRatisServer().getLastAppliedTermIndex();
    long term = termIndex.getTerm();
    long lastAppliedIndex = termIndex.getIndex();

    // Check if current applied log index is smaller than the downloaded
    // checkpoint transaction index. If yes, proceed by stopping the ratis
    // server so that the OM state can be re-initialized. If no then do not
    // proceed with installSnapshot.
    boolean canProceed = OzoneManagerRatisUtils.verifyTransactionInfo(
        checkpointTrxnInfo, lastAppliedIndex, leaderId, checkpointLocation);

    boolean oldOmMetadataManagerStopped = false;
    boolean newMetadataManagerStarted = false;
    boolean omRpcServerStopped = false;
    long time = org.apache.hadoop.util.Time.monotonicNow();
    if (canProceed) {
      // Stop RPC server before stop metadataManager
      serviceManager.stopRpcServer();
      omRpcServerStopped = true;
      LOG.info("RPC server is stopped. Spend " +
          (org.apache.hadoop.util.Time.monotonicNow() - time) + " ms.");
      try {
        // Stop old metadataManager before replacing DB Dir
        time = org.apache.hadoop.util.Time.monotonicNow();
        serviceManager.stopMetadataManager();
        oldOmMetadataManagerStopped = true;
        LOG.info("metadataManager is stopped. Spend " +
            (org.apache.hadoop.util.Time.monotonicNow() - time) + " ms.");
      } catch (Exception e) {
        String errorMsg = "Failed to stop metadataManager. Cannot proceed " +
            "with installing the new checkpoint.";
        LOG.error(errorMsg);
        // Note: exitManager.exitSystem needs to be handled
        throw new RuntimeException(errorMsg, e);
      }
      try {
        time = org.apache.hadoop.util.Time.monotonicNow();
        dbBackup = serviceManager.replaceOMDBWithCheckpoint(lastAppliedIndex,
            oldDBLocation, checkpointLocation);
        term = checkpointTrxnInfo.getTerm();
        lastAppliedIndex = checkpointTrxnInfo.getTransactionIndex();
        LOG.info("Replaced DB with checkpoint from OM: {}, term: {}, " +
            "index: {}, time: {} ms", leaderId, term, lastAppliedIndex,
            org.apache.hadoop.util.Time.monotonicNow() - time);
      } catch (Exception e) {
        LOG.error("Failed to install Snapshot from {} as OM failed to replace" +
            " DB with downloaded checkpoint. Reloading old OM state.",
            leaderId, e);
      }
    } else {
      LOG.warn("Cannot proceed with InstallSnapshot as OM is at TermIndex {} " +
          "and checkpoint has lower TermIndex {}. Reloading old state of OM.",
          termIndex, checkpointTrxnInfo.getTermIndex());
    }

    if (oldOmMetadataManagerStopped) {
      // Close snapDiff's rocksDB instance only if metadataManager gets closed.
      serviceManager.closeSnapshotManager();
    }

    // Reload the OM DB store with the new checkpoint.
    // Restart (unpause) the state machine and update its last applied index
    // to the installed checkpoint's snapshot index.
    try {
      if (oldOmMetadataManagerStopped) {
        time = org.apache.hadoop.util.Time.monotonicNow();
        serviceManager.reloadOMState();
        serviceManager.setTransactionInfo(TransactionInfo.valueOf(termIndex));
        serviceManager.getRatisServer().getOmStateMachine().unpause(lastAppliedIndex, term);
        newMetadataManagerStarted = true;
        LOG.info("Reloaded OM state with Term: {} and Index: {}. Spend {} ms",
            term, lastAppliedIndex, org.apache.hadoop.util.Time.monotonicNow() - time);
      } else {
        // OM DB is not stopped. Start the services.
        serviceManager.startKeyManager(serviceManager.getConfiguration());
        serviceManager.startSecretManagerIfNecessary();
        serviceManager.startTrashEmptier(serviceManager.getConfiguration());
        serviceManager.getRatisServer().getOmStateMachine().unpause(lastAppliedIndex, term);
        LOG.info("OM DB is not stopped. Started services with Term: {} and " +
            "Index: {}", term, lastAppliedIndex);
      }
    } catch (Exception ex) {
      String errorMsg = "Failed to reload OM state and instantiate services.";
      // Note: exitManager.exitSystem needs to be handled
      throw new RuntimeException(errorMsg, ex);
    }

    if (omRpcServerStopped && newMetadataManagerStarted) {
      // Start the RPC server. RPC server start requires metadataManager
      try {
        time = org.apache.hadoop.util.Time.monotonicNow();
        serviceManager.startRpcServer();
        LOG.info("RPC server is re-started. Spend " +
            (org.apache.hadoop.util.Time.monotonicNow() - time) + " ms.");
      } catch (Exception e) {
        String errorMsg = "Failed to start RPC Server.";
        // Note: exitManager.exitSystem needs to be handled
        throw new RuntimeException(errorMsg, e);
      }
    }
    serviceManager.logCheckpointInstallAudit(leaderId, term, lastAppliedIndex);

    // Delete the backup DB
    try {
      if (dbBackup != null) {
        FileUtils.deleteFully(dbBackup);
      }
    } catch (Exception e) {
      LOG.error("Failed to delete the backup of the original DB {}",
          dbBackup, e);
    }

    if (lastAppliedIndex != checkpointTrxnInfo.getTransactionIndex()) {
      // Install Snapshot failed and old state was reloaded. Return null to
      // Ratis to indicate that installation failed.
      return null;
    }

    // TODO: We should only return the snpashotIndex to the leader.
    //  Should be fixed after RATIS-586
    org.apache.ratis.server.protocol.TermIndex newTermIndex = org.apache.ratis.server.protocol.TermIndex.valueOf(term, lastAppliedIndex);
    LOG.info("Install Checkpoint is finished with Term: {} and Index: {}. " +
        "Spend {} ms.", newTermIndex.getTerm(), newTermIndex.getIndex(),
        (org.apache.hadoop.util.Time.monotonicNow() - startTime));
    return newTermIndex;
  }

}
