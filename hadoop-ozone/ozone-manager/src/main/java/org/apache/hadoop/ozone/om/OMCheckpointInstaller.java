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

package org.apache.hadoop.ozone.om;

import static org.apache.hadoop.util.Time.monotonicNow;

import java.io.File;
import java.nio.file.Path;
import org.apache.hadoop.hdds.ExitManager;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles the installation of checkpoints for Ozone Manager.
 * This class encapsulates the complex logic of installing checkpoints
 * during HA snapshot installation.
 */
public class OMCheckpointInstaller {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMCheckpointInstaller.class);

  private final OzoneConfiguration configuration;
  private final OMMetadataManager metadataManager;
  private final OzoneManagerRatisServer omRatisServer;
  private final OmSnapshotManager omSnapshotManager;
  private final OzoneManager om;
  private final ExitManager exitManager;

  public OMCheckpointInstaller(
      OzoneConfiguration configuration,
      OMMetadataManager metadataManager,
      OzoneManagerRatisServer omRatisServer,
      OmSnapshotManager omSnapshotManager,
      OzoneManager om,
      ExitManager exitManager) {
    this.configuration = configuration;
    this.metadataManager = metadataManager;
    this.omRatisServer = omRatisServer;
    this.omSnapshotManager = omSnapshotManager;
    this.om = om;
    this.exitManager = exitManager;
  }

  /**
   * Install checkpoint from Path and TransactionInfo.
   * This is the main method that contains the core checkpoint installation logic.
   */
  public TermIndex installCheckpoint(
      String leaderId,
      Path checkpointLocation,
      TransactionInfo checkpointTrxnInfo) throws Exception {
    long startTime = monotonicNow();
    File oldDBLocation = metadataManager.getStore().getDbLocation();
    try {
      // Stop Background services
      om.getKeyManager().stop();
      om.stopSecretManager();
      om.stopTrashEmptier();
      omSnapshotManager.invalidateCache();
      omRatisServer.getOmStateMachine().pause();
    } catch (Exception e) {
      LOG.error("Failed to stop/ pause the services. Cannot proceed with " +
          "installing the new checkpoint.");
      // Stop the checkpoint install process and restart the services.
      om.getKeyManager().start(configuration);
      om.startSecretManagerIfNecessary();
      om.startTrashEmptier(configuration);
      throw e;
    }

    File dbBackup = null;
    TermIndex termIndex = omRatisServer.getLastAppliedTermIndex();
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
    long time = monotonicNow();
    if (canProceed) {
      // Stop RPC server before stop metadataManager
      if (om.getOmRpcServer() != null) {
        om.getOmRpcServer().stop();
      }
      om.setOmRpcServerRunning(false);
      omRpcServerStopped = true;
      LOG.info("RPC server is stopped. Spend " +
          (monotonicNow() - time) + " ms.");
      try {
        // Stop old metadataManager before replacing DB Dir
        time = monotonicNow();
        metadataManager.stop();
        oldOmMetadataManagerStopped = true;
        LOG.info("metadataManager is stopped. Spend " +
            (monotonicNow() - time) + " ms.");
      } catch (Exception e) {
        String errorMsg = "Failed to stop metadataManager. Cannot proceed " +
            "with installing the new checkpoint.";
        LOG.error(errorMsg);
        exitManager.exitSystem(1, errorMsg, e, LOG);
      }
      try {
        time = monotonicNow();
        dbBackup = om.replaceOMDBWithCheckpoint(lastAppliedIndex,
            oldDBLocation, checkpointLocation);
        term = checkpointTrxnInfo.getTerm();
        lastAppliedIndex = checkpointTrxnInfo.getTransactionIndex();
        LOG.info("Replaced DB with checkpoint from OM: {}, term: {}, " +
            "index: {}, time: {} ms", leaderId, term, lastAppliedIndex,
            monotonicNow() - time);
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
      omSnapshotManager.close();
    }

    // Reload the OM DB store with the new checkpoint.
    // Restart (unpause) the state machine and update its last applied index
    // to the installed checkpoint's snapshot index.
    try {
      if (oldOmMetadataManagerStopped) {
        time = monotonicNow();
        om.reloadOMState();
        om.setTransactionInfo(TransactionInfo.valueOf(termIndex));
        omRatisServer.getOmStateMachine().unpause(lastAppliedIndex, term);
        newMetadataManagerStarted = true;
        LOG.info("Reloaded OM state with Term: {} and Index: {}. Spend {} ms",
            term, lastAppliedIndex, monotonicNow() - time);
      } else {
        // OM DB is not stopped. Start the services.
        om.getKeyManager().start(configuration);
        om.startSecretManagerIfNecessary();
        om.startTrashEmptier(configuration);
        omRatisServer.getOmStateMachine().unpause(lastAppliedIndex, term);
        LOG.info("OM DB is not stopped. Started services with Term: {} and " +
            "Index: {}", term, lastAppliedIndex);
      }
    } catch (Exception ex) {
      String errorMsg = "Failed to reload OM state and instantiate services.";
      exitManager.exitSystem(1, errorMsg, ex, LOG);
    }

    if (omRpcServerStopped && newMetadataManagerStarted) {
      // Start the RPC server. RPC server start requires metadataManager
      try {
        time = monotonicNow();
        RPC.Server server = om.getRpcServer(configuration);
        om.setOmRpcServer(server);
        server.start();
        om.setOmRpcServerRunning(true);
        LOG.info("RPC server is re-started. Spend " +
            (monotonicNow() - time) + " ms.");
      } catch (Exception e) {
        String errorMsg = "Failed to start RPC Server.";
        exitManager.exitSystem(1, errorMsg, e, LOG);
      }
    }
    om.buildDBCheckpointInstallAuditLog(leaderId, term, lastAppliedIndex);

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
    TermIndex newTermIndex = TermIndex.valueOf(term, lastAppliedIndex);
    LOG.info("Install Checkpoint is finished with Term: {} and Index: {}. " +
        "Spend {} ms.", newTermIndex.getTerm(), newTermIndex.getIndex(),
        (monotonicNow() - startTime));
    return newTermIndex;
  }
}

