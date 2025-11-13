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
import java.nio.file.Path;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Responsible for executing checkpoint installation logic.
 * This class uses the Template Method Pattern to define the standard
 * installation flow, making it easier to understand and maintain.
 *
 * The installation process consists of the following phases:
 * 1. Prepare: Stop background services and pause state machine
 * 2. Verify: Check if checkpoint can be installed
 * 3. Stop Services: Stop RPC server and metadata manager
 * 4. Replace Database: Replace old DB with checkpoint
 * 5. Restart Services: Reload state and restart services
 * 6. Cleanup: Delete backup and log audit information
 */
public class OMCheckpointInstaller {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMCheckpointInstaller.class);

  private final OMCheckpointInstallContext context;
  private final ServiceLifecycleManager serviceManager;

  public OMCheckpointInstaller(
      OMCheckpointInstallContext context,
      ServiceLifecycleManager serviceManager) {
    this.context = context;
    this.serviceManager = serviceManager;
  }

  /**
   * Main entry point: Install checkpoint.
   * This is a Template Method that defines the standard installation flow.
   *
   * @param leaderId leader OM ID
   * @param checkpointLocation checkpoint location path
   * @param checkpointTrxnInfo transaction info from checkpoint
   * @return TermIndex if successful, null otherwise
   * @throws Exception if installation fails
   */
  public TermIndex install(String leaderId,
                           Path checkpointLocation,
                           TransactionInfo checkpointTrxnInfo) throws Exception {
    long startTime = org.apache.hadoop.util.Time.monotonicNow();
    CheckpointInstallSession session = new CheckpointInstallSession();

    try {
      // Phase 1: Prepare for installation
      prepareForInstallation(session);

      // Phase 2: Verify checkpoint
      TermIndex oldTermIndex = serviceManager.getRatisServer()
          .getLastAppliedTermIndex();
      long term = oldTermIndex.getTerm();
      long lastAppliedIndex = oldTermIndex.getIndex();
      session.setTerm(term);
      session.setLastAppliedIndex(lastAppliedIndex);
      session.setOldTermIndex(oldTermIndex);

      boolean canProceed = verifyCheckpoint(
          checkpointTrxnInfo, lastAppliedIndex, leaderId,
          checkpointLocation, session);

      if (!canProceed) {
        return rollbackAndReturn(session, null);
      }

      // Phase 3: Stop services
      stopServices(leaderId, session);

      // Phase 4: Replace database
      replaceDatabase(leaderId, checkpointLocation, checkpointTrxnInfo,
          session);

      // Phase 5: Restart services
      // Note: Don't pass termIndex here, as session now has updated values
      restartServices(session);

      // Phase 6: Cleanup
      cleanup(leaderId, session);

      // Verify final state
      if (session.getLastAppliedIndex() !=
          checkpointTrxnInfo.getTransactionIndex()) {
        LOG.warn("Install Snapshot failed and old state was reloaded. " +
            "Returning null to Ratis to indicate installation failed.");
        return null;
      }

      TermIndex newTermIndex = TermIndex.valueOf(
          session.getTerm(), session.getLastAppliedIndex());
      LOG.info("Install Checkpoint is finished with Term: {} and Index: {}. " +
              "Spend {} ms.", newTermIndex.getTerm(), newTermIndex.getIndex(),
          org.apache.hadoop.util.Time.monotonicNow() - startTime);
      return newTermIndex;

    } catch (Exception e) {
      LOG.error("Checkpoint installation failed", e);
      return handleFailure(session, e);
    }
  }

  /**
   * Phase 1: Prepare for installation by stopping background services
   * and pausing the state machine.
   */
  private void prepareForInstallation(CheckpointInstallSession session)
      throws Exception {
    File oldDBLocation = context.getMetadataManager().getStore()
        .getDbLocation();
    session.setOldDBLocation(oldDBLocation);

    try {
      // Stop background services
      serviceManager.stopKeyManager();
      serviceManager.stopSecretManager();
      serviceManager.stopTrashEmptier();
      context.getOmSnapshotManager().invalidateCache();

      // Pause the State Machine so that no new transactions can be applied.
      // This action also clears the OM Double Buffer so that if there are any
      // pending transactions in the buffer, they are discarded.
      serviceManager.getRatisServer().getOmStateMachine().pause();

      session.markCompleted(
          CheckpointInstallSession.InstallationPhase.BACKGROUND_SERVICES_STOPPED);
      session.markCompleted(
          CheckpointInstallSession.InstallationPhase.STATE_MACHINE_PAUSED);

    } catch (Exception e) {
      LOG.error("Failed to stop/ pause the services. Cannot proceed with " +
          "installing the new checkpoint.");
      // Rollback: restart the services
      serviceManager.startKeyManager(context.getConfiguration());
      serviceManager.startSecretManagerIfNecessary();
      serviceManager.startTrashEmptier(context.getConfiguration());
      throw e;
    }
  }

  /**
   * Phase 2: Verify if checkpoint can be installed.
   */
  private boolean verifyCheckpoint(TransactionInfo checkpointTrxnInfo,
                                    long lastAppliedIndex,
                                    String leaderId,
                                    Path checkpointLocation,
                                    CheckpointInstallSession session) {
    boolean canProceed = OzoneManagerRatisUtils.verifyTransactionInfo(
        checkpointTrxnInfo, lastAppliedIndex, leaderId, checkpointLocation);

    if (!canProceed) {
      TermIndex termIndex = serviceManager.getRatisServer()
          .getLastAppliedTermIndex();
      LOG.warn("Cannot proceed with InstallSnapshot as OM is at " +
          "TermIndex {} and checkpoint has lower TermIndex {}. " +
          "Reloading old state of OM.",
          termIndex, checkpointTrxnInfo.getTermIndex());
    }

    return canProceed;
  }

  /**
   * Phase 3: Stop RPC server and metadata manager.
   */
  private void stopServices(String leaderId,
                            CheckpointInstallSession session) throws Exception {
    long time = org.apache.hadoop.util.Time.monotonicNow();

    // Stop RPC server before stopping metadataManager
    context.getOmRpcServer().stop();
    serviceManager.setOmRpcServerRunning(false);
    session.markCompleted(
        CheckpointInstallSession.InstallationPhase.RPC_SERVER_STOPPED);
    LOG.info("RPC server is stopped. Spend {} ms.",
        org.apache.hadoop.util.Time.monotonicNow() - time);

    try {
      // Stop old metadataManager before replacing DB Dir
      time = org.apache.hadoop.util.Time.monotonicNow();
      context.getMetadataManager().stop();
      session.markCompleted(
          CheckpointInstallSession.InstallationPhase.METADATA_MANAGER_STOPPED);
      LOG.info("metadataManager is stopped. Spend {} ms.",
          org.apache.hadoop.util.Time.monotonicNow() - time);
    } catch (Exception e) {
      String errorMsg = "Failed to stop metadataManager. Cannot proceed " +
          "with installing the new checkpoint.";
      LOG.error(errorMsg);
      context.getExitManager().exitSystem(1, errorMsg, e, LOG);
    }
  }

  /**
   * Phase 4: Replace the database with checkpoint.
   */
  private void replaceDatabase(String leaderId,
                                Path checkpointLocation,
                                TransactionInfo checkpointTrxnInfo,
                                CheckpointInstallSession session)
      throws Exception {
    long time = org.apache.hadoop.util.Time.monotonicNow();
    try {
      File dbBackup = serviceManager.replaceOMDBWithCheckpoint(
          session.getLastAppliedIndex(),
          session.getOldDBLocation(),
          checkpointLocation);

      session.setDbBackup(dbBackup);
      session.setTerm(checkpointTrxnInfo.getTerm());
      session.setLastAppliedIndex(
          checkpointTrxnInfo.getTransactionIndex());

      session.markCompleted(
          CheckpointInstallSession.InstallationPhase.DATABASE_REPLACED);

      LOG.info("Replaced DB with checkpoint from OM: {}, term: {}, " +
          "index: {}, time: {} ms", leaderId, session.getTerm(),
          session.getLastAppliedIndex(),
          org.apache.hadoop.util.Time.monotonicNow() - time);
    } catch (Exception e) {
      LOG.error("Failed to install Snapshot from {} as OM failed to replace" +
          " DB with downloaded checkpoint. Reloading old OM state.",
          leaderId, e);
      throw e;
    }
  }

  /**
   * Phase 5: Restart services and reload state.
   */
  private void restartServices(CheckpointInstallSession session)
      throws Exception {
    // Close snapDiff's rocksDB instance only if metadataManager was closed
    if (session.isMetadataManagerStopped()) {
      context.getOmSnapshotManager().close();
      session.markCompleted(
          CheckpointInstallSession.InstallationPhase.SNAPSHOT_MANAGER_CLOSED);
    }

    long time = org.apache.hadoop.util.Time.monotonicNow();
    try {
      if (session.isMetadataManagerStopped()) {
        // Reload the OM DB store with the new checkpoint
        serviceManager.reloadOMState();
        // Use the OLD termIndex (before DB replacement) to match original behavior
        // This is the behavior from the original code - using termIndex from before
        // the checkpoint installation, not the checkpoint's term/index
        serviceManager.setTransactionInfo(
            TransactionInfo.valueOf(session.getOldTermIndex()));
        serviceManager.getRatisServer().getOmStateMachine().unpause(
            session.getLastAppliedIndex(), session.getTerm());

        session.markCompleted(
            CheckpointInstallSession.InstallationPhase.STATE_RELOADED);

        LOG.info("Reloaded OM state with Term: {} and Index: {}. Spend {} ms",
            session.getTerm(), session.getLastAppliedIndex(),
            org.apache.hadoop.util.Time.monotonicNow() - time);
      } else {
        // OM DB is not stopped. Start the services.
        serviceManager.startKeyManager(context.getConfiguration());
        serviceManager.startSecretManagerIfNecessary();
        serviceManager.startTrashEmptier(context.getConfiguration());
        serviceManager.getRatisServer().getOmStateMachine().unpause(
            session.getLastAppliedIndex(), session.getTerm());
        LOG.info("OM DB is not stopped. Started services with Term: {} and " +
            "Index: {}", session.getTerm(), session.getLastAppliedIndex());
      }
    } catch (Exception ex) {
      String errorMsg = "Failed to reload OM state and instantiate services.";
      context.getExitManager().exitSystem(1, errorMsg, ex, LOG);
    }

    // Start RPC server if it was stopped and state was reloaded
    if (session.isRpcServerStopped() && session.isStateReloaded()) {
      try {
        time = org.apache.hadoop.util.Time.monotonicNow();
        RPC.Server newRpcServer = serviceManager.createNewRpcServer(
            context.getConfiguration());
        newRpcServer.start();
        serviceManager.setOmRpcServerRunning(true);
        session.markCompleted(
            CheckpointInstallSession.InstallationPhase.RPC_SERVER_RESTARTED);
        LOG.info("RPC server is re-started. Spend {} ms.",
            org.apache.hadoop.util.Time.monotonicNow() - time);
      } catch (Exception e) {
        String errorMsg = "Failed to start RPC Server.";
        context.getExitManager().exitSystem(1, errorMsg, e, LOG);
      }
    }
  }

  /**
   * Phase 6: Cleanup - delete backup and log audit information.
   */
  private void cleanup(String leaderId, CheckpointInstallSession session) {
    // Log audit information
    serviceManager.logCheckpointInstallAudit(
        leaderId, session.getTerm(), session.getLastAppliedIndex());

    // Delete the backup DB
    try {
      if (session.getDbBackup() != null) {
        FileUtils.deleteFully(session.getDbBackup());
      }
    } catch (Exception e) {
      LOG.error("Failed to delete the backup of the original DB {}",
          session.getDbBackup(), e);
    }
  }

  /**
   * Handle rollback when checkpoint cannot be installed.
   */
  private TermIndex rollbackAndReturn(CheckpointInstallSession session,
                                       TermIndex result) {
    // Restart services that were stopped
    try {
      serviceManager.startKeyManager(context.getConfiguration());
      serviceManager.startSecretManagerIfNecessary();
      serviceManager.startTrashEmptier(context.getConfiguration());
      serviceManager.getRatisServer().getOmStateMachine().unpause(
          session.getLastAppliedIndex(), session.getTerm());
    } catch (Exception e) {
      LOG.error("Failed to restart services during rollback", e);
    }
    return result;
  }

  /**
   * Handle failure during installation.
   */
  private TermIndex handleFailure(CheckpointInstallSession session,
                                   Exception e) throws Exception {
    // Attempt to rollback
    rollbackAndReturn(session, null);
    throw e;
  }
}

