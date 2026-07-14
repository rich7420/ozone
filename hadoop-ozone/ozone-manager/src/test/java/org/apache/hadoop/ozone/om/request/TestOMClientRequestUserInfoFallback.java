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

package org.apache.hadoop.ozone.om.request;

import static org.apache.hadoop.ozone.om.request.OMRequestTestUtils.newBucketInfoBuilder;
import static org.apache.hadoop.ozone.om.request.OMRequestTestUtils.newCreateBucketRequest;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.UUID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.StorageTypeProto;
import org.apache.hadoop.ipc_.Server;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.request.bucket.OMBucketCreateRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BucketInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.UserInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/**
 * Tests {@link OMClientRequest#getUserIfNotExists} identity resolution
 * (HDDS-15467): an internal {@code doAs()} caller is honored, but the OM
 * starter user is never used as a fallback identity.
 */
public class TestOMClientRequestUserInfoFallback {

  private static final String OM_STARTER = "om-starter-user";

  private OMRequest newBucketRequest(UserInfo userInfo) {
    BucketInfo.Builder bucketInfo = newBucketInfoBuilder(
        UUID.randomUUID().toString(), UUID.randomUUID().toString())
        .setIsVersionEnabled(true)
        .setStorageType(StorageTypeProto.DISK);
    OMRequest.Builder builder = newCreateBucketRequest(bucketInfo);
    if (userInfo != null) {
      builder.setUserInfo(userInfo);
    }
    return builder.build();
  }

  private OzoneManager mockOzoneManager(String starterUser) {
    OzoneManager om = mock(OzoneManager.class);
    when(om.getOmStarterUser()).thenReturn(starterUser);
    when(om.getOmRpcServerAddr())
        .thenReturn(new InetSocketAddress("localhost", 9862));
    return om;
  }

  /**
   * No RPC/gRPC context and no UserInfo, and the current user is the OM starter
   * user: the identity must be left unset so createUGI() fails closed rather
   * than silently escalating to the OM admin.
   */
  @Test
  public void noFallbackToStarterUser() throws Exception {
    try (MockedStatic<Server> mockedServer = mockStatic(Server.class)) {
      mockedServer.when(Server::getRemoteUser).thenReturn(null);
      mockedServer.when(Server::getRemoteIp).thenReturn(null);

      // Make the current (test-runner) user look like the OM starter user.
      String currentUser =
          UserGroupInformation.getCurrentUser().getShortUserName();
      OzoneManager om = mockOzoneManager(currentUser);

      OMRequest omRequest = newBucketRequest(null);
      OMClientRequest request = new OMBucketCreateRequest(omRequest);

      UserInfo userInfo = request.getUserIfNotExists(om);
      assertFalse(userInfo.hasUserName());

      OMClientRequest withUserInfo = new OMBucketCreateRequest(
          omRequest.toBuilder().setUserInfo(userInfo).build());
      assertThrows(AuthenticationException.class, withUserInfo::createUGI);
    }
  }

  /**
   * An internal doAs() call runs as a real (non-starter) caller; that identity
   * must be captured from getCurrentUser().
   */
  @Test
  public void doAsCallerIdentityIsHonored() throws Exception {
    try (MockedStatic<Server> mockedServer = mockStatic(Server.class)) {
      mockedServer.when(Server::getRemoteUser).thenReturn(null);
      mockedServer.when(Server::getRemoteIp).thenReturn(null);

      OzoneManager om = mockOzoneManager(OM_STARTER);
      OMClientRequest request =
          new OMBucketCreateRequest(newBucketRequest(null));

      UserGroupInformation caller =
          UserGroupInformation.createRemoteUser("trash-caller");
      UserInfo userInfo = caller.doAs(
          (PrivilegedExceptionAction<UserInfo>)
              () -> request.getUserIfNotExists(om));

      assertEquals("trash-caller", userInfo.getUserName());
      // The address is still filled from the OM node.
      assertTrue(userInfo.hasRemoteAddress());
    }
  }

  /**
   * A request that already carries its own user name (e.g. the Trash emptier,
   * which runs as the OM login user) keeps that identity; the missing address
   * is filled from the OM node, matching the pre-existing behavior.
   */
  @Test
  public void suppliedUserNameIsPreservedAndAddressFilled() throws Exception {
    try (MockedStatic<Server> mockedServer = mockStatic(Server.class)) {
      mockedServer.when(Server::getRemoteUser).thenReturn(null);
      mockedServer.when(Server::getRemoteIp).thenReturn(null);

      OzoneManager om = mockOzoneManager(OM_STARTER);

      UserInfo supplied = UserInfo.newBuilder()
          .setUserName("trash-service-user")
          .build();
      OMClientRequest request =
          new OMBucketCreateRequest(newBucketRequest(supplied));

      UserInfo result = request.getUserIfNotExists(om);
      assertEquals("trash-service-user", result.getUserName());
      assertTrue(result.hasRemoteAddress());
    }
  }
}
