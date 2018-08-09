/*
 * Copyright 2018 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase;

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.message.cluster.GetClusterConfigRequest;
import com.couchbase.client.core.message.cluster.GetClusterConfigResponse;
import com.couchbase.client.core.message.kv.UpsertRequest;
import com.couchbase.client.core.message.kv.UpsertResponse;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.transcoder.TranscoderUtils;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import java.util.zip.CRC32;

import static com.couchbase.client.deps.io.netty.util.CharsetUtil.UTF_8;
import static java.util.Collections.singletonMap;

public class Driver {

  public static void main(String[] args) throws Exception {
    final OptionParser parser = new OptionParser();
    final OptionSpec<String> hostnameOpt = parser.accepts("cluster").withRequiredArg().defaultsTo("localhost").describedAs("hostname");
    final OptionSpec<String> bucketOpt = parser.accepts("bucket").withRequiredArg().defaultsTo("default");
    final OptionSpec<String> usernameOpt = parser.accepts("username").withRequiredArg().required();
    final OptionSpec<String> passwordOpt = parser.accepts("password").withRequiredArg().required();

    final OptionSet options;
    try {
      options = parser.parse(args);
    } catch (Exception e) {
      System.err.println(e.getMessage());
      System.err.println();
      parser.printHelpOn(System.err);
      System.exit(1);
      return;
    }

    final String host = options.valueOf(hostnameOpt);
    final String bucketName = options.valueOf(bucketOpt);
    final String username = options.valueOf(usernameOpt);
    final String password = options.valueOf(passwordOpt);

    final CouchbaseCluster cluster = CouchbaseCluster.create(host);
    cluster.authenticate(username, password);
    final Bucket bucket = cluster.openBucket(bucketName);

    final byte[] content = new ObjectMapper().writeValueAsBytes(singletonMap("greeting", "hello world"));
    final String key = "__i'm_not_in_the_expected_vbucket";

    final int numPartitions = getBucketConfig(bucket).numberOfPartitions();
    final int expectedPartition = partitionForKey(key, numPartitions);
    final int weirdPartition = (expectedPartition + 1) % numPartitions;

    upsertJsonWithPartitionOverride(bucket, key, content, weirdPartition);

    System.out.println();
    System.out.println("I'm feeling naughty because I've just written a document with key '"
        + key + "' to vbucket " + weirdPartition + " when the usual vbucket would be " + expectedPartition + ".");
    System.out.println();
  }

  private static int partitionForKey(String key, int numPartitions) {
    CRC32 crc32 = new CRC32();
    crc32.update(key.getBytes(UTF_8));
    long rv = (crc32.getValue() >> 16) & 0x7fff;
    return (int) rv & numPartitions - 1;
  }

  private static CouchbaseBucketConfig getBucketConfig(Bucket bucket) {
    final GetClusterConfigResponse response = (GetClusterConfigResponse) bucket.core().send(
        new GetClusterConfigRequest()).toBlocking().single();
    return (CouchbaseBucketConfig) response.config().bucketConfig(bucket.name());
  }

  private static void upsertJsonWithPartitionOverride(Bucket bucket, String key, byte[] content, final int vbucket) {
    UpsertRequest request = new UpsertRequest(key, Unpooled.wrappedBuffer(content), 0, TranscoderUtils.JSON_COMPAT_FLAGS, bucket.name(), true) {
      @Override
      public short partition() {
        return (short) vbucket;
      }
    };

    UpsertResponse response = (UpsertResponse) bucket.core().send(request).toBlocking().single();
    try {
      if (!response.status().isSuccess()) {
        throw new CouchbaseException(response.statusDetails().toString());
      }
    } finally {
      response.release();
    }
  }
}
