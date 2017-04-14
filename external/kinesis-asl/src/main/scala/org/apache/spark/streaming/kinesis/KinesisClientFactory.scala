/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.streaming.kinesis

import com.amazonaws.auth.{AWSCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.internal.StaticCredentialsProvider
import com.amazonaws.regions.RegionUtils
import com.amazonaws.services.kinesis.AmazonKinesis

/**
 * A serializable factory for building a Kinesis clients
 * Created by Tristan Nixon <tristan.m.nixon@gmail.com> on 3/22/17.
 */
private[kinesis] abstract class KinesisClientFactory
  ( val endpointURL: String,
    val awsAccessKeyId: String,
    val awsSecretKey: String )
  extends Serializable
{
  def getServicePrefix: String

  def getRegionName: String = RegionUtils.getRegionByEndpoint(endpointURL).getName

  def getCredentialsProvider: AWSCredentialsProvider =
    new StaticCredentialsProvider( new BasicAWSCredentials( awsAccessKeyId, awsSecretKey ) )

  def getKinesisClient: AmazonKinesis
}
