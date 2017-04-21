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

import com.amazonaws.auth.{AWSCredentialsProvider, BasicAWSCredentials, DefaultAWSCredentialsProviderChain}
import com.amazonaws.internal.StaticCredentialsProvider
import com.amazonaws.regions.{RegionUtils, ServiceAbbreviations}
import com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient
import com.amazonaws.services.kinesis.{AmazonKinesis, AmazonKinesisClient}

/**
 * A serializable factory for building a Kinesis clients
 * Created by Tristan Nixon <tristan.m.nixon@gmail.com> on 3/22/17.
 */
private[kinesis] abstract class KCLClientFactory
  ( val endpointURL: String,
    val awsAccessKeyId: Option[String],
    val awsSecretKey: Option[String] )
  extends Serializable
{
  def getServicePrefix: String

  def getRegionName: String = RegionUtils.getRegionByEndpoint(endpointURL).getName

  def getCredentialsProvider: AWSCredentialsProvider =
    awsAccessKeyId match {
      case None => new DefaultAWSCredentialsProviderChain
      case _ =>
        new StaticCredentialsProvider( new BasicAWSCredentials( awsAccessKeyId.get,
                                                                awsSecretKey.get ) )
    }

  def getKinesisClient: AmazonKinesis
}

private[kinesis] class KinesisClientFactory( endpointURL: String,
                                             awsAccessKeyId: Option[String],
                                             awsSecretKey: Option[String] )
  extends KCLClientFactory( endpointURL, awsAccessKeyId, awsSecretKey )
{
  def this( endpointURL: String, awsAccessKeyId: String, awsSecretKey: String ) =
    this( endpointURL, Option(awsAccessKeyId), Option(awsSecretKey) )

  def this( endpointURL: String ) = this( endpointURL, None, None )

  override def getServicePrefix: String = ServiceAbbreviations.Kinesis

  override def getKinesisClient: AmazonKinesis =
  {
    val client = new AmazonKinesisClient(getCredentialsProvider)
    client.setEndpoint(endpointURL)
    client
  }
}

private[kinesis] class DynamoDBStreamsClientFactory( endpointURL: String,
                                                     awsAccessKeyId: Option[String],
                                                     awsSecretKey: Option[String] )
  extends KCLClientFactory( endpointURL, awsAccessKeyId, awsSecretKey )
{
  def this( endpointURL: String, awsAccessKeyId: String, awsSecretKey: String ) =
    this( endpointURL, Option(awsAccessKeyId), Option(awsSecretKey) )

  def this( endpointURL: String ) = this( endpointURL, None, None )

  override def getServicePrefix: String = ServiceAbbreviations.DynamodbStreams

  override def getKinesisClient: AmazonKinesis =
  {
    val client = new AmazonDynamoDBStreamsAdapterClient(getCredentialsProvider)
    client.setEndpoint(endpointURL)
    client
  }
}
