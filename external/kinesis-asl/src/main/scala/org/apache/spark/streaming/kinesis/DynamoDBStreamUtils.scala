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

import scala.reflect.ClassTag

import com.amazonaws.auth.{AWSCredentialsProvider, BasicAWSCredentials, DefaultAWSCredentialsProviderChain}
import com.amazonaws.internal.StaticCredentialsProvider
import com.amazonaws.regions.{RegionUtils, ServiceAbbreviations}
import com.amazonaws.services.cloudwatch.model.ResourceNotFoundException
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.model.{DescribeTableRequest, Record, TableStatus}
import com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient
import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordAdapter
import com.amazonaws.services.kinesis.AmazonKinesis
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.amazonaws.services.kinesis.model.{Record => KinesisRecord}

import org.apache.spark.api.java.function.{Function => JFunction}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.streaming.api.java.{JavaReceiverInputDStream, JavaStreamingContext}
import org.apache.spark.streaming.dstream.ReceiverInputDStream

object DynamoDBStreamUtils
{
  /**
   * Create an input stream that pulls messages from a DynamoDB stream.
   * This uses the DynamoDB Kinesis Client Adapter to pull messages from
   * the stream via the Kinesis Client Library (KCL).
   *
   * @note The KCL tracks client state and progress by writing checkpoints to a separate
   *       DyanamoDB table from the one being monitored by a stream. Consequently,
   *       there are different references to DynamoDB below.
   *       Be warned, as this can get confusing.
   *
   * @param ssc StreamingContext object
   * @param kinesisAppName  Kinesis application name used by the Kinesis Client Library
   *                        (KCL) to update DynamoDB tracking tables
   * @param tableName   DynamoDB table name
   * @param endpointUrl Url of DynamoDB Streams service
   *                    (e.g., https://streams.dynamodb.us-east-1.amazonaws.com)
   * @param regionName   Name of region used by the Kinesis Client Library (KCL)
   *                     to update
   *                     DynamoDB tracking table (lease coordination and checkpointing)
   *                     and CloudWatch (metrics)
   * @param initialPositionInStream  In the absence of Kinesis checkpoint info, this is the
   *                                 worker's initial starting position in the stream.
   *                                 The values are either the beginning of the stream
   *                                 per Kinesis' limit of 24 hours
   *                                 (InitialPositionInStream.TRIM_HORIZON) or
   *                                 the tip of the stream (InitialPositionInStream.LATEST).
   * @param checkpointInterval  Checkpoint interval for Kinesis checkpointing.
   *                            See the Kinesis Spark Streaming documentation for more
   *                            details on the different types of checkpoints.
   * @param storageLevel Storage level to use for storing the received objects.
   *                     StorageLevel.MEMORY_AND_DISK_2 is recommended.
   * @param messageHandler A custom message handler that can generate a generic output from a
   *                       Kinesis `Record`, which contains both message data, and metadata.
   *
   * @note The AWS credentials will be discovered using the DefaultAWSCredentialsProviderChain
   * on the workers. See AWS documentation to understand how DefaultAWSCredentialsProviderChain
   * gets the AWS credentials.
   */
  def createStream[T: ClassTag]( ssc: StreamingContext,
                                 kinesisAppName: String,
                                 tableName: String,
                                 endpointUrl: String,
                                 regionName: String,
                                 initialPositionInStream: InitialPositionInStream,
                                 checkpointInterval: Duration,
                                 storageLevel: StorageLevel,
                                 messageHandler: Record => T ): ReceiverInputDStream[T] =
  {
    val cleanedHandler = ssc.sc.clean(messageHandler.compose(kinesis2DDBRecordConverter))
    // Setting scope to override receiver stream's scope of "receiver stream"
    ssc.withNamedScope("dynamodb stream") {
      new KinesisInputDStream[T](
        ssc,
        ddbStreamARNForTable(tableName),
        validateRegion(regionName),
        kinesisClientBuilder(endpointUrl),
        initialPositionInStream,
        kinesisAppName,
        checkpointInterval,
        storageLevel,
        cleanedHandler )
    }
  }

  /**
   * Create an input stream that pulls messages from a DynamoDB stream.
   * This uses the DynamoDB Kinesis Client Adapter to pull messages from
   * the stream via the Kinesis Client Library (KCL).
   *
   * @note The KCL tracks client state and progress by writing checkpoints to a separate
   *       DyanamoDB table from the one being monitored by a stream. Consequently,
   *       there are different references to DynamoDB below.
   *       Be warned, as this can get confusing.
   *
   * @param ssc StreamingContext object
   * @param kinesisAppName  Kinesis application name used by the Kinesis Client Library
   *                        (KCL) to update DynamoDB tracking tables
   * @param tableName   DynamoDB table name
   * @param endpointUrl Url of DynamoDB Streams service
   *                    (e.g., https://streams.dynamodb.us-east-1.amazonaws.com)
   * @param regionName   Name of region used by the Kinesis Client Library (KCL) to update
   *                     DynamoDB tracking table (lease coordination and checkpointing)
   *                     and CloudWatch (metrics)
   * @param initialPositionInStream  In the absence of Kinesis checkpoint info, this is the
   *                                 worker's initial starting position in the stream.
   *                                 The values are either the beginning of the stream
   *                                 per Kinesis' limit of 24 hours
   *                                 (InitialPositionInStream.TRIM_HORIZON) or
   *                                 the tip of the stream (InitialPositionInStream.LATEST).
   * @param checkpointInterval  Checkpoint interval for Kinesis checkpointing.
   *                            See the Kinesis Spark Streaming documentation for more
   *                            details on the different types of checkpoints.
   * @param storageLevel Storage level to use for storing the received objects.
   *                     StorageLevel.MEMORY_AND_DISK_2 is recommended.
   * @param messageHandler A custom message handler that can generate a generic output from a
   *                       Kinesis `Record`, which contains both message data, and metadata.
   * @param awsAccessKeyId  AWS AccessKeyId (if null, will use DefaultAWSCredentialsProviderChain)
   * @param awsSecretKey  AWS SecretKey (if null, will use DefaultAWSCredentialsProviderChain)
   *
   * @note The given AWS credentials will get saved in DStream checkpoints if checkpointing
   * is enabled. Make sure that your checkpoint directory is secure.
   */
  // scalastyle:off
  def createStream[T: ClassTag]( ssc: StreamingContext,
                                 kinesisAppName: String,
                                 tableName: String,
                                 endpointUrl: String,
                                 regionName: String,
                                 initialPositionInStream: InitialPositionInStream,
                                 checkpointInterval: Duration,
                                 storageLevel: StorageLevel,
                                 messageHandler: Record => T,
                                 awsAccessKeyId: String,
                                 awsSecretKey: String ): ReceiverInputDStream[T] =
  {
    // scalastyle:on
    val cleanedHandler = ssc.sc.clean(messageHandler.compose(kinesis2DDBRecordConverter))
    ssc.withNamedScope("dynamodb stream") {
      new KinesisInputDStream[T](
        ssc,
        ddbStreamARNForTable( tableName, awsAccessKeyId, awsSecretKey ),
        validateRegion(regionName),
        kinesisClientBuilder( endpointUrl, awsAccessKeyId, awsSecretKey ),
        initialPositionInStream,
        kinesisAppName,
        checkpointInterval,
        storageLevel,
        cleanedHandler )
    }
  }

  /**
   * Create an input stream that pulls messages from a DynamoDB stream.
   * This uses the DynamoDB Kinesis Client Adapter to pull messages from
   * the stream via the Kinesis Client Library (KCL).
   *
   * @note The KCL tracks client state and progress by writing checkpoints to a separate
   *       DyanamoDB table from the one being monitored by a stream. Consequently,
   *       there are different references to DynamoDB below.
   *       Be warned, as this can get confusing.
   *
   * @param ssc StreamingContext object
   * @param kinesisAppName  Kinesis application name used by the Kinesis Client Library
   *                        (KCL) to update DynamoDB tracking tables
   * @param tableName   DynamoDB table name
   * @param endpointUrl Url of DynamoDB Streams service
   *                    (e.g., https://streams.dynamodb.us-east-1.amazonaws.com)
   * @param regionName   Name of region used by the Kinesis Client Library (KCL) to update
   *                     DynamoDB tracking table (lease coordination and checkpointing)
   *                     and CloudWatch (metrics)
   * @param initialPositionInStream  In the absence of Kinesis checkpoint info, this is the
   *                                 worker's initial starting position in the stream.
   *                                 The values are either the beginning of the stream
   *                                 per Kinesis' limit of 24 hours
   *                                 (InitialPositionInStream.TRIM_HORIZON) or
   *                                 the tip of the stream (InitialPositionInStream.LATEST).
   * @param checkpointInterval  Checkpoint interval for Kinesis checkpointing.
   *                            See the Kinesis Spark Streaming documentation for more
   *                            details on the different types of checkpoints.
   * @param storageLevel Storage level to use for storing the received objects.
   *                     StorageLevel.MEMORY_AND_DISK_2 is recommended.
   *
   * @note The AWS credentials will be discovered using the DefaultAWSCredentialsProviderChain
   * on the workers. See AWS documentation to understand how DefaultAWSCredentialsProviderChain
   * gets the AWS credentials.
   */
  def createStream( ssc: StreamingContext,
                    kinesisAppName: String,
                    tableName: String,
                    endpointUrl: String,
                    regionName: String,
                    initialPositionInStream: InitialPositionInStream,
                    checkpointInterval: Duration,
                    storageLevel: StorageLevel ): ReceiverInputDStream[Array[Byte]] =
  {
    // Setting scope to override receiver stream's scope of "receiver stream"
    ssc.withNamedScope("dynamodb stream") {
      new KinesisInputDStream[Array[Byte]](
        ssc,
        ddbStreamARNForTable(tableName),
        validateRegion(regionName),
        kinesisClientBuilder(endpointUrl),
        initialPositionInStream,
        kinesisAppName,
        checkpointInterval,
        storageLevel,
        defaultMessageHandler )
    }
  }

  /**
   * Create an input stream that pulls messages from a DynamoDB stream.
   * This uses the DynamoDB Kinesis Client Adapter to pull messages from
   * the stream via the Kinesis Client Library (KCL).
   *
   * @note The KCL tracks client state and progress by writing checkpoints to a separate
   *       DyanamoDB table from the one being monitored by a stream. Consequently,
   *       there are different references to DynamoDB below.
   *       Be warned, as this can get confusing.
   *
   * @param ssc StreamingContext object
   * @param kinesisAppName  Kinesis application name used by the Kinesis Client Library
   *                        (KCL) to update DynamoDB tracking tables
   * @param tableName   DynamoDB table name
   * @param endpointUrl Url of DynamoDB Streams service
   *                    (e.g., https://streams.dynamodb.us-east-1.amazonaws.com)
   * @param regionName   Name of region used by the Kinesis Client Library (KCL) to update
   *                     DynamoDB tracking table (lease coordination and checkpointing)
   *                     and CloudWatch (metrics)
   * @param initialPositionInStream  In the absence of Kinesis checkpoint info, this is the
   *                                 worker's initial starting position in the stream.
   *                                 The values are either the beginning of the stream
   *                                 per Kinesis' limit of 24 hours
   *                                 (InitialPositionInStream.TRIM_HORIZON) or
   *                                 the tip of the stream (InitialPositionInStream.LATEST).
   * @param checkpointInterval  Checkpoint interval for Kinesis checkpointing.
   *                            See the Kinesis Spark Streaming documentation for more
   *                            details on the different types of checkpoints.
   * @param storageLevel Storage level to use for storing the received objects.
   *                     StorageLevel.MEMORY_AND_DISK_2 is recommended.
   * @param awsAccessKeyId  AWS AccessKeyId (if null, will use DefaultAWSCredentialsProviderChain)
   * @param awsSecretKey  AWS SecretKey (if null, will use DefaultAWSCredentialsProviderChain)
   *
   * @note The given AWS credentials will get saved in DStream checkpoints if checkpointing
   * is enabled. Make sure that your checkpoint directory is secure.
   */
  def createStream( ssc: StreamingContext,
                    kinesisAppName: String,
                    tableName: String,
                    endpointUrl: String,
                    regionName: String,
                    initialPositionInStream: InitialPositionInStream,
                    checkpointInterval: Duration,
                    storageLevel: StorageLevel,
                    awsAccessKeyId: String,
                    awsSecretKey: String ): ReceiverInputDStream[Array[Byte]] =
  {
    ssc.withNamedScope("dynamodb stream") {
      new KinesisInputDStream[Array[Byte]](
        ssc,
        ddbStreamARNForTable( tableName, awsAccessKeyId, awsSecretKey ),
        validateRegion(regionName),
        kinesisClientBuilder( endpointUrl, awsAccessKeyId, awsSecretKey ),
        initialPositionInStream,
        kinesisAppName,
        checkpointInterval,
        storageLevel,
        defaultMessageHandler )
    }
  }

  /**
   * Create an input stream that pulls messages from a DynamoDB stream.
   * This uses the DynamoDB Kinesis Client Adapter to pull messages from
   * the stream via the Kinesis Client Library (KCL).
   *
   * @note The KCL tracks client state and progress by writing checkpoints to a separate
   *       DyanamoDB table from the one being monitored by a stream. Consequently,
   *       there are different references to DynamoDB below.
   *       Be warned, as this can get confusing.
   *
   * @param jssc Java StreamingContext object
   * @param kinesisAppName  Kinesis application name used by the Kinesis Client Library
   *                        (KCL) to update DynamoDB tracking tables
   * @param tableName   DynamoDB table name
   * @param endpointUrl Url of DynamoDB Streams service
   *                    (e.g., https://streams.dynamodb.us-east-1.amazonaws.com)
   * @param regionName   Name of region used by the Kinesis Client Library (KCL) to update
   *                     DynamoDB tracking table (lease coordination and checkpointing)
   *                     and CloudWatch (metrics)
   * @param initialPositionInStream  In the absence of Kinesis checkpoint info, this is the
   *                                 worker's initial starting position in the stream.
   *                                 The values are either the beginning of the stream
   *                                 per Kinesis' limit of 24 hours
   *                                 (InitialPositionInStream.TRIM_HORIZON) or
   *                                 the tip of the stream (InitialPositionInStream.LATEST).
   * @param checkpointInterval  Checkpoint interval for Kinesis checkpointing.
   *                            See the Kinesis Spark Streaming documentation for more
   *                            details on the different types of checkpoints.
   * @param storageLevel Storage level to use for storing the received objects.
   *                     StorageLevel.MEMORY_AND_DISK_2 is recommended.
   * @param messageHandler A custom message handler that can generate a generic output from a
   *                       Kinesis `Record`, which contains both message data, and metadata.
   * @param recordClass Class of the records in DStream
   *
   * @note The AWS credentials will be discovered using the DefaultAWSCredentialsProviderChain
   * on the workers. See AWS documentation to understand how DefaultAWSCredentialsProviderChain
   * gets the AWS credentials.
   */
  def createStream[T]( jssc: JavaStreamingContext,
                       kinesisAppName: String,
                       tableName: String,
                       endpointUrl: String,
                       regionName: String,
                       initialPositionInStream: InitialPositionInStream,
                       checkpointInterval: Duration,
                       storageLevel: StorageLevel,
                       messageHandler: JFunction[Record, T],
                       recordClass: Class[T] ): JavaReceiverInputDStream[T] =
  {
    implicit val recordCmt: ClassTag[T] = ClassTag(recordClass)
    val cleanedHandler = jssc.sparkContext.clean(messageHandler.call(_))
    createStream[T](
      jssc.ssc,
      kinesisAppName,
      tableName,
      endpointUrl,
      regionName,
      initialPositionInStream,
      checkpointInterval,
      storageLevel,
      cleanedHandler )
  }

  /**
   * Create an input stream that pulls messages from a DynamoDB stream.
   * This uses the DynamoDB Kinesis Client Adapter to pull messages from
   * the stream via the Kinesis Client Library (KCL).
   *
   * @note The KCL tracks client state and progress by writing checkpoints to a separate
   *       DyanamoDB table from the one being monitored by a stream. Consequently,
   *       there are different references to DynamoDB below.
   *       Be warned, as this can get confusing.
   *
   * @param jssc Java StreamingContext object
   * @param kinesisAppName  Kinesis application name used by the Kinesis Client Library
   *                        (KCL) to update DynamoDB tracking tables
   * @param tableName   DynamoDB table name
   * @param endpointUrl Url of DynamoDB Streams service
   *                    (e.g., https://streams.dynamodb.us-east-1.amazonaws.com)
   * @param regionName   Name of region used by the Kinesis Client Library (KCL) to update
   *                     DynamoDB tracking table (lease coordination and checkpointing)
   *                     and CloudWatch (metrics)
   * @param initialPositionInStream  In the absence of Kinesis checkpoint info, this is the
   *                                 worker's initial starting position in the stream.
   *                                 The values are either the beginning of the stream
   *                                 per Kinesis' limit of 24 hours
   *                                 (InitialPositionInStream.TRIM_HORIZON) or
   *                                 the tip of the stream (InitialPositionInStream.LATEST).
   * @param checkpointInterval  Checkpoint interval for Kinesis checkpointing.
   *                            See the Kinesis Spark Streaming documentation for more
   *                            details on the different types of checkpoints.
   * @param storageLevel Storage level to use for storing the received objects.
   *                     StorageLevel.MEMORY_AND_DISK_2 is recommended.
   * @param messageHandler A custom message handler that can generate a generic output from a
   *                       Kinesis `Record`, which contains both message data, and metadata.
   * @param recordClass Class of the records in DStream
   * @param awsAccessKeyId  AWS AccessKeyId (if null, will use DefaultAWSCredentialsProviderChain)
   * @param awsSecretKey  AWS SecretKey (if null, will use DefaultAWSCredentialsProviderChain)
   *
   * @note The given AWS credentials will get saved in DStream checkpoints if checkpointing
   * is enabled. Make sure that your checkpoint directory is secure.
   */
  // scalastyle:off
  def createStream[T]( jssc: JavaStreamingContext,
                       kinesisAppName: String,
                       tableName: String,
                       endpointUrl: String,
                       regionName: String,
                       initialPositionInStream: InitialPositionInStream,
                       checkpointInterval: Duration,
                       storageLevel: StorageLevel,
                       messageHandler: JFunction[Record, T],
                       recordClass: Class[T],
                       awsAccessKeyId: String,
                       awsSecretKey: String ): JavaReceiverInputDStream[T] =
  {
    // scalastyle:on
    implicit val recordCmt: ClassTag[T] = ClassTag(recordClass)
    val cleanedHandler = jssc.sparkContext.clean(messageHandler.call(_))
    createStream[T](
      jssc.ssc,
      kinesisAppName,
      tableName,
      endpointUrl,
      regionName,
      initialPositionInStream,
      checkpointInterval,
      storageLevel,
      cleanedHandler,
      awsAccessKeyId,
      awsSecretKey )
  }

  /**
   * Create an input stream that pulls messages from a DynamoDB stream.
   * This uses the DynamoDB Kinesis Client Adapter to pull messages from
   * the stream via the Kinesis Client Library (KCL).
   *
   * @note The KCL tracks client state and progress by writing checkpoints to a separate
   *       DyanamoDB table from the one being monitored by a stream. Consequently,
   *       there are different references to DynamoDB below.
   *       Be warned, as this can get confusing.
   *
   * @param jssc Java StreamingContext object
   * @param kinesisAppName  Kinesis application name used by the Kinesis Client Library
   *                        (KCL) to update DynamoDB tracking tables
   * @param tableName   DynamoDB table name
   * @param endpointUrl Url of DynamoDB Streams service
   *                    (e.g., https://streams.dynamodb.us-east-1.amazonaws.com)
   * @param regionName   Name of region used by the Kinesis Client Library (KCL) to update
   *                     DynamoDB tracking table (lease coordination and checkpointing)
   *                     and CloudWatch (metrics)
   * @param initialPositionInStream  In the absence of Kinesis checkpoint info, this is the
   *                                 worker's initial starting position in the stream.
   *                                 The values are either the beginning of the stream
   *                                 per Kinesis' limit of 24 hours
   *                                 (InitialPositionInStream.TRIM_HORIZON) or
   *                                 the tip of the stream (InitialPositionInStream.LATEST).
   * @param checkpointInterval  Checkpoint interval for Kinesis checkpointing.
   *                            See the Kinesis Spark Streaming documentation for more
   *                            details on the different types of checkpoints.
   * @param storageLevel Storage level to use for storing the received objects.
   *                     StorageLevel.MEMORY_AND_DISK_2 is recommended.
   *
   * @note The AWS credentials will be discovered using the DefaultAWSCredentialsProviderChain
   * on the workers. See AWS documentation to understand how DefaultAWSCredentialsProviderChain
   * gets the AWS credentials.
   */
  def createStream( jssc: JavaStreamingContext,
                    kinesisAppName: String,
                    tableName: String,
                    endpointUrl: String,
                    regionName: String,
                    initialPositionInStream: InitialPositionInStream,
                    checkpointInterval: Duration,
                    storageLevel: StorageLevel ): JavaReceiverInputDStream[Array[Byte]] =
  {
    createStream(
      jssc.ssc,
      kinesisAppName,
      tableName,
      endpointUrl,
      regionName,
      initialPositionInStream,
      checkpointInterval,
      storageLevel )
  }

  /**
   * Create an input stream that pulls messages from a DynamoDB stream.
   * This uses the DynamoDB Kinesis Client Adapter to pull messages from
   * the stream via the Kinesis Client Library (KCL).
   *
   * @note The KCL tracks client state and progress by writing checkpoints to a separate
   *       DyanamoDB table from the one being monitored by a stream. Consequently,
   *       there are different references to DynamoDB below.
   *       Be warned, as this can get confusing.
   *
   * @param jssc Java StreamingContext object
   * @param kinesisAppName  Kinesis application name used by the Kinesis Client Library
   *                        (KCL) to update DynamoDB tracking tables
   * @param tableName   DynamoDB table name
   * @param endpointUrl Url of DynamoDB Streams service
   *                    (e.g., https://streams.dynamodb.us-east-1.amazonaws.com)
   * @param regionName   Name of region used by the Kinesis Client Library (KCL) to update
   *                     DynamoDB tracking table (lease coordination and checkpointing)
   *                     and CloudWatch (metrics)
   * @param initialPositionInStream  In the absence of Kinesis checkpoint info, this is the
   *                                 worker's initial starting position in the stream.
   *                                 The values are either the beginning of the stream
   *                                 per Kinesis' limit of 24 hours
   *                                 (InitialPositionInStream.TRIM_HORIZON) or
   *                                 the tip of the stream (InitialPositionInStream.LATEST).
   * @param checkpointInterval  Checkpoint interval for Kinesis checkpointing.
   *                            See the Kinesis Spark Streaming documentation for more
   *                            details on the different types of checkpoints.
   * @param storageLevel Storage level to use for storing the received objects.
   *                     StorageLevel.MEMORY_AND_DISK_2 is recommended.
   * @param awsAccessKeyId  AWS AccessKeyId (if null, will use DefaultAWSCredentialsProviderChain)
   * @param awsSecretKey  AWS SecretKey (if null, will use DefaultAWSCredentialsProviderChain)
   *
   * @note The given AWS credentials will get saved in DStream checkpoints if checkpointing
   * is enabled. Make sure that your checkpoint directory is secure.
   */
  def createStream( jssc: JavaStreamingContext,
                    kinesisAppName: String,
                    tableName: String,
                    endpointUrl: String,
                    regionName: String,
                    initialPositionInStream: InitialPositionInStream,
                    checkpointInterval: Duration,
                    storageLevel: StorageLevel,
                    awsAccessKeyId: String,
                    awsSecretKey: String ): JavaReceiverInputDStream[Array[Byte]] =
  {
    createStream(
      jssc.ssc,
      kinesisAppName,
      tableName,
      endpointUrl,
      regionName,
      initialPositionInStream,
      checkpointInterval,
      storageLevel,
      awsAccessKeyId,
      awsSecretKey )
  }

  private def ddbStreamARNForTable( tableName: String ): String =
    ddbStreamARNForTable( tableName, new DefaultAWSCredentialsProviderChain )

  private def ddbStreamARNForTable( tableName: String,
                                    awsAccessKeyId: String,
                                    awsSecretKey: String ): String =
  {
    val cred = new BasicAWSCredentials( awsAccessKeyId, awsSecretKey )
    ddbStreamARNForTable( tableName, new StaticCredentialsProvider(cred) )
  }

  private def ddbStreamARNForTable( tableName: String,
                                    credentialsProvider: AWSCredentialsProvider ): String =
  {
    // get the table description
    val ddbClient = new AmazonDynamoDBClient( credentialsProvider )
    val req = new DescribeTableRequest().withTableName(tableName)
    val res = ddbClient.describeTable(req)

    // identify the stream
    val table = res.getTable
    if( table == null || !TableStatus.ACTIVE.name.equals( table.getTableStatus ) ) {
      throw new ResourceNotFoundException("Table " + tableName + " does not exist or is not active")
    }
    val spec = res.getTable.getStreamSpecification
    if(spec == null || !spec.isStreamEnabled) {
      throw new ResourceNotFoundException("No active stream for table " + tableName)
    }
    ddbClient.shutdown()
    table.getLatestStreamArn
  }

  private def kinesisClientBuilder( endpointURL: String,
                                    awsAccessKeyId: String,
                                    awsSecretKey: String ): KinesisClientFactory =
    new KinesisClientFactory( endpointURL, awsAccessKeyId, awsSecretKey )
    {
      override def getServicePrefix: String = ServiceAbbreviations.DynamodbStreams

      override def getKinesisClient: AmazonKinesis =
      {
        val client = new AmazonDynamoDBStreamsAdapterClient(getCredentialsProvider)
        client.setEndpoint(endpointURL)
        client
      }
    }

  private def kinesisClientBuilder( endpointURL: String ): KinesisClientFactory =
    kinesisClientBuilder( endpointURL, new DefaultAWSCredentialsProviderChain )

  private def kinesisClientBuilder( endpointURL: String,
                                    credentials: AWSCredentialsProvider ): KinesisClientFactory =
    kinesisClientBuilder( endpointURL,
                          credentials.getCredentials.getAWSAccessKeyId,
                          credentials.getCredentials.getAWSSecretKey )


  private def validateRegion(regionName: String): String = {
    Option(RegionUtils.getRegion(regionName)).map { _.getName }.getOrElse {
      throw new IllegalArgumentException(s"Region name '$regionName' is not valid")
    }
  }

  private[kinesis] def kinesis2DDBRecordConverter( record: KinesisRecord ): Record =
    record match {
      case rec: RecordAdapter => rec.getInternalObject
      case _ => null
    }

  private[kinesis] def defaultMessageHandler(record: KinesisRecord): Array[Byte] = {
    if (record == null) return null
    val byteBuffer = record.getData
    val byteArray = new Array[Byte](byteBuffer.remaining())
    byteBuffer.get(byteArray)
    byteArray
  }
}