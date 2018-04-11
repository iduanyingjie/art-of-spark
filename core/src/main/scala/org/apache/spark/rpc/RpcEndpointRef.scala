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

package org.apache.spark.rpc

import scala.concurrent.Future
import scala.reflect.ClassTag

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.util.RpcUtils

/**
 * A reference for a remote [[RpcEndpoint]]. [[RpcEndpointRef]] is thread-safe.
 *
 * RpcEndpointRef类似于Akka中ActorRef，顾名思义，它是RpcEndpoint的引用，提供的方法send等同于!,
 * ask方法等同于?，send用于单向发送请求（RpcEndpoint中的receive响应它），提供fire-and-forget语义，
 * 而ask提供请求响应的语义（RpcEndpoint中的receiveAndReply响应它），默认是需要返回response的，
 * 带有超时机制，可以同步阻塞等待，也可以返回一个Future句柄，不阻塞发起请求的工作线程。
 *
 * RpcEndpointRef是客户端发起请求的入口，它可以从RpcEnv中获取，并且聪明的做本地调用或者RPC。
 */
private[spark] abstract class RpcEndpointRef(conf: SparkConf)
  extends Serializable with Logging {

  // RPC最大重新连接次数
  private[this] val maxRetries = RpcUtils.numRetries(conf)
  // RPC每次重新连接需要等待的毫秒数
  private[this] val retryWaitMs = RpcUtils.retryWaitMs(conf)
  // RPC的ask操作默认的超时时间
  private[this] val defaultAskTimeout = RpcUtils.askRpcTimeout(conf)

  /**
   * return the address for the [[RpcEndpointRef]]
   *
   * 返回当前RpcEndpointRef对应的RpcEndpoint的RPC地址
   */
  def address: RpcAddress

  /**
   * 返回当前RpcEndpointRef对应的RpcEndpoint的RPC名称
   */
  def name: String

  /**
   * Sends a one-way asynchronous message. Fire-and-forget semantics.
   *
   * 发送单向异步消息（at-most-once）
   */
  def send(message: Any): Unit

  /**
   * Send a message to the corresponding [[RpcEndpoint.receiveAndReply)]] and return a [[Future]] to
   * receive the reply within the specified timeout.
   *
   * This method only sends the message once and never retries.
   */
  def ask[T: ClassTag](message: Any, timeout: RpcTimeout): Future[T]

  /**
   * Send a message to the corresponding [[RpcEndpoint.receiveAndReply)]] and return a [[Future]] to
   * receive the reply within a default timeout.
   *
   * This method only sends the message once and never retries.
   */
  def ask[T: ClassTag](message: Any): Future[T] = ask(message, defaultAskTimeout)

  /**
   * Send a message to the corresponding [[RpcEndpoint]] and get its result within a default
   * timeout, or throw a SparkException if this fails even after the default number of retries.
   * The default `timeout` will be used in every trial of calling `sendWithReply`. Because this
   * method retries, the message handling in the receiver side should be idempotent.
   *
   * Note: this is a blocking action which may cost a lot of time,  so don't call it in a message
   * loop of [[RpcEndpoint]].
   *
   * @param message the message to send
   * @tparam T type of the reply message
   * @return the reply message from the corresponding [[RpcEndpoint]]
   */
  def askWithRetry[T: ClassTag](message: Any): T = askWithRetry(message, defaultAskTimeout)

  /**
   * Send a message to the corresponding [[RpcEndpoint.receive]] and get its result within a
   * specified timeout, throw a SparkException if this fails even after the specified number of
   * retries. `timeout` will be used in every trial of calling `sendWithReply`. Because this method
   * retries, the message handling in the receiver side should be idempotent.
   *
   * Note: this is a blocking action which may cost a lot of time, so don't call it in a message
   * loop of [[RpcEndpoint]].
   *
   * @param message the message to send
   * @param timeout the timeout duration
   * @tparam T type of the reply message
   * @return the reply message from the corresponding [[RpcEndpoint]]
   */
  def askWithRetry[T: ClassTag](message: Any, timeout: RpcTimeout): T = {
    // TODO: Consider removing multiple attempts
    var attempts = 0
    var lastException: Exception = null
    while (attempts < maxRetries) {
      attempts += 1
      try {
        val future = ask[T](message, timeout)
        val result = timeout.awaitResult(future)
        if (result == null) {
          throw new SparkException("RpcEndpoint returned null")
        }
        return result
      } catch {
        case ie: InterruptedException => throw ie
        case e: Exception =>
          lastException = e
          logWarning(s"Error sending message [message = $message] in $attempts attempts", e)
      }

      if (attempts < maxRetries) {
        Thread.sleep(retryWaitMs)
      }
    }

    throw new SparkException(
      s"Error sending message [message = $message]", lastException)
  }

}
