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

package org.apache.spark.rpc.netty

import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap, LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}
import javax.annotation.concurrent.GuardedBy

import scala.collection.JavaConverters._
import scala.concurrent.Promise
import scala.util.control.NonFatal

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.network.client.RpcResponseCallback
import org.apache.spark.rpc._
import org.apache.spark.util.ThreadUtils

/**
 * A message dispatcher, responsible for routing RPC messages to the appropriate endpoint(s).
 *
 * Dispatcher负责将RPC消息路由到要该对此消息处理的RpcEndpoint
 */
private[netty] class Dispatcher(nettyEnv: NettyRpcEnv) extends Logging {

  /**
   * Inbox与RpcEndpoint，NettyRpcEndpointRef通过此EndpointData相关联
   */
  private class EndpointData(
      val name: String,
      val endpoint: RpcEndpoint,
      val ref: NettyRpcEndpointRef) {
    val inbox = new Inbox(ref, endpoint)
  }

  private val endpoints: ConcurrentMap[String, EndpointData] =
    new ConcurrentHashMap[String, EndpointData]
  private val endpointRefs: ConcurrentMap[RpcEndpoint, RpcEndpointRef] =
    new ConcurrentHashMap[RpcEndpoint, RpcEndpointRef]

  // Track the receivers whose inboxes may contain messages.
  private val receivers = new LinkedBlockingQueue[EndpointData]

  /**
   * True if the dispatcher has been stopped. Once stopped, all messages posted will be bounced
   * immediately.
   */
  @GuardedBy("this")
  private var stopped = false

  def registerRpcEndpoint(name: String, endpoint: RpcEndpoint): NettyRpcEndpointRef = {
    // 使用当前RpcEndpoint所在的NettyRpcEnv的地址和RpcEndpoint的名称创建RpcEndpointAddress
    val addr = RpcEndpointAddress(nettyEnv.address, name)
    // 创建NettyRpcEndpoint的引用对象
    val endpointRef = new NettyRpcEndpointRef(nettyEnv.conf, addr, nettyEnv)
    synchronized {
      if (stopped) {
        throw new IllegalStateException("RpcEnv has been stopped")
      }
      // 创建EndpointData，并放入endpoints缓存
      // 由于EndpointData是新创建的，所以在构造EndpointData的过程中也会构造它内部的Inbox。
      // 因此Inbox在构造的最后会将OnStart放入messages中。
      if (endpoints.putIfAbsent(name, new EndpointData(name, endpoint, endpointRef)) != null) {
        throw new IllegalArgumentException(s"There is already an RpcEndpoint called $name")
      }
      val data = endpoints.get(name)
      // 将RpcEndpoint与NettyRpcEndpointRef的映射关系放入endpointRefs缓存
      endpointRefs.put(data.endpoint, data.ref)
      // 将EndpointData放入阻塞队列receivers的队尾
      // MessageLoop线程异步获取到此EndpointData，并处理其Inbox中刚刚放入的OnStart消息，
      // 最终调用RpcEndpoint的OnStart方法在RpcEndpoint开始处理消息之前做一些准备工作
      receivers.offer(data)  // for the OnStart message
    }
    endpointRef
  }

  def getRpcEndpointRef(endpoint: RpcEndpoint): RpcEndpointRef = endpointRefs.get(endpoint)

  def removeRpcEndpointRef(endpoint: RpcEndpoint): Unit = endpointRefs.remove(endpoint)

  // Should be idempotent
  private def unregisterRpcEndpoint(name: String): Unit = {
    // 从endpoints移除EndpointData
    val data = endpoints.remove(name)
    if (data != null) {
      // 调用EndpointData中的Inbox的stop方法停止Inbox
      data.inbox.stop()
      // 将EndpointData重新放入receivers
      receivers.offer(data)  // for the OnStop message
    }
    // Don't clean `endpointRefs` here because it's possible that some messages are being processed
    // now and they can use `getRpcEndpointRef`. So `endpointRefs` will be cleaned in Inbox via
    // `removeRpcEndpointRef`.

    // 当要移除一个EndpointData时，其Inbox可能正在对消息进行处理，所以不能贸然停止，
    // 包括移除RpcEndpoint与RpcEndpointRef的映射关系。
    // 这里采用了更平滑的停止方式，即调用了Inbox的stop方法来平滑过渡。
  }

  /** 用于对RpcEndpoint移除注册 */
  def stop(rpcEndpointRef: RpcEndpointRef): Unit = {
    synchronized {
      if (stopped) {
        // This endpoint will be stopped by Dispatcher.stop() method.
        return
      }
      unregisterRpcEndpoint(rpcEndpointRef.name)
    }
  }

  /**
   * Send a message to all registered [[RpcEndpoint]]s in this process.
   *
   * This can be used to make network events known to all end points (e.g. "a new node connected").
   */
  def postToAll(message: InboxMessage): Unit = {
    val iter = endpoints.keySet().iterator()
    while (iter.hasNext) {
      val name = iter.next
      postMessage(name, message, (e) => logWarning(s"Message $message dropped. ${e.getMessage}"))
    }
  }

  /** Posts a message sent by a remote endpoint. */
  def postRemoteMessage(message: RequestMessage, callback: RpcResponseCallback): Unit = {
    val rpcCallContext =
      new RemoteNettyRpcCallContext(nettyEnv, callback, message.senderAddress)
    val rpcMessage = RpcMessage(message.senderAddress, message.content, rpcCallContext)
    postMessage(message.receiver.name, rpcMessage, (e) => callback.onFailure(e))
  }

  /** Posts a message sent by a local endpoint. */
  def postLocalMessage(message: RequestMessage, p: Promise[Any]): Unit = {
    val rpcCallContext =
      new LocalNettyRpcCallContext(message.senderAddress, p)
    val rpcMessage = RpcMessage(message.senderAddress, message.content, rpcCallContext)
    postMessage(message.receiver.name, rpcMessage, (e) => p.tryFailure(e))
  }

  /** Posts a one-way message. */
  def postOneWayMessage(message: RequestMessage): Unit = {
    postMessage(message.receiver.name, OneWayMessage(message.senderAddress, message.content),
      (e) => throw e)
  }

  /**
   * Posts a message to a specific endpoint.
   *
   * @param endpointName name of the endpoint.
   * @param message the message to post
   * @param callbackIfStopped callback function if the endpoint is stopped.
   */
  private def postMessage(
      endpointName: String,
      message: InboxMessage,
      callbackIfStopped: (Exception) => Unit): Unit = {
    val error = synchronized {
      // 根据endpointName获取EndpointData
      val data = endpoints.get(endpointName)
      if (stopped) {
        Some(new RpcEnvStoppedException())
      } else if (data == null) {
        Some(new SparkException(s"Could not find $endpointName."))
      } else {
        // 如果当前Dispatcher没有停止并且endpoints中确实存在名为endpointName的EndpointData
        // 那么将调用EndpointData对应的Inbox的post方法将消息加入Inbox的消息列表中，
        // 因此还需要将EndpointData推入receivers中，以便MessageLoop处理此Inbox中的消息。
        data.inbox.post(message)
        receivers.offer(data)
        None
      }
    }
    // We don't need to call `onStop` in the `synchronized` block
    error.foreach(callbackIfStopped)
  }

  def stop(): Unit = {
    synchronized {
      if (stopped) {
        return
      }
      stopped = true
    }
    // Stop all endpoints. This will queue all endpoints for processing by the message loops.
    // 对endpoints中所有的EndpointData取消注册，
    // 将向endpoints中的每个EndpointData的Inbox里放置OnStop消息。
    endpoints.keySet().asScala.foreach(unregisterRpcEndpoint)
    // Enqueue a message that tells the message loops to stop.
    // 由与Dispatcher都停止了，所以Dispatcher中的所有MessageLoop线程也就没有存在的必要了
    receivers.offer(PoisonPill)
    // 关闭线程池
    threadpool.shutdown()
  }

  def awaitTermination(): Unit = {
    threadpool.awaitTermination(Long.MaxValue, TimeUnit.MILLISECONDS)
  }

  /**
   * Return if the endpoint exists
   */
  def verify(name: String): Boolean = {
    endpoints.containsKey(name)
  }

  /** Thread pool used for dispatching messages. */
  private val threadpool: ThreadPoolExecutor = {
    val numThreads = nettyEnv.conf.getInt("spark.rpc.netty.dispatcher.numThreads",
      math.max(2, Runtime.getRuntime.availableProcessors()))
    val pool = ThreadUtils.newDaemonFixedThreadPool(numThreads, "dispatcher-event-loop")
    for (i <- 0 until numThreads) {
      pool.execute(new MessageLoop)
    }
    pool
  }

  /** Message loop used for dispatching messages. */
  private class MessageLoop extends Runnable {
    override def run(): Unit = {
      try {
        while (true) {
          try {
            // 从receivers获取EndpointData，其Inbox的messages列表中肯定有了新的消息。
            // 换言之，只有Inbox的messages列表中有了新的消息此EndpointData才会被放入receivers中。
            // 由于receivers是一个阻塞队列，所以当receivers中没有EndpointData时，MessageLoop线程会被阻塞。
            val data = receivers.take()
            if (data == PoisonPill) {
              // Put PoisonPill back so that other MessageLoops can see it.
              // 如果取到的EndpointData是一个PoisonPill（毒药），那么此MessageLoop线程将退出（通过return语句）。
              // 将PoisonPill重新放入receivers，是因为threadpool中有可能不止一个MessageLoop线程，为了让大家都“毒发身亡”
              // 还需要将PoisonPill重新放入receivers中，这样其他“活着”的线程就会再次误食“毒药”，达到所有MessageLoop都结束的效果。
              receivers.offer(PoisonPill)
              return
            }
            // 对inbox中的消息进行处理
            data.inbox.process(Dispatcher.this)
          } catch {
            case NonFatal(e) => logError(e.getMessage, e)
          }
        }
      } catch {
        case ie: InterruptedException => // exit
      }
    }
  }

  /** A poison endpoint that indicates MessageLoop should exit its message loop. */
  private val PoisonPill = new EndpointData(null, null, null)
}
