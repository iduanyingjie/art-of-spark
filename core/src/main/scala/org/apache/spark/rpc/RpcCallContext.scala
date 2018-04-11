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

/**
 * A callback that [[RpcEndpoint]] can use to send back a message or failure. It's thread-safe
 * and can be called in any thread.
 *
 * RpcCallContext是用于分离核心业务逻辑和底层传输的桥接方法，
 * 这也可以看出Spark RPC多用组合，聚合以及回调callback的设计模式来做OO抽象，
 * 这样可以剥离业务逻辑->RPC封装（Spark-core模块内）->底层通信（spark-network-common）三者。
 * RpcCallContext可以用于回复正常的响应以及错误异常
 *
 * RpcCallContext也分为了两个子类，分别是LocalNettyRpcCallContext和RemoteNettyRpcCallContext，
 * 这个主要是框架内部使用，如果是本地就走LocalNettyRpcCallContext直接调用Endpoint即可，
 * 否则就走RemoteNettyRpcCallContext需要通过RPC和远程交互，这点也体现了RPC的核心概念，
 * 就是如何执行另外一个地址空间上的函数、方法，就仿佛在本地调用一样。
 */
private[spark] trait RpcCallContext {

  /**
   * Reply a message to the sender. If the sender is [[RpcEndpoint]], its [[RpcEndpoint.receive]]
   * will be called.
   */
  def reply(response: Any): Unit

  /**
   * Report a failure to the sender.
   */
  def sendFailure(e: Throwable): Unit

  /**
   * The sender of this message.
   */
  def senderAddress: RpcAddress
}
