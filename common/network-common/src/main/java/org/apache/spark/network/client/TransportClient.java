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

package org.apache.spark.network.client;

import java.io.Closeable;
import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.buffer.NioManagedBuffer;
import org.apache.spark.network.protocol.ChunkFetchRequest;
import org.apache.spark.network.protocol.OneWayMessage;
import org.apache.spark.network.protocol.RpcRequest;
import org.apache.spark.network.protocol.StreamChunkId;
import org.apache.spark.network.protocol.StreamRequest;
import static org.apache.spark.network.util.NettyUtils.getRemoteAddress;

/**
 * Client for fetching consecutive chunks of a pre-negotiated stream. This API is intended to allow
 * efficient transfer of a large amount of data, broken up into chunks with size ranging from
 * hundreds of KB to a few MB.
 *
 * Note that while this client deals with the fetching of chunks from a stream (i.e., data plane),
 * the actual setup of the streams is done outside the scope of the transport layer. The convenience
 * method "sendRPC" is provided to enable control plane communication between the client and server
 * to perform this setup.
 *
 * For example, a typical workflow might be:
 * client.sendRPC(new OpenFile("/foo")) --&gt; returns StreamId = 100
 * client.fetchChunk(streamId = 100, chunkIndex = 0, callback)
 * client.fetchChunk(streamId = 100, chunkIndex = 1, callback)
 * ...
 * client.sendRPC(new CloseStream(100))
 *
 * Construct an instance of TransportClient using {@link TransportClientFactory}. A single
 * TransportClient may be used for multiple streams, but any given stream must be restricted to a
 * single client, in order to avoid out-of-order responses.
 *
 * NB: This class is used to make requests to the server, while {@link TransportResponseHandler} is
 * responsible for handling responses from the server.
 *
 * Concurrency: thread safe and can be called from multiple threads.
 *
 * 客户端用于获取预先协商的流的连续块。
 * 这个API旨在允许大量数据的高效传输，分解成大小从几百kb到几MB的块。
 *
 * 请注意，虽然此客户端处理从流（即数据平面）获取块的操作，但实际的流设置是在传输层范围之外完成的。
 * 提供了便捷方法“sendrpc”来启用客户端和服务器之间的控制平面通信来执行此设置。
 *
 * 例如，典型的工作流程可能是：
 * client.sendRPC(new OpenFile("/foo")) --&gt; returns StreamId = 100
 * client.fetchChunk(streamId = 100, chunkIndex = 0, callback)
 * client.fetchChunk(streamId = 100, chunkIndex = 1, callback)
 * ...
 * client.sendRPC(new CloseStream(100))
 *
 * 使用transportclientfactory构造一个transportclient实例。
 * 一个传输客户端可以用于多个流，但是任何给定的流必须被限制到单个客户端，以避免乱序响应。
 *
 * NB：这个类用于向服务器发送请求，而transportresponsehandler负责处理来自服务器的响应。
 *
 * 并发性：线程安全，可以从多个线程中调用。
 *
 *
 * art_of_spark:
 * RPC框架的客户端用于获取预先协商好的流中的数据块。
 * TransportClient旨在允许有效传输大量数据，这些数据将被拆分成几百KB到几MB的块。
 * 当TransportClient处理从流中获取的块时，实际的设置是在传输层之外完成的。
 * sendRPC方法能够在客户端和服务端的同一水平线的通信进行这些设置
 *
 */
public class TransportClient implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(TransportClient.class);

  private final Channel channel;
  private final TransportResponseHandler handler;
  @Nullable private String clientId;
  private volatile boolean timedOut;

  public TransportClient(Channel channel, TransportResponseHandler handler) {
    this.channel = Preconditions.checkNotNull(channel);
    this.handler = Preconditions.checkNotNull(handler);
    this.timedOut = false;
  }

  public Channel getChannel() {
    return channel;
  }

  public boolean isActive() {
    return !timedOut && (channel.isOpen() || channel.isActive());
  }

  public SocketAddress getSocketAddress() {
    return channel.remoteAddress();
  }

  /**
   * Returns the ID used by the client to authenticate itself when authentication is enabled.
   *
   * @return The client ID, or null if authentication is disabled.
   */
  public String getClientId() {
    return clientId;
  }

  /**
   * Sets the authenticated client ID. This is meant to be used by the authentication layer.
   *
   * Trying to set a different client ID after it's been set will result in an exception.
   */
  public void setClientId(String id) {
    Preconditions.checkState(clientId == null, "Client ID has already been set.");
    this.clientId = id;
  }

  /**
   * Requests a single chunk from the remote side, from the pre-negotiated streamId.
   *
   * Chunk indices go from 0 onwards. It is valid to request the same chunk multiple times, though
   * some streams may not support this.
   *
   * Multiple fetchChunk requests may be outstanding simultaneously, and the chunks are guaranteed
   * to be returned in the same order that they were requested, assuming only a single
   * TransportClient is used to fetch the chunks.
   *
   * 从预先协商的streamid请求远程端的单个块。
   *
   * 大块指数从0开始。多次请求相同的块是有效的，但有些流可能不支持这种情况。
   *
   * 假设只有一个传输客户端用于获取块，多个fetchchunk请求可能同时处于未完成状态，
   * 并且保证块将按照请求的相同顺序返回。
   *
   * 请求发送成功后，客户端将等待接收服务端的响应，
   * 返回的消息会传递给TransportChannelHandler的channelRead方法，
   * 消息最后交给TransportResponseHandler的handle方法来处理。
   *
   * @param streamId Identifier that refers to a stream in the remote StreamManager. This should
   *                 be agreed upon by client and server beforehand.
   * @param chunkIndex 0-based index of the chunk to fetch
   * @param callback Callback invoked upon successful receipt of chunk, or upon any failure.
   */
  public void fetchChunk(
      long streamId,
      final int chunkIndex,
      final ChunkReceivedCallback callback) {
    final long startTime = System.currentTimeMillis();
    if (logger.isDebugEnabled()) {
      logger.debug("Sending fetch chunk request {} to {}", chunkIndex, getRemoteAddress(channel));
    }

    // 使用流的标记streamId和块的索引chunkIndex创建StreamChunkId
    final StreamChunkId streamChunkId = new StreamChunkId(streamId, chunkIndex);
    // 向handler添加StreamChunkId与回调类ChunkReceivedCallback的引用之间的关系
    handler.addFetchRequest(streamChunkId, callback);

    // 调用Channel的writeAndFlush方法将块请求发送出去
    channel.writeAndFlush(new ChunkFetchRequest(streamChunkId)).addListener(
      new ChannelFutureListener() {
        /**
         * 当发送成功或者失败会回调该方法
         */
        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
          if (future.isSuccess()) {
            long timeTaken = System.currentTimeMillis() - startTime;
            if (logger.isTraceEnabled()) {
              logger.trace("Sending request {} to {} took {} ms", streamChunkId,
                getRemoteAddress(channel), timeTaken);
            }
          } else {
            String errorMsg = String.format("Failed to send request %s to %s: %s", streamChunkId,
              getRemoteAddress(channel), future.cause());
            logger.error(errorMsg, future.cause());
            // 将此次请求从outstandingFetches缓存中移除
            handler.removeFetchRequest(streamChunkId);
            channel.close();
            try {
              callback.onFailure(chunkIndex, new IOException(errorMsg, future.cause()));
            } catch (Exception e) {
              logger.error("Uncaught exception in RPC response callback handler!", e);
            }
          }
        }
      });
  }

  /**
   * Request to stream the data with the given stream ID from the remote end.
   *
   * @param streamId The stream to fetch.
   * @param callback Object to call with the stream data.
   */
  public void stream(final String streamId, final StreamCallback callback) {
    final long startTime = System.currentTimeMillis();
    if (logger.isDebugEnabled()) {
      logger.debug("Sending stream request for {} to {}", streamId, getRemoteAddress(channel));
    }

    // Need to synchronize here so that the callback is added to the queue and the RPC is
    // written to the socket atomically, so that callbacks are called in the right order
    // when responses arrive.
    synchronized (this) {
      handler.addStreamCallback(callback);
      channel.writeAndFlush(new StreamRequest(streamId)).addListener(
        new ChannelFutureListener() {
          @Override
          public void operationComplete(ChannelFuture future) throws Exception {
            if (future.isSuccess()) {
              long timeTaken = System.currentTimeMillis() - startTime;
              if (logger.isTraceEnabled()) {
                logger.trace("Sending request for {} to {} took {} ms", streamId,
                  getRemoteAddress(channel), timeTaken);
              }
            } else {
              String errorMsg = String.format("Failed to send request for %s to %s: %s", streamId,
                getRemoteAddress(channel), future.cause());
              logger.error(errorMsg, future.cause());
              channel.close();
              try {
                callback.onFailure(streamId, new IOException(errorMsg, future.cause()));
              } catch (Exception e) {
                logger.error("Uncaught exception in RPC response callback handler!", e);
              }
            }
          }
        });
    }
  }

  /**
   * Sends an opaque message to the RpcHandler on the server-side. The callback will be invoked
   * with the server's response or upon any failure.
   *
   * 向服务器端的RpcHandler发送一个不透明的消息。 回调将在服务器的响应或任何失败时调用。
   *
   * 通过 At least Once Delivery 原则保证请求不会丢失
   *
   * 请求发送成功后，客户端将等待接收服务端的响应，
   * 返回的消息会传递给TransportChannelHandler的channelRead方法，
   * 消息最后交给TransportResponseHandler的handle方法来处理。
   *
   * @param message The message to send.
   * @param callback Callback to handle the RPC's reply.
   * @return The RPC's id.
   */
  public long sendRpc(ByteBuffer message, final RpcResponseCallback callback) {
    final long startTime = System.currentTimeMillis();
    if (logger.isTraceEnabled()) {
      logger.trace("Sending RPC to {}", getRemoteAddress(channel));
    }

    // 使用UUID生成请求主键requestId
    final long requestId = Math.abs(UUID.randomUUID().getLeastSignificantBits());
    // 添加requestId与RpcResponseCallback的引用之间的关系
    handler.addRpcRequest(requestId, callback);

    // 发送RPC请求
    channel.writeAndFlush(new RpcRequest(requestId, new NioManagedBuffer(message))).addListener(
      new ChannelFutureListener() {
        /**
         * 当发送成功或者失败会回调该方法
         */
        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
          if (future.isSuccess()) {
            long timeTaken = System.currentTimeMillis() - startTime;
            if (logger.isTraceEnabled()) {
              logger.trace("Sending request {} to {} took {} ms", requestId,
                getRemoteAddress(channel), timeTaken);
            }
          } else {
            String errorMsg = String.format("Failed to send RPC %s to %s: %s", requestId,
              getRemoteAddress(channel), future.cause());
            logger.error(errorMsg, future.cause());
            // 将此次请求从outstandingRpcs缓存中移除
            handler.removeRpcRequest(requestId);
            channel.close();
            try {
              callback.onFailure(new IOException(errorMsg, future.cause()));
            } catch (Exception e) {
              logger.error("Uncaught exception in RPC response callback handler!", e);
            }
          }
        }
      });

    return requestId;
  }

  /**
   * Synchronously sends an opaque message to the RpcHandler on the server-side, waiting for up to
   * a specified timeout for a response.
   *
   * 向服务端发送异步的RPC请求，并根据指定的超时时间等待
   */
  public ByteBuffer sendRpcSync(ByteBuffer message, long timeoutMs) {
    final SettableFuture<ByteBuffer> result = SettableFuture.create();

    sendRpc(message, new RpcResponseCallback() {
      @Override
      public void onSuccess(ByteBuffer response) {
        ByteBuffer copy = ByteBuffer.allocate(response.remaining());
        copy.put(response);
        // flip "copy" to make it readable
        copy.flip();
        result.set(copy);
      }

      @Override
      public void onFailure(Throwable e) {
        result.setException(e);
      }
    });

    try {
      return result.get(timeoutMs, TimeUnit.MILLISECONDS);
    } catch (ExecutionException e) {
      throw Throwables.propagate(e.getCause());
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Sends an opaque message to the RpcHandler on the server-side. No reply is expected for the
   * message, and no delivery guarantees are made.
   *
   * 向服务端发送RPC的请求，但是并不期望能够获取响应，因而不能保证投递的可靠性。
   * @param message The message to send.
   */
  public void send(ByteBuffer message) {
    channel.writeAndFlush(new OneWayMessage(new NioManagedBuffer(message)));
  }

  /**
   * Removes any state associated with the given RPC.
   *
   * @param requestId The RPC id returned by {@link #sendRpc(ByteBuffer, RpcResponseCallback)}.
   */
  public void removeRpcRequest(long requestId) {
    handler.removeRpcRequest(requestId);
  }

  /** Mark this channel as having timed out. */
  public void timeOut() {
    this.timedOut = true;
  }

  @VisibleForTesting
  public TransportResponseHandler getHandler() {
    return handler;
  }

  @Override
  public void close() {
    // close is a local operation and should finish with milliseconds; timeout just to be safe
    channel.close().awaitUninterruptibly(10, TimeUnit.SECONDS);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("remoteAdress", channel.remoteAddress())
      .add("clientId", clientId)
      .add("isActive", isActive())
      .toString();
  }
}
