package com.helium.ingestor.videoservice;
/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

import com.helium.ingestor.config.Config;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;

import javax.annotation.Nullable;
import java.io.File;

public final class HttpStaticFileServer {
  @Nullable EventLoopGroup bossGroup;
  @Nullable EventLoopGroup workerGroup;
  final int bossGroupThreadCount;
  final int workerGroupThreadCount;

    public HttpStaticFileServer() {
        this(1, 1);
    }

    public HttpStaticFileServer(int bossGroupThreadCount, int workerGroupThreadCount) {
      this.bossGroupThreadCount = bossGroupThreadCount;
      this.workerGroupThreadCount = workerGroupThreadCount;
    }

  public static void main(String[] args) throws Exception {
    File rootFolder = new File("/home/john");
    HttpStaticFileServer staticFileServer = new HttpStaticFileServer(1, 1);
    try {
      Channel ch = staticFileServer.startServer(rootFolder, false, 8080, null);
      ch.closeFuture().sync();
    } finally {
      staticFileServer.stopServer();
    }
  }

  public Channel startServer(File rootFolder, boolean ssl, int port,
                             @Nullable Config.Credentials credentials) throws Exception {
    // Configure SSL.
    final SslContext sslCtx = ServerUtil.buildSslContext();

    EventLoopGroup bossGroup = new NioEventLoopGroup();
    EventLoopGroup workerGroup = new NioEventLoopGroup();

    ServerBootstrap b = new ServerBootstrap();
    b.group(bossGroup, workerGroup)
      .channel(NioServerSocketChannel.class)
      .handler(new LoggingHandler(LogLevel.DEBUG))
      .childHandler(new HttpStaticFileServerInitializer(sslCtx, rootFolder, credentials));

     return b.bind(port).sync().channel();
  }

  public void stopServer() throws Exception {
    if (bossGroup != null) { bossGroup.shutdownGracefully(); }
    if (workerGroup != null) { workerGroup.shutdownGracefully(); }
  }
}