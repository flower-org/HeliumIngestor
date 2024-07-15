package com.helium.ingestor.videoservice;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.CharsetUtil;

import javax.annotation.Nullable;
import java.util.Base64;

public class HttpBasicAuthHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
    @Nullable private final String username;
    @Nullable private final String password;

    public HttpBasicAuthHandler(@Nullable String username, @Nullable String password) {
        this.username = username;
        this.password = password;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) throws Exception {
        if (username != null && password != null) {
            HttpHeaders headers = request.headers();
            String authHeader = headers.get(HttpHeaderNames.AUTHORIZATION);

            if (authHeader == null || !authHeader.startsWith("Basic ")) {
                sendUnauthorized(ctx);
                return;
            }

            String base64Credentials = authHeader.substring("Basic".length()).trim();
            String credentials = new String(Base64.getDecoder().decode(base64Credentials), CharsetUtil.UTF_8);

            String[] values = credentials.split(":", 2);
            if (values.length != 2 || !username.equals(values[0]) || !password.equals(values[1])) {
                sendUnauthorized(ctx);
                return;
            }
        }

        // Forward to the next handler in the pipeline
        ctx.fireChannelRead(request.retain());
    }

    private void sendUnauthorized(ChannelHandlerContext ctx) {
        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.UNAUTHORIZED);
        response.headers().set(HttpHeaderNames.WWW_AUTHENTICATE, "Basic realm=\"Access to the site\"");
        response.content().writeBytes("Unauthorized".getBytes(CharsetUtil.UTF_8));
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");

        ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
}