/*
 * Copyright (c) 2011, Sho SHIMIZU
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following
 * conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.galibier;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.codec.oneone.OneToOneDecoder;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;
import org.openflow.protocol.*;
import org.openflow.protocol.action.OFAction;
import org.openflow.protocol.action.OFActionOutput;
import org.openflow.protocol.factory.BasicFactory;
import org.openflow.protocol.factory.OFMessageFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;

public class SimpleController {
    //  according to the ofp_header.length, the length of the field is 2 byte.
    public static final int MAXIMUM_PACKET_LENGTH = 65535;
    public static final int LENGTH_FIELD_OFFSET = 2;
    public static final int LENGTH_FIELD_LENGTH = 2;
    public static final int LENGTH_FIELD_MODIFICATION = -4;
    public static final int CONTROLLER_DEFAULT_PORT = 6633;

    private static final Logger log = LoggerFactory.getLogger(SimpleController.class);
    private static final OFMessageFactory factory = new BasicFactory();

    private final CopyOnWriteArrayList<MessageListener> listeners =
            new CopyOnWriteArrayList<MessageListener>();

    public void addMessageListener(MessageListener listener) {
        //  avoid duplication
        listeners.addIfAbsent(listener);
    }

    public void removeMessageListener(MessageListener listener) {
        listeners.remove(listener);
    }

    public void invokeMessageListeners(Channel channel, OFMessage msg) {
        for (MessageListener listener: listeners) {
            listener.messageReceived(channel, msg);
        }
    }

    public static interface MessageListener {
        public void messageReceived(Channel channel, OFMessage msg);
    }

    private static class OpenFlowDecoder extends OneToOneDecoder {
        @Override
        protected Object decode(ChannelHandlerContext ctx, Channel channel, Object msg) throws Exception {
            if (!(msg instanceof ChannelBuffer)) {
                return msg;
            }

            ChannelBuffer channelBuffer = (ChannelBuffer)msg;
            ByteBuffer byteBuffer = channelBuffer.toByteBuffer();
            List<OFMessage> messages = factory.parseMessages(byteBuffer);
            return messages.get(0);
        }
    }

    private static class OpenFlowEncoder extends OneToOneEncoder {
        @Override
        protected Object encode(ChannelHandlerContext ctx, Channel channel, Object msg) throws Exception {
            if (msg instanceof OFMessage) {
                OFMessage response = (OFMessage) msg;
                //  may have an bad effect on performance
                ByteBuffer buffer = ByteBuffer.allocate(response.getLength());
                response.writeTo(buffer);
                buffer.flip();
                return ChannelBuffers.wrappedBuffer(buffer);
            }

            return ChannelBuffers.EMPTY_BUFFER;
        }
    }

    private static class OpenFlowServerPipelineFactory implements ChannelPipelineFactory {
        private final SimpleController controller;

        public OpenFlowServerPipelineFactory(SimpleController controller) {
            this.controller = controller;
        }

        public ChannelPipeline getPipeline() throws Exception {
            ChannelPipeline pipeline = Channels.pipeline();

            //  add the binary codec combination first
            pipeline.addLast("framer", new LengthFieldBasedFrameDecoder(
                    MAXIMUM_PACKET_LENGTH, LENGTH_FIELD_OFFSET, LENGTH_FIELD_LENGTH, LENGTH_FIELD_MODIFICATION, 0));
            pipeline.addLast("decoder", new OpenFlowDecoder());
            pipeline.addLast("encoder", new OpenFlowEncoder());

            //  add then the business logic
            pipeline.addLast("handler", new OpenFlowSimpleControllerHandler(controller));

            return pipeline;
        }
    }

    private static class OpenFlowSimpleControllerHandler extends SimpleChannelUpstreamHandler {
        private final SimpleController controller;

        public OpenFlowSimpleControllerHandler(SimpleController controller) {
            this.controller = controller;
        }

        @Override
        public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
            if (e.getMessage() instanceof OFMessage) {
                OFMessage in = (OFMessage)e.getMessage();
                log.debug("Message received: message({}), switch({})", in.getType(), ctx.getChannel().getRemoteAddress());
                switch (in.getType()) {
                    case HELLO:
                        ctx.getChannel().write(factory.getMessage(OFType.FEATURES_REQUEST));
                    case ECHO_REQUEST:
                        int xid = in.getXid();
                        OFMessage out = factory.getMessage(OFType.ECHO_REPLY);
                        out.setXid(xid);
                        //  replies an ECHO_REPLY message
                        ctx.getChannel().write(out);
                        break;
                    case FEATURES_REPLY:
                        log.info("Handshake completed: switch({})", ctx.getChannel().getRemoteAddress());
                        break;
                    case ERROR:
                        OFError error = (OFError)in;
                        log.warn("Error occurred: type({}), switch({})", error.getErrorType(), ctx.getChannel().getRemoteAddress());
                        break;
                    default:
                        controller.invokeMessageListeners(ctx.getChannel(), in);
                }
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
            log.warn("Exception occurred: {}", e.getCause());
            ctx.getChannel().close();
        }

        @Override
        public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
            OFMessage out = factory.getMessage(OFType.HELLO);
            ctx.getChannel().write(out);
        }

        @Override
        public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
            log.info("Switch disconnected (Switch: {})", ctx.getChannel().getRemoteAddress());
        }
    }

    public static void main(String[] args) {
        SimpleController controller = new SimpleController();
        ChannelFactory channelFactory = new NioServerSocketChannelFactory(
                Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool()
        );
        ServerBootstrap bootstrap = new ServerBootstrap(channelFactory);
        bootstrap.setPipelineFactory(new OpenFlowServerPipelineFactory(controller));
        bootstrap.setOption("reuseAddress", true);
        bootstrap.setOption("child.tcpNoDelay", true);
        bootstrap.setOption("child.keepAlive", true);

        int port = CONTROLLER_DEFAULT_PORT;
        if (args.length >= 1) {
            port = Integer.parseInt(args[0]);
        }

        Channel channel = bootstrap.bind(new InetSocketAddress(port));
        log.info("Controller started: {}", channel.getLocalAddress());

        //  act as a dumb hub / repeater hub
        MessageListener hub = new MessageListener() {
            public void messageReceived(Channel channel, OFMessage msg) {
                if (msg.getType() == OFType.PACKET_IN) {
                    OFPacketIn in = (OFPacketIn)msg;
                    OFPacketOut out = (OFPacketOut)factory.getMessage(OFType.PACKET_OUT);
                    out.setBufferId(in.getBufferId());
                    out.setInPort(in.getInPort());

                    //  action for flooding
                    OFActionOutput action = new OFActionOutput();
                    action.setPort(OFPort.OFPP_FLOOD.getValue());
                    out.setActions(Collections.singletonList((OFAction)action));
                    out.setActionsLength((short)OFActionOutput.MINIMUM_LENGTH);

                    //  set data using the incoming packet
                    if (in.getBufferId() == 0xffffffff) {
                        byte[] packetData = in.getPacketData();
                        out.setLength((short)(OFPacketOut.MINIMUM_LENGTH + out.getActionsLength() + packetData.length));
                        out.setPacketData(packetData);
                    } else {
                        out.setLength((short)(OFPacketOut.MINIMUM_LENGTH + out.getActionsLength()));
                    }

                    channel.write(out);
                    log.debug("Message sent: message({}), switch({})", out.getType(), channel.getRemoteAddress());
                }
            }
        };
        controller.addMessageListener(hub);
    }
}
