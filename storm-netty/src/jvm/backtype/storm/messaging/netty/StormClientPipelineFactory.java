package backtype.storm.messaging.netty;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;

class StormClientPipelineFactory implements ChannelPipelineFactory {
    private Client client;

    StormClientPipelineFactory(Client client) {
        this.client = client;        
    }

    public ChannelPipeline getPipeline() throws Exception {
        // Create a default pipeline implementation.
        ChannelPipeline pipeline = Channels.pipeline();

        // Decoder
        pipeline.addLast("decoder", new MessageDecoder());
        // Encoder
        pipeline.addLast("encoder", new MessageEncoder());
        // business logic.
        pipeline.addLast("handler", new StormClientHandler(client));

        return pipeline;
    }
}
