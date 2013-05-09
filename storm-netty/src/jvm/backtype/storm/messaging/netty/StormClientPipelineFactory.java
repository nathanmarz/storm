package backtype.storm.messaging.netty;

import java.util.Map;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;

class StormClientPipelineFactory implements ChannelPipelineFactory {
    private Client client;
    @SuppressWarnings("rawtypes")
    private Map conf;

    @SuppressWarnings("rawtypes")
    StormClientPipelineFactory(Client client, Map conf) {
        this.client = client;        
        this.conf = conf;
    }

    public ChannelPipeline getPipeline() throws Exception {
        // Create a default pipeline implementation.
        ChannelPipeline pipeline = Channels.pipeline();

        // Decoder
        pipeline.addLast("decoder", new MessageDecoder());
        // Encoder
        pipeline.addLast("encoder", new MessageEncoder(conf));
        // business logic.
        pipeline.addLast("handler", new StormClientHandler(client));

        return pipeline;
    }
}
