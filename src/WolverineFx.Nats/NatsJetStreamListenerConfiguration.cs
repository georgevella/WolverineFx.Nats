using Wolverine.Configuration;

namespace WolverineFx.Nats;

public class
    NatsJetStreamListenerConfiguration : ListenerConfiguration<NatsJetStreamListenerConfiguration, NatsStreamEndpoint>
{
    public NatsJetStreamListenerConfiguration(NatsStreamEndpoint endpoint) : base(endpoint)
    {
    }

    public NatsJetStreamListenerConfiguration(Func<NatsStreamEndpoint> source) : base(source)
    {
    }

    public NatsJetStreamListenerConfiguration WithConsumerName(string consumerName)
    {
        add(
            e => { e.ConsumerName = consumerName.Replace('.', '-').ToLower(); }
        );
        return this;
    }
}