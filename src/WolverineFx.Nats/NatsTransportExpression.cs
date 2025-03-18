using Wolverine;
using Wolverine.Transports;

namespace WolverineFx.Nats;

public class NatsTransportExpression
    : BrokerExpression<NatsJetStreamTransport, 
        NatsStreamEndpoint, 
        NatsStreamEndpoint, 
        NatsJetStreamListenerConfiguration, 
        NatsJetStreamPublisherConfiguration, 
        NatsTransportExpression>
{
    public NatsTransportExpression(NatsJetStreamTransport transport, WolverineOptions options) : base(transport, options)
    {
    }

    protected override NatsJetStreamListenerConfiguration createListenerExpression(NatsStreamEndpoint listenerEndpoint)
    {
        return new NatsJetStreamListenerConfiguration(listenerEndpoint);
    }

    protected override NatsJetStreamPublisherConfiguration createSubscriberExpression(NatsStreamEndpoint subscriberEndpoint)
    {
        return new NatsJetStreamPublisherConfiguration(subscriberEndpoint);
    }
}