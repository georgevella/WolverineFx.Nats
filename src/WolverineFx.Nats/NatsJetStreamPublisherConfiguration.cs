using Wolverine.Configuration;

namespace WolverineFx.Nats;

public class NatsJetStreamPublisherConfiguration(NatsStreamEndpoint endpoint)
    : SubscriberConfiguration<NatsJetStreamPublisherConfiguration, NatsStreamEndpoint>(endpoint);