using NATS.Client.Core;

namespace WolverineFx.Nats;

public class OutgoingNatsEnvelope
{
    public NatsHeaders Headers { get; } = new();
}

