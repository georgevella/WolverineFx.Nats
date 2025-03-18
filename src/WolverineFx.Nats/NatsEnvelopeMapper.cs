using Microsoft.Extensions.Primitives;
using NATS.Client.JetStream;
using Wolverine.Configuration;
using Wolverine.Transports;

namespace WolverineFx.Nats;

public class NatsEnvelopeMapper(Endpoint endpoint)
    : EnvelopeMapper<NatsJSMsg<WolverineFxMessage>, OutgoingNatsEnvelope>(endpoint)
{
    protected override void writeOutgoingHeader(OutgoingNatsEnvelope outgoing, string key, string value)
    {
        outgoing.Headers?.Add(key, new StringValues(value));
    }

    protected override bool tryReadIncomingHeader(NatsJSMsg<WolverineFxMessage> incoming, string key, out string? value)
    {
        if (incoming.Headers?.TryGetValue(key, out var values) ?? false)
        {
            value = values.FirstOrDefault();
            return true;
        }

        value = null;
        return false;
    }
}