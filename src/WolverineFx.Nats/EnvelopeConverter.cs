using System.Text;
using Wolverine;

namespace WolverineFx.Nats;

public static class EnvelopeConverter
{
    public static (WolverineFxMessage Message, OutgoingNatsEnvelope Envelope) Convert(
        NatsEnvelopeMapper mapper, 
        Envelope envelope
        )
    {
        var outgoingEnvelope = new OutgoingNatsEnvelope();

        mapper.MapEnvelopeToOutgoing(envelope, outgoingEnvelope);

        WolverineFxMessage m;

        if (envelope.Data != null && envelope.Data.Any())
        {
            m = new(envelope.MessageType,
                Encoding.Default.GetString(envelope.Data),
                null,
                envelope.PartitionKey,
                envelope.TopicName
            );
        }
        else if (envelope.Message != null)
        {
            m = new(envelope.MessageType, null, envelope.Message,
                envelope.PartitionKey,
                envelope.TopicName);
        }
        else
        {
            throw new InvalidOperationException(
                $"Envelope {envelope.Id} has neither {nameof(envelope.Data)} nor {nameof(envelope.Message)}");
        }
        
        return (m, outgoingEnvelope);
    }
}