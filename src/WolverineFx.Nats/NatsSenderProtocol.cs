using System.Text;
using NATS.Client.JetStream;
using Wolverine.Transports;
using Wolverine.Transports.Sending;

namespace WolverineFx.Nats;

public class NatsSenderProtocol(NatsStreamEndpoint natsStreamEndpoint, NatsJSContext js, CancellationToken cancellationToken) : ISenderProtocol
{
    public async Task SendBatchAsync(ISenderCallback callback, OutgoingMessageBatch batch)
    {
        var mapper = natsStreamEndpoint.Mapper;
        foreach (var envelope in batch.Messages)
        {
            var (m, outgoingEnvelope) = EnvelopeConverter.Convert(mapper, envelope);

            var subject =
                $"{natsStreamEndpoint.SubjectPrefix}.{envelope.TopicName ?? natsStreamEndpoint.DefaultSubject}";
            var r = await js.PublishAsync(
                subject, 
                m, 
                headers: outgoingEnvelope.Headers, 
                cancellationToken: cancellationToken);

            if (r.Error != null)
                throw new InvalidOperationException(
                    $"{r.Error.Code}:{r.Error.ErrCode} - {r.Error.Description ?? "n/a"}"
                );
        }

        await callback.MarkSuccessfulAsync(batch);
    }
}