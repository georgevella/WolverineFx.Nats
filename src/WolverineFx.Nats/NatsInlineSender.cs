using NATS.Client.JetStream;
using Wolverine;
using Wolverine.Runtime;
using Wolverine.Transports.Sending;

namespace WolverineFx.Nats;

public class NatsInlineSender(NatsStreamEndpoint natsStreamEndpoint, IWolverineRuntime runtime, NatsJSContext js)
    : ISender
{
    private readonly CancellationToken _cancellationToken = runtime.Cancellation;
    private readonly NatsEnvelopeMapper _mapper = natsStreamEndpoint.Mapper;

    public async Task<bool> PingAsync()
    {
        try
        {
            await SendAsync(Envelope.ForPing(Destination));
            return true;
        }
        catch (Exception)
        {
            return false;
        }
    }

    public async ValueTask SendAsync(Envelope envelope)
    {
        var (m, outgoingEnvelope) = EnvelopeConverter.Convert(_mapper, envelope);

        var subject =
            $"{natsStreamEndpoint.SubjectPrefix}.{envelope.TopicName ?? natsStreamEndpoint.DefaultSubject}";
        var r = await js.PublishAsync(
            subject, 
            m, 
            headers: outgoingEnvelope.Headers, 
            cancellationToken: _cancellationToken);

        if (r.Error != null)
            throw new InvalidOperationException(
                $"{r.Error.Code}:{r.Error.ErrCode} - {r.Error.Description ?? "n/a"}"
            );
    }

    public bool SupportsNativeScheduledSend => false;
    public Uri Destination { get; } = natsStreamEndpoint.Uri;
}