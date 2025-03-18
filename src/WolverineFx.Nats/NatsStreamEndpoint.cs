using Microsoft.CSharp.RuntimeBinder;
using Microsoft.Extensions.Logging;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using Wolverine.Configuration;
using Wolverine.Runtime;
using Wolverine.Transports;
using Wolverine.Transports.Sending;

namespace WolverineFx.Nats;

public class NatsStreamEndpoint : Endpoint, IBrokerEndpoint
{
    private readonly NatsJetStreamTransport _transport;
    private readonly string _streamName;
    private INatsJSStream? _natsStream = null;
    private readonly CancellationTokenSource _cts = new CancellationTokenSource();

    public NatsStreamEndpoint(NatsJetStreamTransport transport, string streamName, EndpointRole role) : base(
        new Uri($"{transport.Protocol}://{streamName}"), role)
    {
        _transport = transport;
        _streamName = streamName;

        Mapper = new NatsEnvelopeMapper(this);
    }

    internal NatsEnvelopeMapper Mapper { get; }
    
    public override ValueTask<IListener> BuildListenerAsync(IWolverineRuntime runtime, IReceiver receiver)
    {
        var js = new NatsJSContext(_transport.GetConnection());
        var l = new NatsListener(
            this,
            runtime,
            js,
            receiver, runtime.LoggerFactory.CreateLogger<NatsListener>());

        return ValueTask.FromResult<IListener>(l);
    }

    internal string StreamName => _streamName;
    internal string DefaultSubject { get; set; } = "generic";

    internal string SubjectPrefix { get; set; } = "messagebus";
    internal string ConsumerName { get; set; }


    protected override ISender CreateSender(IWolverineRuntime runtime)
    {
        var js = new NatsJSContext(_transport.GetConnection());
        // return new BatchedSender(
        //     this,
        //     new NatsSenderProtocol(this, js, _cts.Token),
        //     runtime.Cancellation,
        //     runtime.LoggerFactory.CreateLogger<NatsSenderProtocol>()
        // );
        
        return new NatsInlineSender(this, runtime, js);
    }

    public async ValueTask<bool> CheckAsync()
    {
        var js = new NatsJSContext(_transport.GetConnection());
        try
        {
            await js.PublishAsync($"{SubjectPrefix}.ping", new WolverineFxMessage(IsPing: true));

            return true;
        }
        catch
        {
            return false;
        }
    }

    public ValueTask TeardownAsync(ILogger logger)
    {
        return ValueTask.CompletedTask;
    }

    public async ValueTask SetupAsync(ILogger logger)
    {
        var js = new NatsJSContext(_transport.GetConnection());

        try
        {
            _natsStream = await js.GetStreamAsync(_streamName);
        }
        catch (Exception ex)
        {
            _natsStream = await js.CreateStreamAsync(new StreamConfig(
                    _streamName,
                    new List<string>()
                    {
                        $"{SubjectPrefix}.*"
                    }
                )
            );
        }
    }
}