using JasperFx.Core;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NATS.Client.Serializers.Json;
using Wolverine.Configuration;
using Wolverine.Runtime;
using Wolverine.Transports;

namespace WolverineFx.Nats;

public class NatsJetStreamTransport : BrokerTransport<NatsStreamEndpoint>
{
    private readonly Cache<string, NatsStreamEndpoint> _endpoints;
    private NatsConnection? _natsConnection = null;

    public NatsJetStreamTransport() : base("natsjs", "Nats JetStreams")
    {
        this._endpoints = new Cache<string, NatsStreamEndpoint>(
            stream => new NatsStreamEndpoint(this, stream, EndpointRole.Application)
        );
    }

    public string NatsUrl { get; set; } = "nats://127.0.0.1:4222";

    protected override IEnumerable<NatsStreamEndpoint> endpoints()
    {
        return _endpoints;
    }

    protected override NatsStreamEndpoint findEndpointByUri(Uri uri)
    {
        var streamName = uri.Host;
        return _endpoints[streamName];
    }
    
    internal NatsStreamEndpoint GetEndpointByName(string name) => _endpoints[name];

    public override ValueTask ConnectAsync(IWolverineRuntime runtime)
    {
        _natsConnection = new NatsConnection(new NatsOpts()
        {
            Url = NatsUrl,
            SerializerRegistry = NatsJsonSerializerRegistry.Default,
            LoggerFactory = runtime.LoggerFactory,
        });
        
        foreach (var natsStreamEndpoint in endpoints())
        {
            natsStreamEndpoint.Compile(runtime);
        }
        
        return ValueTask.CompletedTask;
    }

    public override IEnumerable<PropertyColumn> DiagnosticColumns()
    {
        yield break;
    }

    internal NatsConnection GetConnection()
    {
        return _natsConnection ?? throw new InvalidOperationException("The NatsConnection has not been initialized.");
    }
}