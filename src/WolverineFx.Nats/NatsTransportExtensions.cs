using JasperFx.Core.Reflection;
using Wolverine;
using Wolverine.Configuration;

namespace WolverineFx.Nats;

public static class NatsTransportExtensions
{
    /// <summary>
    ///     Quick access to the Kafka Transport within this application.
    ///     This is for advanced usage
    /// </summary>
    /// <param name="endpoints"></param>
    /// <returns></returns>
    internal static NatsJetStreamTransport GetNatsTransport(this WolverineOptions endpoints)
    {
        var transports = endpoints.As<WolverineOptions>().Transports;

        return transports.GetOrCreate<NatsJetStreamTransport>();
    }

    /// <summary>
    /// Add a connection to an Kafka broker within this application
    /// </summary>
    /// <param name="options"></param>
    /// <param name="natsUrl"></param>
    /// <returns></returns>
    public static NatsTransportExpression UseNats(this WolverineOptions options, string natsUrl)
    {
        var transport = options.GetNatsTransport();
        transport.NatsUrl = natsUrl;
        
        return new NatsTransportExpression(transport, options);
    }
    
    public static NatsJetStreamPublisherConfiguration ToNatsStream(
        this IPublishToExpression publishing,
        string streamName,
        Action<NatsStreamEndpoint>? configure = null
    )
    {
        var transports = publishing.As<PublishingExpression>().Parent.Transports;
        var transport = transports.GetOrCreate<NatsJetStreamTransport>();
        var endpoint = transport.GetEndpointByName(streamName);
        
        configure?.Invoke(endpoint);
        publishing.To(endpoint.Uri);

        return new NatsJetStreamPublisherConfiguration(endpoint);
    }
    
    public static NatsJetStreamListenerConfiguration ListenToNatsStream(this WolverineOptions options, string streamName)
    {
        var transport = options.GetNatsTransport();
        
        var endpoint = transport.GetEndpointByName(streamName);
        endpoint.EndpointName = streamName;
        endpoint.IsListener = true;

        return new NatsJetStreamListenerConfiguration(endpoint);
    }
}