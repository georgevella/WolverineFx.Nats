using System.Text;
using JasperFx.Core;
using Microsoft.Extensions.Logging;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using Wolverine;
using Wolverine.Runtime;
using Wolverine.Transports;

namespace WolverineFx.Nats;

public class NatsListener : IListener, IDisposable
{
    private readonly Task _runner;
    private readonly CancellationTokenSource _stopSignal = new();

    public NatsListener(NatsStreamEndpoint natsStreamEndpoint,
        IWolverineRuntime runtime,
        NatsJSContext js,
        IReceiver receiver,
        ILogger<NatsListener> log)
    {
        Address = natsStreamEndpoint.Uri;
        
        _runner = Task.Run(async () =>
        {
            var mapper = natsStreamEndpoint.Mapper;

            var consumerName = runtime.Options.ServiceName
                .Replace('.', '-')
                .ToLower();
            
            if (!string.IsNullOrEmpty(natsStreamEndpoint.ConsumerName))
                consumerName = natsStreamEndpoint.ConsumerName;
            
            var consumer = await js.CreateOrUpdateConsumerAsync(
                natsStreamEndpoint.StreamName,
                new ConsumerConfig(consumerName)
                {
                    AckPolicy = ConsumerConfigAckPolicy.Explicit,
                    DurableName = consumerName,
                    AckWait = TimeSpan.FromSeconds(30),
                    MaxAckPending = 1000,
                    DeliverPolicy = ConsumerConfigDeliverPolicy.All,
                    DeliverGroup = consumerName
                });
            
            
            while (!_stopSignal.IsCancellationRequested)
            {
                var asyncConsumer = consumer.ConsumeAsync<WolverineFxMessage>(
                    cancellationToken: _stopSignal.Token
                );
            
                await foreach (var f in asyncConsumer)
                {
                    if (_stopSignal.IsCancellationRequested)
                        break;
                
                    await f.AckProgressAsync();

                    if (f.Data != null)
                    {
                        var envelope = new Envelope
                        {
                            PartitionKey = f.Data.PartitionKey,
                            TopicName = f.Data.TopicName
                        };

                        if (f.Data.DataPayload != null)
                        {
                            envelope.Data = Encoding.Default.GetBytes(f.Data.DataPayload);
                        }
                        else if (f.Data.MessagePayload != null)
                        {
                            envelope.Message = f.Data.MessagePayload;
                        }
                        envelope.MessageType = f.Data.MessageType;
                        
                        mapper.MapIncomingToEnvelope(envelope, f);
                        
                        await receiver.ReceivedAsync(this, envelope );
                    }
                    
                    await f.AckAsync();
                }    
            }
        });
    }
    public ValueTask CompleteAsync(Envelope envelope)
    {
        return ValueTask.CompletedTask;
    }

    public ValueTask DeferAsync(Envelope envelope)
    {
        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        if (!_stopSignal.IsCancellationRequested)
        {
            await _stopSignal.CancelAsync();
            await _runner;
        }
        _stopSignal.SafeDispose();
        _runner.SafeDispose();
    }

    public async ValueTask StopAsync()
    {
        await _stopSignal.CancelAsync();
        await _runner;
    }

    public Uri Address { get; }
    public void Dispose()
    {
        _stopSignal.Dispose();
    }
}