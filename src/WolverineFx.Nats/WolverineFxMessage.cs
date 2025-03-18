namespace WolverineFx.Nats;

public record WolverineFxMessage(
    string? MessageType = null,
    string? DataPayload = null,
    object? MessagePayload = null,
    string? PartitionKey = null,
    string? TopicName = null,
    bool IsPing = false
    );