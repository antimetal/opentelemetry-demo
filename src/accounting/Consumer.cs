// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Oteldemo;

namespace Accounting;

internal class Consumer : IDisposable
{
    private const string TopicName = "orders";
    private static readonly TimeSpan ConsumeTimeout = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan HealthCheckThreshold = TimeSpan.FromMinutes(5);

    private ILogger _logger;
    private IConsumer<string, byte[]> _consumer;
    private bool _isListening;
    private DateTime _lastMessageTime = DateTime.UtcNow;

    public Consumer(ILogger<Consumer> logger)
    {
        _logger = logger;

        var servers = Environment.GetEnvironmentVariable("KAFKA_ADDR")
            ?? throw new ArgumentNullException("KAFKA_ADDR");

        _consumer = BuildConsumer(servers);
        _consumer.Subscribe(TopicName);

        _logger.LogInformation($"Connecting to Kafka: {servers}");
    }

    public async Task StartListeningAsync()
    {
        _isListening = true;

        try
        {
            while (_isListening)
            {
                try
                {
                    var consumeResult = _consumer.Consume(ConsumeTimeout);
                    
                    if (consumeResult == null)
                    {
                        _logger.LogDebug("No message received within timeout period");
                        continue;
                    }

                    _lastMessageTime = DateTime.UtcNow;
                    ProcessMessage(consumeResult.Message);
                }
                catch (ConsumeException e)
                {
                    _logger.LogError(e, "Consume error: {0}", e.Error.Reason);
                    
                    // Add delay before retrying to prevent tight error loops
                    await Task.Delay(1000);
                }
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Closing consumer");

            _consumer.Close();
        }
    }

    public void StartListening()
    {
        StartListeningAsync().GetAwaiter().GetResult();
    }

    private void ProcessMessage(Message<string, byte[]> message)
    {
        try
        {
            var order = OrderResult.Parser.ParseFrom(message.Value);

            Log.OrderReceivedMessage(_logger, order);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Order parsing failed:");
        }
    }

    public bool IsHealthy()
    {
        var timeSinceLastMessage = DateTime.UtcNow - _lastMessageTime;
        var isHealthy = timeSinceLastMessage <= HealthCheckThreshold;
        
        if (!isHealthy)
        {
            _logger.LogWarning("Consumer health check failed: no messages consumed in {TimeSinceLastMessage}", timeSinceLastMessage);
        }
        
        return isHealthy;
    }

    private IConsumer<string, byte[]> BuildConsumer(string servers)
    {
        var conf = new ConsumerConfig
        {
            GroupId = $"accounting",
            BootstrapServers = servers,
            // https://github.com/confluentinc/confluent-kafka-dotnet/tree/07de95ed647af80a0db39ce6a8891a630423b952#basic-consumer-example
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true
        };

        return new ConsumerBuilder<string, byte[]>(conf)
            .Build();
    }

    public void Dispose()
    {
        _isListening = false;
        _consumer?.Dispose();
    }
}
