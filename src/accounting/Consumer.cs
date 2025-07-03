// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Oteldemo;

namespace Accounting;

internal class Consumer : IDisposable
{
    private const string TopicName = "orders";

    private ILogger _logger;
    private IConsumer<string, byte[]> _consumer;
    private bool _isListening;

    public Consumer(ILogger<Consumer> logger)
    {
        _logger = logger;

        var servers = Environment.GetEnvironmentVariable("KAFKA_ADDR")
            ?? throw new ArgumentNullException("KAFKA_ADDR");

        _consumer = BuildConsumer(servers);
        _consumer.Subscribe(TopicName);

        _logger.LogInformation($"Connecting to Kafka: {servers}");
    }

    public void StartListening()
    {
        _isListening = true;

        try
        {
            while (_isListening)
            {
                try
                {
                    var consumeResults = new List<ConsumeResult<string, byte[]>>();
                    var batchSize = 100;
                    var timeout = TimeSpan.FromMilliseconds(1000);
                    
                    // Consume messages in batches
                    for (int i = 0; i < batchSize; i++)
                    {
                        var consumeResult = _consumer.Consume(timeout);
                        if (consumeResult != null)
                        {
                            consumeResults.Add(consumeResult);
                        }
                        else
                        {
                            break; // No more messages available within timeout
                        }
                    }

                    if (consumeResults.Count > 0)
                    {
                        var processedCount = 0;
                        
                        foreach (var result in consumeResults)
                        {
                            ProcessMessage(result.Message);
                            processedCount++;
                        }
                        
                        _consumer.Commit();
                        _logger.LogInformation($"Processed and committed {processedCount} messages");
                    }
                }
                catch (ConsumeException e)
                {
                    _logger.LogError(e, "Consume error: {0}", e.Error.Reason);
                }
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Closing consumer");

            _consumer.Close();
        }
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

    private IConsumer<string, byte[]> BuildConsumer(string servers)
    {
        var conf = new ConsumerConfig
        {
            GroupId = $"accounting",
            BootstrapServers = servers,
            // https://github.com/confluentinc/confluent-kafka-dotnet/tree/07de95ed647af80a0db39ce6a8891a630423b952#basic-consumer-example
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,  // Manual commit for batching
            MaxPollIntervalMs = 300000,  // 5 minutes
            SessionTimeoutMs = 45000,    // 45 seconds
            FetchMinBytes = 1024,        // Wait for at least 1KB
            MaxPartitionFetchBytes = 1048576  // 1MB per partition
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
