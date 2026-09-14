using DistributedWorkflow.Worker.Configuration;
using DistributedWorkflow.Worker.Data;
using Microsoft.Extensions.Options;
using System.Threading.Channels;

namespace DistributedWorkflow.Worker.Services;

public sealed class WorkerInboxBatcher : BackgroundService
{
    private sealed record ClaimRequest(
        InboxClaimEntry Entry,
        TaskCompletionSource<InboxClaimResult> Completion);

    private sealed record CompletionRequest(
        InboxCompletionEntry Entry,
        TaskCompletionSource<InboxCompletionResult> Completion);

    private readonly WorkerInboxStore _inboxStore;
    private readonly WorkerBatchOptions _options;
    private readonly ILogger<WorkerInboxBatcher> _logger;
    private readonly Channel<ClaimRequest> _claimRequests;
    private readonly Channel<CompletionRequest> _completionRequests;

    public WorkerInboxBatcher(
        WorkerInboxStore inboxStore,
        IOptions<WorkerBatchOptions> options,
        ILogger<WorkerInboxBatcher> logger)
    {
        _inboxStore = inboxStore;
        _options = options.Value;
        _logger = logger;

        _claimRequests =
            CreateRequestChannel<ClaimRequest>(
                _options.Capacity);

        _completionRequests =
            CreateRequestChannel<CompletionRequest>(
                _options.Capacity);
    }

    public async Task<InboxClaimResult> ClaimAsync(
        InboxClaimEntry entry,
        CancellationToken cancellationToken)
    {
        var completion =
            new TaskCompletionSource<InboxClaimResult>(
                TaskCreationOptions.RunContinuationsAsynchronously);

        var request =
            new ClaimRequest(entry, completion);

        await _claimRequests.Writer.WriteAsync(
            request,
            cancellationToken);

        return await completion.Task.WaitAsync(cancellationToken);
    }

    public async Task<InboxCompletionResult> CompleteAndEnqueueAsync(
        InboxCompletionEntry entry,
        CancellationToken cancellationToken)
    {
        var completion =
            new TaskCompletionSource<InboxCompletionResult>(
                TaskCreationOptions.RunContinuationsAsynchronously);

        var request =
            new CompletionRequest(entry, completion);

        await _completionRequests.Writer.WriteAsync(
            request,
            cancellationToken);

        return await completion.Task.WaitAsync(cancellationToken);
    }

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        try
        {
            await Task.WhenAll(
                ProcessClaimRequestsAsync(stoppingToken),
                ProcessCompletionRequestsAsync(stoppingToken));
        }
        catch (OperationCanceledException)
            when (stoppingToken.IsCancellationRequested)
        {
            // Normal application shutdown.
        }
        finally
        {
            _claimRequests.Writer.TryComplete();
            _completionRequests.Writer.TryComplete();

            while (
                _claimRequests.Reader.TryRead(
                    out var claimRequest))
            {
                claimRequest.Completion.TrySetCanceled(
                    stoppingToken);
            }

            while (
                _completionRequests.Reader.TryRead(
                    out var completionRequest))
            {
                completionRequest.Completion.TrySetCanceled(
                    stoppingToken);
            }
        }
    }

    private async Task ProcessClaimRequestsAsync(
        CancellationToken stoppingToken)
    {
        var reader = _claimRequests.Reader;

        while (await reader.WaitToReadAsync(stoppingToken))
        {
            var batch =
                await ReadClaimBatchAsync(
                    reader,
                    stoppingToken);

            await ExecuteClaimBatchAsync(
                batch,
                stoppingToken);
        }
    }

    private async Task ProcessCompletionRequestsAsync(
        CancellationToken stoppingToken)
    {
        var reader = _completionRequests.Reader;

        while (await reader.WaitToReadAsync(stoppingToken))
        {
            var batch =
                await ReadCompletionBatchAsync(
                    reader,
                    stoppingToken);

            await ExecuteCompletionBatchAsync(
                batch,
                stoppingToken);
        }
    }

    private async Task<List<ClaimRequest>> ReadClaimBatchAsync(
        ChannelReader<ClaimRequest> reader,
        CancellationToken stoppingToken)
    {
        var batch =
            new List<ClaimRequest>(_options.MaxBatchSize)
            {
                await reader.ReadAsync(stoppingToken)
            };

        using var batchDelayCancellation =
            CancellationTokenSource.CreateLinkedTokenSource(
                stoppingToken);

        batchDelayCancellation.CancelAfter(
            _options.MaxBatchDelay);

        try
        {
            while (batch.Count < _options.MaxBatchSize)
            {
                while (
                    batch.Count < _options.MaxBatchSize
                    && reader.TryRead(out var request))
                {
                    batch.Add(request);
                }

                if (batch.Count >= _options.MaxBatchSize)
                {
                    break;
                }

                try
                {
                    var hasMoreRequests =
                        await reader.WaitToReadAsync(
                            batchDelayCancellation.Token);

                    if (!hasMoreRequests)
                    {
                        break;
                    }
                }
                catch (OperationCanceledException)
                    when (!stoppingToken.IsCancellationRequested)
                {
                    // MaxBatchDelay elapsed.
                    break;
                }
            }
        }
        catch (OperationCanceledException)
            when (stoppingToken.IsCancellationRequested)
        {
            foreach (var request in batch)
            {
                request.Completion.TrySetCanceled(stoppingToken);
            }

            throw;
        }

        return batch;
    }

    private async Task<List<CompletionRequest>> ReadCompletionBatchAsync(
        ChannelReader<CompletionRequest> reader,
        CancellationToken stoppingToken)
    {
        var batch =
            new List<CompletionRequest>(_options.MaxBatchSize)
            {
            await reader.ReadAsync(stoppingToken)
            };

        using var batchDelayCancellation =
            CancellationTokenSource.CreateLinkedTokenSource(
                stoppingToken);

        batchDelayCancellation.CancelAfter(
            _options.MaxBatchDelay);

        try
        {
            while (batch.Count < _options.MaxBatchSize)
            {
                while (
                    batch.Count < _options.MaxBatchSize
                    && reader.TryRead(out var request))
                {
                    batch.Add(request);
                }

                if (batch.Count >= _options.MaxBatchSize)
                {
                    break;
                }

                try
                {
                    var hasMoreRequests =
                        await reader.WaitToReadAsync(
                            batchDelayCancellation.Token);

                    if (!hasMoreRequests)
                    {
                        break;
                    }
                }
                catch (OperationCanceledException)
                    when (!stoppingToken.IsCancellationRequested)
                {
                    // MaxBatchDelay elapsed.
                    break;
                }
            }
        }
        catch (OperationCanceledException)
            when (stoppingToken.IsCancellationRequested)
        {
            foreach (var request in batch)
            {
                request.Completion.TrySetCanceled(
                    stoppingToken);
            }

            throw;
        }

        return batch;
    }

    private async Task ExecuteClaimBatchAsync(
        List<ClaimRequest> batch,
        CancellationToken stoppingToken)
    {
        try
        {
            var entries = batch
                .Select(request => request.Entry)
                .ToArray();

            var results =
                await _inboxStore.ClaimBatchAsync(
                    entries,
                    stoppingToken);

            for (var index = 0; index < batch.Count; index++)
            {
                var request = batch[index];
                var result = results[index];

                if (result.MessageId != request.Entry.MessageId
                    || result.LockOwner != request.Entry.LockOwner)
                {
                    throw new InvalidOperationException(
                        "Inbox claim batch result does not match its request.");
                }
            }

            for (var index = 0; index < batch.Count; index++)
            {
                batch[index].Completion.TrySetResult(
                    results[index]);
            }
        }
        catch (OperationCanceledException)
            when (stoppingToken.IsCancellationRequested)
        {
            foreach (var request in batch)
            {
                request.Completion.TrySetCanceled(stoppingToken);
            }

            throw;
        }
        catch (Exception exception)
        {
            _logger.LogError(
                exception,
                "Inbox claim batch failed. BatchSize={BatchSize}.",
                batch.Count);

            foreach (var request in batch)
            {
                request.Completion.TrySetException(exception);
            }
        }
    }

    private async Task ExecuteCompletionBatchAsync(
        List<CompletionRequest> batch,
        CancellationToken stoppingToken)
    {
        try
        {
            var entries = batch
                .Select(request => request.Entry)
                .ToArray();

            var results =
                await _inboxStore.CompleteAndEnqueueBatchAsync(
                    entries,
                    stoppingToken);

            for (var index = 0; index < batch.Count; index++)
            {
                var request = batch[index];
                var result = results[index];

                if (result.MessageId != request.Entry.MessageId
                    || result.LockOwner != request.Entry.LockOwner)
                {
                    throw new InvalidOperationException(
                        "Inbox completion batch result does not match its request.");
                }
            }

            for (var index = 0; index < batch.Count; index++)
            {
                batch[index].Completion.TrySetResult(
                    results[index]);
            }
        }
        catch (OperationCanceledException)
            when (stoppingToken.IsCancellationRequested)
        {
            foreach (var request in batch)
            {
                request.Completion.TrySetCanceled(
                    stoppingToken);
            }

            throw;
        }
        catch (Exception exception)
        {
            _logger.LogError(
                exception,
                "Inbox completion batch failed. BatchSize={BatchSize}.",
                batch.Count);

            foreach (var request in batch)
            {
                request.Completion.TrySetException(exception);
            }
        }
    }

    private static Channel<TRequest> CreateRequestChannel<TRequest>(int capacity)
    {
        return Channel.CreateBounded<TRequest>(
            new BoundedChannelOptions(capacity)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = true,
                SingleWriter = false,
                AllowSynchronousContinuations = false
            });
    }
}