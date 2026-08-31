using Microsoft.Extensions.Options;
using System.Threading.Channels;

namespace DistributedWorkflow.Api.Batching
{
    internal sealed class RegistrationBatchReader
    {
        private readonly ChannelReader<QueuedRegistrationWrite> _reader;
        private readonly int _maxBatchSize;
        private readonly TimeSpan _maxBatchDelay;

        public RegistrationBatchReader(RegistrationWriteQueue queue,
            IOptions<RegistrationBatchOptions> options)
        {
            _reader = queue.Reader;
            _maxBatchSize = options.Value.MaxBatchSize;
            _maxBatchDelay = options.Value.MaxBatchDelay;
        }

        public async Task<IReadOnlyList<QueuedRegistrationWrite>> ReadAsync(
            CancellationToken cancellationToken)
        {
            var batch =
                new List<QueuedRegistrationWrite>(_maxBatchSize);

            var firstItem =
                await _reader.ReadAsync(cancellationToken);

            batch.Add(firstItem);

            using var batchWindowCancellation =
                CancellationTokenSource.CreateLinkedTokenSource(
                    cancellationToken);

            batchWindowCancellation.CancelAfter(_maxBatchDelay);

            while (batch.Count < _maxBatchSize)
            {
                if (_reader.TryRead(out var item))
                {
                    batch.Add(item);
                    continue;
                }

                try
                {
                    var canRead = await _reader.WaitToReadAsync(
                        batchWindowCancellation.Token);

                    if (!canRead)
                    {
                        break;
                    }
                }
                catch (OperationCanceledException)
                    when (!cancellationToken.IsCancellationRequested)
                {
                    break;
                }
            }

            return batch;
        }
    }
}
