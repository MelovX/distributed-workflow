using DistributedWorkflow.Api.Models;
using Microsoft.Extensions.Options;
using System.Threading.Channels;

namespace DistributedWorkflow.Api.Batching
{
    public sealed class RegistrationWriteQueue : IRegistrationWriteQueue
    {
        private readonly Channel<QueuedRegistrationWrite> _channel;

        public RegistrationWriteQueue(
            IOptions<RegistrationBatchOptions> options)
        {
            var batchOptions = options.Value;

            var channelOptions =
                new BoundedChannelOptions(batchOptions.Capacity)
                {
                    FullMode = BoundedChannelFullMode.Wait,
                    SingleReader = true,
                    SingleWriter = false,
                    AllowSynchronousContinuations = false
                };

            _channel = Channel.CreateBounded<QueuedRegistrationWrite>(channelOptions);
        }

        internal ChannelReader<QueuedRegistrationWrite> Reader => _channel.Reader;

        public async Task<RegisterDocumentResponse> EnqueueAsync(
            RegistrationWriteRequest request,
            CancellationToken cancellationToken)
        {
            var queuedWrite =
                new QueuedRegistrationWrite(request);

            await _channel.Writer.WriteAsync(
                queuedWrite,
                cancellationToken);

            return await queuedWrite.Completion.WaitAsync(
                cancellationToken);
        }
    }
}
