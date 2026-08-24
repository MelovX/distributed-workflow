using Interview.Playground.Api.HealthChecks;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Moq;
using StackExchange.Redis;

namespace Interview.Playground.Api.UnitTests.HealthChecks
{
    public sealed class RedisHealthCheckTests
    {
        [Fact]
        public async Task CheckHealthAsync_WhenRedisResponds_ReturnsHealthy()
        {
            // Arrange
            var databaseMock = new Mock<IDatabase>();

            databaseMock
                .Setup(database =>
                    database.PingAsync(It.IsAny<CommandFlags>()))
                .ReturnsAsync(TimeSpan.FromMilliseconds(1));

            var redisMock = new Mock<IConnectionMultiplexer>();

            redisMock
                .Setup(redis =>
                    redis.GetDatabase(
                        It.IsAny<int>(),
                        It.IsAny<object?>()))
                .Returns(databaseMock.Object);
            
            var healthCheck = new RedisHealthCheck(redisMock.Object);
            var context = new HealthCheckContext();

            // Act
            var result = await healthCheck.CheckHealthAsync(context, TestContext.Current.CancellationToken);

            // Assert
            Assert.Equal(HealthStatus.Healthy, result.Status);
            databaseMock.Verify(
                database => database.PingAsync(It.IsAny<CommandFlags>()),
                Times.Once);
        }

        [Fact]
        public async Task CheckHealthAsync_WhenRedisPingThrows_ReturnsUnhealthy()
        {
            // Arrange
            var exception = new InvalidOperationException("Redis is unavailable");

            var databaseMock = new Mock<IDatabase>();

            databaseMock
                .Setup(database =>
                    database.PingAsync(It.IsAny<CommandFlags>()))
                .ThrowsAsync(exception);

            var redisMock = new Mock<IConnectionMultiplexer>();

            redisMock
                .Setup(redis =>
                    redis.GetDatabase(
                        It.IsAny<int>(),
                        It.IsAny<object?>()))
                .Returns(databaseMock.Object);

            var healthCheck = new RedisHealthCheck(redisMock.Object);
            var context = new HealthCheckContext();

            // Act
            var result = await healthCheck.CheckHealthAsync(
                context,
                TestContext.Current.CancellationToken);

            // Assert
            Assert.Equal(HealthStatus.Unhealthy, result.Status);
            Assert.Equal("Redis is unavailable", result.Description);
            Assert.Same(exception, result.Exception);
        }

        [Fact]
        public async Task CheckHealthAsync_WhenCancellationRequested_ThrowsOperationCanceledException()
        {
            // Arrange
            var pingCompletionSource = new TaskCompletionSource<TimeSpan>();

            using var cancellationTokenSource = new CancellationTokenSource();
            cancellationTokenSource.Cancel();

            var databaseMock = new Mock<IDatabase>();

            databaseMock
                .Setup(database =>
                    database.PingAsync(It.IsAny<CommandFlags>()))
                .Returns(pingCompletionSource.Task);

            var redisMock = new Mock<IConnectionMultiplexer>();

            redisMock
                .Setup(redis =>
                    redis.GetDatabase(
                        It.IsAny<int>(),
                        It.IsAny<object?>()))
                .Returns(databaseMock.Object);

            var healthCheck = new RedisHealthCheck(redisMock.Object);
            var context = new HealthCheckContext();

            // Act & Assert
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => healthCheck.CheckHealthAsync(
                context,
                cancellationTokenSource.Token));
        }
    }
}
