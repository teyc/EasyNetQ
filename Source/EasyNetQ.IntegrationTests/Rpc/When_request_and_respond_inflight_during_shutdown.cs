using System;
using System.Threading;
using System.Threading.Tasks;
using EasyNetQ.Logging;
using EasyNetQ.Logging.LogProviders;
using Xunit;
using Xunit.Abstractions;

namespace EasyNetQ.IntegrationTests.Rpc
{
    [Collection("RabbitMQ")]
    public class When_request_and_respond_inflight_during_shutdown : IDisposable
    {
        public When_request_and_respond_inflight_during_shutdown(RabbitMQFixture fixture, ITestOutputHelper output)
        {
            Assert.NotNull(output);

            var xUnitLoggingProvider = new XUnitLoggingProvider(output);
            LogProvider.SetCurrentLogProvider(xUnitLoggingProvider);
            logClient = LogProvider.GetLogger("** client");
            logServer = LogProvider.GetLogger("** server");

            clientBus = RabbitHutch.CreateBus($"host={fixture.Host};prefetchCount=1;timeout=-1");
            serverBus = RabbitHutch.CreateBus($"host={fixture.Host};prefetchCount=1;timeout=-1");


            handlerStartedSignal = new ManualResetEvent(false);


        }

        [Theory]
        [InlineData(2, 1.5)]
        [InlineData(2, 2.0)]
        [InlineData(2, 2.5)]
        public async Task Client_should_receive_response(int processingTimeInSeconds, float shutdownTimeInSeconds)
        {
            // Given the server takes `processingTimeInSeconds` seconds to process message
            var subscriptionResult = serverBus.Rpc.RespondAsync(async (Request request) =>
            {
                logServer.Info("Received request id=" + request.Id);
                handlerStartedSignal.Set();
                await Task.Delay(TimeSpan.FromSeconds(processingTimeInSeconds));
                logServer.Info("Responding with Response id=" + request.Id);
                return new Response(request.Id);
            }).GetAwaiter().GetResult();

            // When the client places a request
            logClient.Info("Make request");
            var timeout = TimeSpan.FromSeconds(processingTimeInSeconds + 3);
            var responseTask = clientBus.Rpc.RequestAsync<Request, Response>(
                new Request(42), r => r.WithExpiration(timeout));

            // and when the handler has started processing request
            logClient.Info("Wait for server to receive request");
            handlerStartedSignal.WaitOne();

            logClient.Info("Give server "+ shutdownTimeInSeconds +" seconds before cancelling channel");
            await Task.Delay(TimeSpan.FromSeconds(shutdownTimeInSeconds));

            // the handler runs basicCancel() and is no longer subscribed to new messages
            logClient.Info("Force server to cancel subscription to channel");
            subscriptionResult.Dispose();

            var response = await responseTask;
            Assert.Equal(42, response.Id);
        }

        public void Dispose()
        {
            clientBus.Dispose();
            serverBus.Dispose();
        }

        private readonly IBus clientBus;
        private readonly IBus serverBus;
        private readonly ManualResetEvent handlerStartedSignal;
        private readonly ILog logClient;
        private readonly ILog logServer;
    }

    public class XUnitLoggingProvider : ILogProvider
    {
        private readonly ITestOutputHelper output;

        public XUnitLoggingProvider(ITestOutputHelper output)
        {
            this.output = output;
        }

        public IDisposable OpenNestedContext(string message)
        {
            return NullDisposable.Instance;
        }

        public IDisposable OpenMappedContext(string key, object value, bool destructure = false)
        {
            return NullDisposable.Instance;
        }

        /// <inheritdoc />
        public Logger GetLogger(string name)
        {
            return (logLevel, messageFunc, exception, formatParameters) =>
            {
                if (messageFunc == null)
                {
                    return true;
                }

                WriteMessage(logLevel, name, messageFunc, formatParameters, exception);

                return true;
            };
        }

        private void WriteMessage(
            LogLevel logLevel,
            string name,
            Func<string> messageFunc,
            object[] formatParameters,
            Exception exception)
        {
            var formattedMessage = LogMessageFormatter.FormatStructuredMessage(messageFunc(), formatParameters, out _);

            if (exception != null)
            {
                formattedMessage = formattedMessage + " -> " + exception;
            }

            output.WriteLine("[{0:HH:mm:ss} {1}] {2} {3}", DateTime.UtcNow, logLevel, name, formattedMessage);
        }

        private class NullDisposable : IDisposable
        {
            internal static readonly IDisposable Instance = new NullDisposable();

            public void Dispose()
            {
            }
        }
    }
}
