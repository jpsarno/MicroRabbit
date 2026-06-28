using MediatR;
using MicroRabbit.Domain.Core.Bus;
using MicroRabbit.Domain.Core.Commands;
using MicroRabbit.Domain.Core.Events;
using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MicroRabbit.Infra.Bus
{
    public sealed class RabbitMQBus : IEventBus
    {
        private readonly IMediator _mediator;
        private readonly Dictionary<string, List<Type>> _handlers;
        private readonly List<Type> _eventTypes;

        public RabbitMQBus(IMediator mediator)
        {
            _mediator = mediator;
            _handlers = new Dictionary<string, List<Type>>();
            _eventTypes = new List<Type>();
        }

        public Task SendCommand<T>(T command) where T : Command
        {
            return _mediator.Send(command);
        }

        public async Task PublishAsync<T>(T @event) where T : Event
        {
            var factory = new ConnectionFactory() { HostName = "localhost" }; // 1 - criar a factory de conexão
            using var connection = await factory.CreateConnectionAsync(); // 2 - abrir a conexão
            using var channel = await connection.CreateChannelAsync(); // 3 - abrir o canal

            var eventName = @event.GetType().Name; // 4 - obter o nome do evento

            await channel.QueueDeclareAsync(queue: eventName, durable: true, exclusive: false, autoDelete: false,
            arguments: new Dictionary<string, object?> { { "x-queue-type", "quorum" } }); // 5 - declarar a fila

            var message = JsonConvert.SerializeObject(@event); // 6 - criar a mensagem
            var body = System.Text.Encoding.UTF8.GetBytes(message);

            await channel.BasicPublishAsync(exchange: string.Empty, routingKey: eventName, body: body); // 7 - publicar a mensagem
        }        

        public void Subscribe<T, TH>()
            where T : Event
            where TH : IEventHandler<T>
        {
            var eventName = typeof(T).Name;
            var handlerType = typeof(TH);

            if (!_eventTypes.Contains(typeof(T)))
            {
                _eventTypes.Add(typeof(T));
            }

            if (!_handlers.ContainsKey(eventName))
            {
                _handlers.Add(eventName, new List<Type>());
            }

            if (_handlers[eventName].Any(s => s.GetType() == handlerType))
            {
                throw new ArgumentException($"Handler Type {handlerType.Name} already registered for '{eventName}'", nameof(handlerType));
            }

            _handlers[eventName].Add(handlerType);

            _ = StartBasicConsumeAsync<T>();
        }

        private async Task StartBasicConsumeAsync<T>() where T : Event
        {
            var factory = new ConnectionFactory { HostName = "localhost", ConsumerDispatchConcurrency = 1};
            using var connection = await factory.CreateConnectionAsync();
            using var channel = await connection.CreateChannelAsync();

            var eventName = typeof(T).Name;

            await channel.QueueDeclareAsync(queue: eventName, durable: true, exclusive: false, autoDelete: false,
            arguments: new Dictionary<string, object?> { { "x-queue-type", "quorum" } });

            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.ReceivedAsync += Consumer_ReceivedAsync;

            await channel.BasicConsumeAsync(eventName, autoAck: true, consumer: consumer);
        }

        private async Task Consumer_ReceivedAsync(object sender, BasicDeliverEventArgs e)
        {
            var eventName = e.RoutingKey;            
            var message = Encoding.UTF8.GetString(e.Body.ToArray());

            try
            {
                await ProcessEvent(eventName, message).ConfigureAwait(false);
            }
            catch (Exception)
            {

                throw;
            }            
        }

        private async Task ProcessEvent(string eventName, string message)
        {
            if(_handlers.ContainsKey(eventName))
            {
                var subscriptions = _handlers[eventName];
                foreach (var subscription in subscriptions)
                {
                    var handler = Activator.CreateInstance(subscription);
                    if (handler == null) continue;
                    var eventType = _eventTypes.SingleOrDefault(t => t.Name == eventName);
                    if (eventType == null) continue;
                    var @event = JsonConvert.DeserializeObject(message, eventType);
                    if (@event == null) continue;
                    var concreteType = typeof(IEventHandler<>).MakeGenericType(eventType);
                    await (Task)concreteType.GetMethod("Handle")!.Invoke(handler, new object[] { @event })!;
                }
            }
        }
    }
}
