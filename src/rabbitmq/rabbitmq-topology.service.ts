import {
  Injectable,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';

import * as amqp from 'amqplib';
import { Channel, ChannelModel } from 'amqplib';

@Injectable()
export class RabbitmqTopologyService
  implements OnModuleInit, OnModuleDestroy
{
  private connection: ChannelModel;
  private channel: Channel;

  async onModuleInit() {
    this.connection = await amqp.connect(
      'amqp://admin:admin@localhost:5672',
    );

    this.channel = await this.connection.createChannel();

    await this.createTopology();

    console.log(
      '✅ Topología RabbitMQ de Notification configurada',
    );
  }

  private async createTopology() {
  const eventsExchange = 'eventshop.events';

  const notificationQueue = 'notification_queue';

  const retryExchange = 'notification.retry.exchange';

  const retryReturnExchange =
    'notification.retry.return.exchange';

  const retryQueue =
    'notification_retry_queue';

  const retryRoutingKey =
    'notification.retry';

  const retryReturnRoutingKey =
    'notification.retry.return';

  const deadLetterExchange = 'notification.dlx';
  const deadLetterQueue = 'notification_dlq';

  // Exchange principal de eventos
  await this.channel.assertExchange(
    eventsExchange,
    'topic',
    {
      durable: true,
    },
  );

  // Exchange de retry
  await this.channel.assertExchange(
    retryExchange,
    'direct',
    {
      durable: true,
    },
  );

  // Exchange de mensajes muertos
  await this.channel.assertExchange(
    deadLetterExchange,
    'direct',
    {
      durable: true,
    },
  );

  await this.channel.assertExchange(
  retryExchange,
  'direct',
  {
    durable: true,
  },
);

await this.channel.assertExchange(
  retryReturnExchange,
  'direct',
  {
    durable: true,
  },
);

  // Queue principal
  await this.channel.assertQueue(
    notificationQueue,
    {
      durable: true,
      arguments: {
        'x-dead-letter-exchange': retryExchange,
        'x-dead-letter-routing-key': retryRoutingKey,
      },
    },
  );

  // Queue de retry
  await this.channel.assertQueue(
    retryQueue,
    {
      durable: true,
      arguments: {
        'x-message-ttl': 10000,

        'x-dead-letter-exchange':
          retryReturnExchange,

        'x-dead-letter-routing-key':
          retryReturnRoutingKey,
      },
    },
  );

  // DLQ
  await this.channel.assertQueue(
    deadLetterQueue,
    {
      durable: true,
    },
  );

  await this.channel.assertQueue(
  notificationQueue,
  {
    durable: true,
    arguments: {
      'x-dead-letter-exchange':
        retryExchange,

      'x-dead-letter-routing-key':
        retryRoutingKey,
    },
  },
);

  // order.created → notification_queue
  await this.channel.bindQueue(
    notificationQueue,
    eventsExchange,
    'order.created',
  );

  // mensajes fallidos → retry queue
  await this.channel.bindQueue(
    retryQueue,
    retryExchange,
    retryRoutingKey,
  );

  // mensajes definitivamente fallidos → DLQ
  await this.channel.bindQueue(
    deadLetterQueue,
    deadLetterExchange,
    'notification.failed',
  );
}

  async onModuleDestroy() {
    await this.channel?.close();
    await this.connection?.close();
  }
}