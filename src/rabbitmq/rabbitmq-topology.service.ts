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
    const retryQueue = 'notification_retry_queue';
    const retryRoutingKey = 'notification.retry';

    const deadLetterExchange = 'notification.dlx';
    const deadLetterQueue = 'notification_dlq';
    const deadLetterRoutingKey = 'notification.failed';

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

    // Exchange de mensajes fallidos definitivos
    await this.channel.assertExchange(
      deadLetterExchange,
      'direct',
      {
        durable: true,
      },
    );

    // Queue principal de Notification
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
          // Espera 10 segundos antes de volver a intentar
          'x-message-ttl': 10000,

          // Cuando vence el TTL, vuelve directo a notification_queue
          'x-dead-letter-exchange': '',

          'x-dead-letter-routing-key': notificationQueue,
        },
      },
    );

    // Dead Letter Queue
    await this.channel.assertQueue(
      deadLetterQueue,
      {
        durable: true,
      },
    );

      await this.channel.bindQueue(
      notificationQueue,
      eventsExchange,
      'payment.completed',
    );

    await this.channel.bindQueue(
      notificationQueue,
      eventsExchange,
      'payment.failed',
    );

    // errores temporales -> notification_retry_queue
    await this.channel.bindQueue(
      retryQueue,
      retryExchange,
      retryRoutingKey,
    );

    // errores definitivos -> notification_dlq
    await this.channel.bindQueue(
      deadLetterQueue,
      deadLetterExchange,
      deadLetterRoutingKey,
    );
  }

  async onModuleDestroy() {
    await this.channel?.close();
    await this.connection?.close();
  }
}