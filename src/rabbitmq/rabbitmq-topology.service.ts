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

    const deadLetterExchange = 'notification.dlx';

    const deadLetterQueue = 'notification_dlq';

    await this.channel.assertExchange(
      eventsExchange,
      'topic',
      {
        durable: true,
      },
    );

    await this.channel.assertExchange(
      deadLetterExchange,
      'direct',
      {
        durable: true,
      },
    );

    await this.channel.assertQueue(
      notificationQueue,
      {
        durable: true,
        arguments: {
          'x-dead-letter-exchange': deadLetterExchange,
          'x-dead-letter-routing-key':
            'notification.failed',
        },
      },
    );

    await this.channel.assertQueue(
      deadLetterQueue,
      {
        durable: true,
      },
    );

    await this.channel.bindQueue(
      notificationQueue,
      eventsExchange,
      'order.created',
    );

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