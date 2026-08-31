import { Controller } from '@nestjs/common';
import {
  Ctx,
  EventPattern,
  Payload,
  RmqContext,
} from '@nestjs/microservices';

@Controller()
export class AppController {
  @EventPattern('order.created')
  handleOrderCreated(
    @Payload() data: unknown,
    @Ctx() context: any,
  ) {
    const rmqContext = context as RmqContext;

    const channel =
      rmqContext.getChannelRef();

    const message =
      rmqContext.getMessage();

    try {
      console.log(
        '📩 Evento recibido: order.created',
      );

      console.log(data);

      // Temporal para probar retry
      throw new Error(
        'Error simulado enviando notificación',
      );

      channel.ack(message);
    } catch (error) {
      const headers =
        message.properties.headers ?? {};

      const xDeath =
        headers['x-death'] ?? [];

      const retryDeath = xDeath.find(
        (death: any) =>
          death.queue ===
          'notification_retry_queue',
      );

      const retryCount =
        retryDeath?.count ?? 0;

      console.log(
        `❌ Error procesando notificación. Retry actual: ${retryCount}`,
      );

      if (retryCount >= 2) {
        console.log(
          '☠️ Máximo de intentos alcanzado. Enviando a DLQ.',
        );

        channel.publish(
          'notification.dlx',
          'notification.failed',
          message.content,
          {
            persistent: true,

            headers:
              message.properties.headers,

            contentType:
              message.properties.contentType,
          },
        );

        channel.ack(message);

        return;
      }

      console.log(
        '🔄 Enviando mensaje a retry...',
      );

      channel.nack(
        message,
        false,
        false,
      );
    }
  }}