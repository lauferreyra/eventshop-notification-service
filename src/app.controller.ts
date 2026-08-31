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
  async handleOrderCreated(
    @Payload() data: unknown,
    @Ctx() context: any,
  ) {
    const rmqContext = context as RmqContext;

    const channel = rmqContext.getChannelRef();
    const message = rmqContext.getMessage();

    try {
      console.log('📩 Evento recibido: order.created');
      console.log(data);

      console.log('📧 Simulando envío de notificación...');

      await new Promise((resolve) =>
        setTimeout(resolve, 10000),
      );

      channel.ack(message);

      console.log('✅ Mensaje confirmado con ACK');
    } catch (error) {
      console.error(
        '❌ Error procesando notificación',
        error,
      );

      channel.nack(
        message,
        false,
        false,
      );
    }
  }
}