import { Controller } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';

@Controller()
export class AppController {
  @EventPattern('order.created')
  handleOrderCreated(@Payload() data: unknown) {
    console.log('📩 Evento recibido: order.created');
    console.log(data);
  }
}