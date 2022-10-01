import { Inject, Injectable } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';
import { CreateOrderRequest } from './create-order-request.dto';
import { OrderCreatedEvent } from './order-created.event';
import {
  SchemaRegistry,
  readAVSCAsync,
} from "@kafkajs/confluent-schema-registry";
 
// If we use AVRO, we need to configure a Schema Registry
// which keeps track of the schema
const registry = new SchemaRegistry({
  host: "http://localhost:8081",
});
 
// declaring a TypeScript type for our message structure
declare type MyMessage = {
  orderId: string;
  userId: string;
  price: number
};
 
// This will create an AVRO schema from an .avsc file
const registerSchema = async () => {
  try {
    const schema = await readAVSCAsync("./avro/schema.avsc");    
    const { id } = await registry.register(schema);
    return id;
  } catch (e) {
    console.log(e);
  }
};



@Injectable()
export class AppService {
   order_created_topic: string = 'order_created';
   
  constructor(
    @Inject('BILLING_SERVICE') private readonly billingClient: ClientKafka,
  ) {}
 
  getHello(): string {
    return 'Hello World!';
  }
 
  
  async createOrder({ userId, price }: CreateOrderRequest) {   
    try {
      const registryId = await registerSchema();
            
      if (registryId) {
        const message: MyMessage = new OrderCreatedEvent('123', userId, price);        
        console.log(message);
        const outgoingMessage = await registry.encode(registryId, message);            
        this.billingClient.emit(
          this.order_created_topic,
          outgoingMessage,
        );
        console.log(`Produced message to Kafka: ${JSON.stringify(message)}`);
      }
    } catch (error) {
        console.log(error);
    }    
  }
} 