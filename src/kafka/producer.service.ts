import { Injectable, OnApplicationShutdown, OnModuleInit } from "@nestjs/common";
import { Kafka, Producer, ProducerRecord } from "kafkajs";


@Injectable()
export class ProducerService implements  OnModuleInit, OnApplicationShutdown{
    private readonly kafka = new Kafka({
        brokers: [`${process.env.KAFKA_BROKER_HOST}:${process.env.KAFKA_BROKER_PORT}`],
    });
    private readonly producer: Producer = this.kafka.producer();

    async onModuleInit() {
        await this.producer.connect();
    }

    async produce(record : ProducerRecord) {
        await this.producer.send(record);
    }

    async onApplicationShutdown() {
        await this.producer.disconnect();
    }
}