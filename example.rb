# frozen_string_literal: true

require './lib/rabbit_mq_producer'

payload = {
  push_data: {
    message: 'test gem'
  },
  device_token: '21312313',
  source: 'solar',
  platform: 'ios'
}

RabbitMQProducer.new('push').publish(payload)
