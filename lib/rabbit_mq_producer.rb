# frozen_string_literal: true

require 'json'
require_relative 'rabbit_mq_producer/rabbit_mq_connection'

class RabbitMQProducer
  attr_reader :connection, :channel, :exchange, :sync

  # durable - сохраняет очереди при перезагрузки rabbitmq
  def initialize(exchange, sync: true)
    @connection = RabbitMQConnection.connection
    @channel = connection.start.create_channel
    @exchange = channel.topic(exchange, durable: true)
    @sync = sync
  end

  def publish(payload, routing_key:)
    exchange.publish(
      payload.to_json,
      routing_key: routing_key,
      persistent: true
    )
    close if sync
  end

  def close
    channel.close if channel.open?
    connection.close if connection.open?
  end
end
