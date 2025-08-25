# frozen_string_literal: true

require 'json'
require_relative 'rabbit_mq_producer/rabbit_mq_connection'

class RabbitMQProducer
  attr_reader :connection, :channel, :exchange, :shard_count, :sync

  # durable - сохраняет очереди при перезагрузки rabbitmq
  def initialize(exchange, sync: true, shards: 1)
    @connection = RabbitMQConnection.connection
    @channel = connection.start.create_channel
    @exchange = channel.topic(exchange, durable: true)
    @shard_count = shards
    @sync = sync
  end

  def publish(payload, routing_key:)
    shard_id = select_shard(routing_key)
    exchange.publish(
      payload.to_json,
      routing_key: "#{routing_key}.shard:#{shard_id}",
      persistent: true
    )
    close if sync
  end

  def close
    channel.close if channel.open?
    connection.close if connection.open?
  end

  private

  def select_shard(routing_key)
    shard_key = routing_key.include?('.') ? routing_key.split('.').last : nil
    return Zlib.crc32(shard_key.to_s) % shard_count if shard_key

    rand(shard_count)
  end
end
