require 'date'
require 'disc'
require 'msgpack'

module ActiveJob
  module QueueAdapters
    # == Disc adapter for Active Job
    #
    # Disc is a simple Ruby Job/Worker implementation
    # backed by antirez's(1) Disque(2).
    #
    # Rails and ActiveJob aren't required in order tu run Disc, but this
    # adapter provides a way to use Disc with ActiveJob for those who
    # would rather follow the Rails standard.
    #
    # 1) Disc    - See the documentation:       https://github.com/pote/disc
    # 2) Antirez - Author of Redis and Disque:  http://antirez.com/
    # 3) Disque  - Read more about the backend: https://github.com/antirez/disque
    #
    # To use Disc set the queue_adapter config to +:disc+.
    #
    #     Rails.application.config.active_job.queue_adapter = :disc
    #
    class DiscAdapter
      def self.enqueue(job)
        enqueue_at(job, nil)
      end

      def self.enqueue_at(job, timestamp)
        Disc.disque.push(
          job.queue_name,
          {
            class: job.class.name,
            arguments: job.arguments
          }.to_msgpack,
          Disc.disque_timeout,
          delay: timestamp.nil? ? nil : (timestamp.to_time.to_i - DateTime.now.to_time.to_i)
        )
      end
    end
  end
end
