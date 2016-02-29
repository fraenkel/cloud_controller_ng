require 'cloud_controller/dea/nats_messages/stager_advertisment'

module VCAP::CloudController
  module Dea
    class StagerPool
      attr_reader :config

      def initialize(config, blobstore_url_generator)
        @advertise_timeout = config[:dea_advertisement_timeout_in_seconds]
        @percentage_of_top_stagers = (config[:placement_top_stager_percentage] || 0) / 100.0
        @stager_advertisements = {}
        @blobstore_url_generator = blobstore_url_generator
      end

      def process_advertise_message(msg)
        advertisement = NatsMessages::StagerAdvertisement.new(msg, Time.now.utc.to_i + @advertise_timeout)

        mutex.synchronize do
          @stager_advertisements[advertisement.stager_id] = advertisement
        end
      end

      def find_stager(stack, memory, disk)
        mutex.synchronize do
          validate_stack_availability(stack)

          prune_stale_advertisements
          best_ad = top_n_stagers_for(memory, disk, stack).sample
          best_ad && best_ad.stager_id
        end
      end

      def reserve_app_memory(stager_id, app_memory)
        @stager_advertisements[stager_id].decrement_memory(app_memory)
      end

      private

      def admin_buildpacks
        AdminBuildpacksPresenter.new(@blobstore_url_generator).to_staging_message_array
      end

      def validate_stack_availability(stack)
        unless @stager_advertisements.any? { |_, ad| ad.has_stack?(stack) }
          raise Errors::ApiError.new_from_details('StackNotFound', "The requested app stack #{stack} is not available on this system.")
        end
      end

      def top_n_stagers_for(memory, disk, stack)
        @stager_advertisements.values.select do |advertisement|
          advertisement.meets_needs?(memory, stack) && advertisement.has_sufficient_disk?(disk)
        end.sort do |advertisement_a, advertisement_b|
          advertisement_a.available_memory <=> advertisement_b.available_memory
        end.last([5, @percentage_of_top_stagers * @stager_advertisements.size].max.to_i)
      end

      def prune_stale_advertisements
        now = Time.now.utc.to_i
        @stager_advertisements.delete_if { |_, ad| ad.expired?(now) }
      end

      def mutex
        @mutex ||= Mutex.new
      end
    end
  end
end
