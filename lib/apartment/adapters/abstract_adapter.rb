module Apartment
  module Adapters
    class AbstractAdapter
      ConnectionName = Struct.new('ConnectionName', :name, :primary_class?, :current_preventing_writes)

      CONNECTION_MANAGEMENT_MUTEX = Mutex.new

      include ActiveSupport::Callbacks
      define_callbacks :create, :switch

      # attr_reader :current
      def current
        Apartment.connection_class.connection_db_config.database || ''
      end

      def current_database
        current
      end

      def initialize
        reset
      rescue Apartment::TenantNotFound
        Rails.logger.warn "Unable to connect to default tenant"
      end

      def switch(tenant)
        Rails.logger.info "[Apartment] Attempting to switch to tenant: #{tenant}"
        config = config_for(tenant)
        create_pool_if_none!(config)
        @current = tenant
        Rails.logger.info "[Apartment] Successfully switched to tenant: #{tenant} (database: #{config[:database]})"

        stack = Apartment.connection_class.connected_to_stack.dup

        Apartment.connection_class.connected_to(shard: config[:database]) do
          Rails.logger.debug "[Apartment] Inside tenant context for: #{tenant}"
          result = yield
          Rails.logger.debug "[Apartment] Exiting tenant context for: #{tenant}"
          result
        end
      rescue => e
        Rails.logger.error "[Apartment] Failed to switch to tenant #{tenant}: #{e.message}"
        raise
      ensure
        if stack
          Apartment.connection_class.connected_to_stack.replace(stack)
        end
      end

      def switch!(tenant)
        config = config_for(tenant)

        create_pool_if_none!(config)

        Apartment.connection_class.connecting_to(shard: config[:database])
      end

      def reset
        Apartment.connection_class.connected_to_stack.clear
      end


      def create_pool_if_none!(config)
        name = config[:database]
        handler = Apartment.connection_class.connection_handler
        spec_name = Apartment.connection_class.connection_specification_name

        Rails.logger.debug "[Apartment] Checking connection pool for database: #{name}"

        CONNECTION_MANAGEMENT_MUTEX.synchronize do
          conn = handler.retrieve_connection(spec_name, shard: name) rescue nil

          if conn
            Rails.logger.debug "[Apartment] Found existing connection for database: #{name}"
          else
            Rails.logger.debug "[Apartment] Creating new connection for database: #{name}"
            conn = handler.establish_connection(config, shard: name).lease_connection
          end

          if conn.connected? || conn.database_exists?
            Rails.logger.debug "[Apartment] Connection verified for database: #{name}"
            return
          end

          Rails.logger.warn "[Apartment] Database not found or connection failed for: #{name}"

          cleanup(config)

          raise TenantNotFound, "Error while connecting to tenant #{name}"
        rescue ActiveRecord::NoDatabaseError
          cleanup(config)

          Rails.logger.warn "[Apartment] Database raised: #{name}"

          raise TenantNotFound, "Error while connecting to tenant #{name}"
        end
      end

      def cleanup(config)
        name = config[:database]
        handler = Apartment.connection_class.connection_handler
        spec_name = Apartment.connection_class.connection_specification_name

        pool = handler.retrieve_connection_pool(spec_name, shard: name)
        pool&.release_connection
        handler.remove_connection_pool(spec_name, shard: name)

        Rails.logger.error "[Apartment] Cleaned up connection pool for non-existent database: #{name}"
      end

      def create(tenant)
        run_callbacks :create do
          begin
            config = config_for(tenant)

            create_tenant!(config)
            switch(config) do
              # we also need to switch the base as the schema isn't scoped to ApplicationRecord
              ActiveRecord::Base.connected_to(shard: config[:database]) do
                import_database_schema
                seed_data if Apartment.seed_after_create

                yield if block_given?
              end
            end
          end
        end
      end

      def drop(tenant)
        previous_tenant = @current

        config = config_for(tenant)

        unless database_exists?(config[:database])
          raise TenantNotFound, "Error while dropping database #{config[:database]} for tenant #{tenant}"
        end

        Apartment.connection.drop_database(config[:database])

        @current = tenant
      ensure
        switch!(previous_tenant) rescue reset
      end

      def config_for(tenant)
        return tenant if tenant.is_a?(Hash)

        decorated_tenant = decorate(tenant)
        Apartment.tenant_resolver.resolve(decorated_tenant)
      end

      def decorate(tenant)
        decorator = Apartment.tenant_decorator
        decorator ? decorator.call(tenant) : tenant
      end

      def process_excluded_models
        excluded_config = config_for(Apartment.default_tenant)
        Apartment.connection_handler.establish_connection(excluded_config, owner_name: ConnectionName.new("_apartment_excluded", false))

        Apartment.excluded_models.each do |excluded_model|
          # user mustn't have overridden `connection_specification_name`
          # cattr_accessor in model
          excluded_model.constantize.connection_specification_name = "_apartment_excluded"
        end
      end

      def setup_connection_specification_name
        Apartment.connection_class.connection_specification_name = nil
        Apartment.connection_class.instance_eval do
          def connection_specification_name
            if !defined?(@connection_specification_name) || @connection_specification_name.nil?
              apartment_spec_name = Thread.current[:_apartment_connection_specification_name]
              return apartment_spec_name ||
                     (self == ActiveRecord::Base ? "ActiveRecord::Base" : superclass.connection_specification_name)
            end
            @connection_specification_name
          end
        end
      end

      def current_difference_from(config)
        current_config = config_for(@current)
        config.select{ |k, v| current_config[k] != v }
      end

      def import_database_schema
        ActiveRecord::Schema.verbose = false

        load_or_abort(Apartment.database_schema_file) if Apartment.database_schema_file
      end

      def seed_data
        silence_warnings{ load_or_abort(Apartment.seed_data_file) } if Apartment.seed_data_file
      end

      def load_or_abort(file)
        if File.exist?(file)
          load(file)
        else
          abort %{#{file} doesn't exist yet}
        end
      end

      def raise_connect_error!(tenant, exception)
        raise TenantNotFound, "Error while connecting to tenant #{tenant}: #{exception.message}"
      end
    end
  end
end