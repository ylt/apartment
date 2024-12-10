require 'apartment/adapters/abstract_adapter'
require 'digest'

module Apartment
  module Adapters
    class Mysql2Adapter < AbstractAdapter
      def switch_tenant(config)
        difference = current_difference_from(config)

        if difference[:host]
          connection_switch!(config)
        else
          simple_switch(config)
        end
      end

      def create_tenant!(config)
        Apartment.connection.create_database(config[:database], config)
      end

      def simple_switch(config)
        Apartment.connection.execute("use `#{config[:database]}`")
      rescue ActiveRecord::StatementInvalid => e
        if !e.message.match?("We could not find your database: #{config[:database]}")
          # borked connection, remove it and reconnect the connection
          connection_switch!(config, reconnect: true)
        else
          raise_connect_error!(config[:database], e)
        end
      end

      def connection_specification_name(config)
        if Apartment.pool_per_config
          "_apartment_#{config.hash}"
        else
          host_hash = Digest::MD5.hexdigest(config[:host] || config[:url] || "127.0.0.1")
          "_apartment_#{host_hash}_#{config[:adapter]}"
        end
      end

      private
        def database_exists?(database)
          result = Apartment.connection.exec_query(<<-SQL).try(:first)
            SELECT 1 AS `exists`
            FROM INFORMATION_SCHEMA.SCHEMATA
            WHERE SCHEMA_NAME = #{Apartment.connection.quote(database)}
          SQL
          result.present? && result['exists'] == 1
        end

        def valid_tenant?(tenant)
          db = tenant.is_a?(Hash) ? tenant.with_indifferent_access[:database] : tenant

          db && db.bytes.size <= 64 && db.match?(/[^\.\\\/]+/)
        end
    end
  end
end
