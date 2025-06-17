require 'apartment/adapters/abstract_adapter'
require 'digest'

module Apartment
  module Adapters
    class Mysql2Adapter < AbstractAdapter
      def create_tenant!(config)
        Apartment.connection.create_database(config[:database], config)
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
