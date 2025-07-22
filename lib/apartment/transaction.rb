module Apartment
  class Transaction
    def initialize
      @connections = []
      @transactions = []
      @committed = false
      @rolled_back = false
    end

    def add_connection(connection)
      raise "Cannot add connection to completed transaction" if completed?
      
      # Don't add the same connection twice
      return if @connections.include?(connection)
      
      @connections << connection
      
      # Rails handles savepoints automatically for nested transactions
      @transactions << connection.begin_transaction
    end

    def commit
      raise "Transaction already completed" if completed?
      
      begin
        @transactions.each(&:commit)
        @committed = true
      rescue => e
        # If any commit fails, rollback all remaining transactions
        rollback unless @rolled_back
        raise e
      end
    end

    def rollback
      return if @rolled_back
      
      @transactions.reverse_each do |transaction|
        begin
          transaction.rollback
        rescue => e
          # Log but continue rolling back other transactions
          Rails.logger.error "Failed to rollback transaction: #{e.message}"
        end
      end
      
      @rolled_back = true
    end

    def within_transaction
      begin
        yield self
        commit unless completed?
      rescue => e
        rollback unless completed?
        raise e
      end
    end

    def completed?
      @committed || @rolled_back
    end

    def committed?
      @committed
    end

    def rolled_back?
      @rolled_back
    end
    #
    # # Add all connections for specified tenants
    # def add_tenants(*tenant_names)
    #   tenant_names.flatten.each do |tenant|
    #     connection = Apartment.connection_class.connection_pool.checkout
    #     Apartment::Tenant.switch!(tenant) do
    #       add_connection(connection)
    #     end
    #   end
    # end

    # Execute block across all added connections
    def execute
      results = {}
      
      @connections.each_with_index do |connection, index|
        transaction = @transactions[index]
        result = yield(connection, transaction)
        results[connection] = result
      end
      
      results
    end
  end
end