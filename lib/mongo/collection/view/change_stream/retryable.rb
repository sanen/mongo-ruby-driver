module Mongo
  class Collection
    class View
      class ChangeStream < Aggregation

        module Retryable

          # Execute a read operation with a single retry.
          #
          # @api private
          #
          # @example Execute the read.
          #   read_with_one_retry do
          #     ...
          #   end
          #
          # @note This only retries read operations on socket errors.
          #
          # @param [ Proc ] block The block to execute.
          #
          # @return [ Result ] The result of the operation.
          #
          # @since 2.5.0
          def read_with_one_retry
            yield
          rescue Error::SocketError, Error::SocketTimeoutError
            close_cursor
            yield
          rescue Mongo::OperationFailure => e
            raise unless e.message == 'not master'
            close_cursor
            yield
          end
        end
      end
    end
  end
end
