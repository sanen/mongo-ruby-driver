module Mongo
  class Collection
    class View

      class ChangeStream < Aggregation

        class Cursor < Mongo::Cursor
          include Retryable

          # Iterate through documents returned from the query.
          #
          # @example Iterate over the documents in the cursor.
          #   cursor.each do |doc|
          #     ...
          #   end
          #
          # @return [ Enumerator ] The enumerator.
          #
          # @since 2.5.0
          def each
            process(@initial_result).each { |doc| yield doc }
            while more?
              return kill_cursors if exhausted?

              read_with_one_retry do
                get_more.each { |doc| yield doc }
              end
            end
          end

          alias :close_cursor :kill_cursors
        end
      end
    end
  end
end
