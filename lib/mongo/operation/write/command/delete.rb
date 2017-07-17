# Copyright (C) 2014-2017 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

module Mongo
  module Operation
    module Write
      module Command

        # A MongoDB delete write command operation.
        #
        # @example Initialize a delete write command.
        #   Write::Command::Delete.new({
        #     :deletes => [{ :q => { :foo => 1 }, :limit => 1 }],
        #     :db_name => 'test',
        #     :coll_name => 'test_coll',
        #     :write_concern => write_concern,
        #     :ordered => true
        #   })
        #
        # @since 2.0.0
        class Delete
          include Specifiable
          include Writable

          private

          # The query selector for this delete command operation.
          #
          # @return [ Hash ] The selector describing this delete operation.
          #
          # @since 2.0.0
          def selector
            { delete: coll_name,
              deletes: deletes
            }.merge(command_options)
          end

          def command_options
            opts = { ordered: ordered? }
            opts[:writeConcern] = write_concern.options if write_concern
            opts[:collation] = collation if collation
            opts
          end

          # The wire protocol message for this write operation.
          #
          # @return [ Mongo::Protocol::Query ] Wire protocol message.
          #
          # @since 2.0.0
          def message(server)
            if server.features.op_msg_enabled?

              args = { delete: coll_name, "$db" => db_name }.merge!(command_options)
              global_arguments = Protocol::Msg::PayloadZero.new(args)

              payload = Protocol::Msg::PayloadOne.new('deletes', deletes)
              Protocol::Msg.new([:none], options, global_arguments, payload)
            else
              Protocol::Query.new(db_name, Database::COMMAND, selector, options)
            end
          end
        end
      end
    end
  end
end

