# Copyright (C) 2014-2016 MongoDB, Inc.
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

require 'mongo/collection/view/change_stream/retryable'
require 'mongo/collection/view/change_stream/cursor'

module Mongo
  class Collection
    class View

      # Provides behaviour around a `$changeNotification` pipeline stage in the
      # aggregation framework. Specifying this stage allows users to request that
      # notifications are sent for all changes to a particular collection or database.
      #
      # @since 2.5.0
      class ChangeStream < Aggregation
        include Retryable

        # @return [ BSON::Document, Hash ] resume_token The resume token.
        attr_reader :resume_token

        # @return [ Symbol ] The full document option default.
        FULL_DOCUMENT_DEFAULT = :none

        # Initialize the change stream for the provided collection view, pipeline
        # and options.
        #
        # @example Create the new change stream view.
        #   ChangeStream.new(view, pipeline, options)
        #
        # @param [ Collection::View ] view The collection view.
        # @param [ Array<Hash> ] pipeline The pipeline of operators to filter the change notifications.
        # @param [ Hash ] options The change stream options.
        #
        # @since 2.5.0
        def initialize(view, pipeline, options = {})
          @view = view
          @pipeline = pipeline
          @options = BSON::Document.new(options).freeze
          @resume_token = options[:resume_after]
        end

        def each
          @cursor = nil
          read_with_one_retry do
            server = read.select_server(cluster, false)
            result = send_initial_query(server)
            @cursor = ChangeStream::Cursor.new(view, result, server)
          end
          @cursor.each do |doc|
            yield doc
          end if block_given?
          @cursor.to_enum
        end

        private

        def close_cursor
          @cursor.send(:kill_cursors)
        end

        def full_pipeline
          change_doc = { fullDocument: ( @options[:full_document] || FULL_DOCUMENT_DEFAULT ) }
          change_doc[:resumeAfter] = resume_token
          @pipeline.unshift({ '$changeNotification' => change_doc })
        end

        def process(result)
          if first_doc = result.first
            @resume_token = first_doc[:_id]
          end
          result
        end

        def send_initial_query(server)
          validate_collation!(server)
          process(initial_query_op.execute(server))
        end
      end
    end
  end
end
