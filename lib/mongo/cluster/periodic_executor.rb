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

module Mongo
  class Cluster

    class PeriodicExecutor

      # The default time interval for the reaper executor to execute.
      #
      # @since 2.5.0
      FREQUENCY = 1.freeze

      # Create a reaper executor.
      #
      # @example Create a PeriodicExecutor.
      #   Mongo::Cluster::PeriodicExecutor.new(reaper, reaper2)
      #
      # @api private
      #
      # @since 2.5.0
      def initialize(*executors)
        @thread = nil
        @executors = executors
      end

      # Start the thread.
      #
      # @example Start the reaper executor's thread.
      #   periodic_executor.run!
      #
      # @api private
      #
      # @since 2.5.0
      def run!
        @thread && @thread.alive? ? @thread : start!
      end
      alias :restart! :run!

      # Stop the executor's thread.
      #
      # @example Stop the executors's thread.
      #   periodic_executor.stop!
      #
      # @api private
      #
      # @since 2.5.0
      def stop!
        @thread.kill && @thread.stop?
      end

      # Trigger an execute call on each reaper.
      #
      # @example Execute all reapers.
      #   periodic_executor.execute
      #
      # @api private
      #
      # @since 2.5.0
      def execute
        @executors.each { |executor| executor.execute }
      end

      private

      def start!
        @thread = Thread.new(FREQUENCY) do |i|
          loop do
            sleep(i)
            execute
          end
        end
      end
    end
  end
end
