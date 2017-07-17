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

require 'zlib'

module Mongo
  module Protocol

    # MongoDB Wire protocol Compressed message.
    #
    # This is a bi-directional message that compresses another opcode.
    #
    # @api semipublic
    #
    # @since 2.5.0
    class Compressed < Message

      ZLIB_BYTE = 2.chr.force_encoding(BSON::BINARY).freeze

      ZLIB = 'zlib'.freeze

      COMPRESSOR_ID_MAP = {
                            ZLIB => ZLIB_BYTE
                          }.freeze

      # Creates a new OP_COMPRESSED message
      #
      # @example Create an OP_COMPRESSED message.
      #   Compressed.new(original_message, 'zlib')
      #
      # @param [ Mongo::Protocol::Message ] message The original message.
      # @param [ String, Symbol ] compressor The compression algorithm to use.
      #
      # @since 2.5.0
      def initialize(message, compressor)
        @original_message = message
        @original_op_code = message.op_code
        @uncompressed_size = 0
        @compressor_id = COMPRESSOR_ID_MAP[compressor]
        @compressed_message = ''
        @request_id = message.set_request_id
        @upconverter = message.send(:upconverter)
      end

      def inflate!
        message = Registry.get(@original_op_code).allocate
        uncompressed_message = Zlib::Inflate.inflate(@compressed_message)

        buf = BSON::ByteBuffer.new(uncompressed_message)

        message.send(:fields).each do |field|
          if field[:multi]
            Message.deserialize_array(message, buf, field)
          else
            Message.deserialize_field(message, buf, field)
          end
        end
        message
      end

      private

      # The operation code for a +Compressed+ message.
      # @return [ Fixnum ] the operation code.
      #
      # @since 2.5.0
      OP_CODE = 2012

      # @!attribute
      # Field representing the original message's op code as an Int32.
      field :original_op_code, Int32

      # @!attribute
      # @return [ Fixnum ] The size of the original message, excluding header as an Int32.
      field :uncompressed_size, Int32

      # @!attribute
      # @return [ String ] The id of the compressor as a single byte.
      field :compressor_id, Byte

      # @!attribute
      # @return [ String ] The actual compressed message bytes.
      field :compressed_message, Bytes

      def serialize_fields(buffer, max_bson_size)
        buf = BSON::ByteBuffer.new
        @original_message.send(:serialize_fields, buf)
        @uncompressed_size = buf.length
        @compressed_message = Zlib::Deflate.deflate(buf.to_s).force_encoding(BSON::BINARY)
        super
      end

      Registry.register(OP_CODE, self)
    end
  end
end
