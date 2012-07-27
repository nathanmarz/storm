#
# Autogenerated by Thrift
#
# DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
#

require 'thrift'
require 'storm_types'

        module BackType
          module Storm
            module DistributedRPCInvocations
              class Client
                include ::Thrift::Client

                def result(id, result)
                  send_result(id, result)
                  recv_result()
                end

                def send_result(id, result)
                  send_message('result', Result_args, :id => id, :result => result)
                end

                def recv_result()
                  result = receive_message(Result_result)
                  return
                end

                def fetchRequest(functionName)
                  send_fetchRequest(functionName)
                  return recv_fetchRequest()
                end

                def send_fetchRequest(functionName)
                  send_message('fetchRequest', FetchRequest_args, :functionName => functionName)
                end

                def recv_fetchRequest()
                  result = receive_message(FetchRequest_result)
                  return result.success unless result.success.nil?
                  raise ::Thrift::ApplicationException.new(::Thrift::ApplicationException::MISSING_RESULT, 'fetchRequest failed: unknown result')
                end

                def failRequest(id)
                  send_failRequest(id)
                  recv_failRequest()
                end

                def send_failRequest(id)
                  send_message('failRequest', FailRequest_args, :id => id)
                end

                def recv_failRequest()
                  result = receive_message(FailRequest_result)
                  return
                end

              end

              class Processor
                include ::Thrift::Processor

                def process_result(seqid, iprot, oprot)
                  args = read_args(iprot, Result_args)
                  result = Result_result.new()
                  @handler.result(args.id, args.result)
                  write_result(result, oprot, 'result', seqid)
                end

                def process_fetchRequest(seqid, iprot, oprot)
                  args = read_args(iprot, FetchRequest_args)
                  result = FetchRequest_result.new()
                  result.success = @handler.fetchRequest(args.functionName)
                  write_result(result, oprot, 'fetchRequest', seqid)
                end

                def process_failRequest(seqid, iprot, oprot)
                  args = read_args(iprot, FailRequest_args)
                  result = FailRequest_result.new()
                  @handler.failRequest(args.id)
                  write_result(result, oprot, 'failRequest', seqid)
                end

              end

              # HELPER FUNCTIONS AND STRUCTURES

              class Result_args
                include ::Thrift::Struct, ::Thrift::Struct_Union
                ID = 1
                RESULT = 2

                FIELDS = {
                  ID => {:type => ::Thrift::Types::STRING, :name => 'id'},
                  RESULT => {:type => ::Thrift::Types::STRING, :name => 'result'}
                }

                def struct_fields; FIELDS; end

                def validate
                end

                ::Thrift::Struct.generate_accessors self
              end

              class Result_result
                include ::Thrift::Struct, ::Thrift::Struct_Union

                FIELDS = {

                }

                def struct_fields; FIELDS; end

                def validate
                end

                ::Thrift::Struct.generate_accessors self
              end

              class FetchRequest_args
                include ::Thrift::Struct, ::Thrift::Struct_Union
                FUNCTIONNAME = 1

                FIELDS = {
                  FUNCTIONNAME => {:type => ::Thrift::Types::STRING, :name => 'functionName'}
                }

                def struct_fields; FIELDS; end

                def validate
                end

                ::Thrift::Struct.generate_accessors self
              end

              class FetchRequest_result
                include ::Thrift::Struct, ::Thrift::Struct_Union
                SUCCESS = 0

                FIELDS = {
                  SUCCESS => {:type => ::Thrift::Types::STRUCT, :name => 'success', :class => BackType::Storm::DRPCRequest}
                }

                def struct_fields; FIELDS; end

                def validate
                end

                ::Thrift::Struct.generate_accessors self
              end

              class FailRequest_args
                include ::Thrift::Struct, ::Thrift::Struct_Union
                ID = 1

                FIELDS = {
                  ID => {:type => ::Thrift::Types::STRING, :name => 'id'}
                }

                def struct_fields; FIELDS; end

                def validate
                end

                ::Thrift::Struct.generate_accessors self
              end

              class FailRequest_result
                include ::Thrift::Struct, ::Thrift::Struct_Union

                FIELDS = {

                }

                def struct_fields; FIELDS; end

                def validate
                end

                ::Thrift::Struct.generate_accessors self
              end

            end

          end
        end
