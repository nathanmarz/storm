#
# Autogenerated by Thrift Compiler (0.7.0)
#
# DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
#

from thrift.Thrift import *
from ttypes import *
from thrift.Thrift import TProcessor
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol, TProtocol
try:
  from thrift.protocol import fastbinary
except:
  fastbinary = None


class Iface:
  def execute(self, functionName, funcArgs):
    """
    Parameters:
     - functionName
     - funcArgs
    """
    pass


class Client(Iface):
  def __init__(self, iprot, oprot=None):
    self._iprot = self._oprot = iprot
    if oprot is not None:
      self._oprot = oprot
    self._seqid = 0

  def execute(self, functionName, funcArgs):
    """
    Parameters:
     - functionName
     - funcArgs
    """
    self.send_execute(functionName, funcArgs)
    return self.recv_execute()

  def send_execute(self, functionName, funcArgs):
    self._oprot.writeMessageBegin('execute', TMessageType.CALL, self._seqid)
    args = execute_args()
    args.functionName = functionName
    args.funcArgs = funcArgs
    args.write(self._oprot)
    self._oprot.writeMessageEnd()
    self._oprot.trans.flush()

  def recv_execute(self, ):
    (fname, mtype, rseqid) = self._iprot.readMessageBegin()
    if mtype == TMessageType.EXCEPTION:
      x = TApplicationException()
      x.read(self._iprot)
      self._iprot.readMessageEnd()
      raise x
    result = execute_result()
    result.read(self._iprot)
    self._iprot.readMessageEnd()
    if result.success is not None:
      return result.success
    if result.e is not None:
      raise result.e
    if result.aze is not None:
      raise result.aze
    raise TApplicationException(TApplicationException.MISSING_RESULT, "execute failed: unknown result");


class Processor(Iface, TProcessor):
  def __init__(self, handler):
    self._handler = handler
    self._processMap = {}
    self._processMap["execute"] = Processor.process_execute

  def process(self, iprot, oprot):
    (name, type, seqid) = iprot.readMessageBegin()
    if name not in self._processMap:
      iprot.skip(TType.STRUCT)
      iprot.readMessageEnd()
      x = TApplicationException(TApplicationException.UNKNOWN_METHOD, 'Unknown function %s' % (name))
      oprot.writeMessageBegin(name, TMessageType.EXCEPTION, seqid)
      x.write(oprot)
      oprot.writeMessageEnd()
      oprot.trans.flush()
      return
    else:
      self._processMap[name](self, seqid, iprot, oprot)
    return True

  def process_execute(self, seqid, iprot, oprot):
    args = execute_args()
    args.read(iprot)
    iprot.readMessageEnd()
    result = execute_result()
    try:
      result.success = self._handler.execute(args.functionName, args.funcArgs)
    except DRPCExecutionException, e:
      result.e = e
    except AuthorizationException, aze:
      result.aze = aze
    oprot.writeMessageBegin("execute", TMessageType.REPLY, seqid)
    result.write(oprot)
    oprot.writeMessageEnd()
    oprot.trans.flush()


# HELPER FUNCTIONS AND STRUCTURES

class execute_args:
  """
  Attributes:
   - functionName
   - funcArgs
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRING, 'functionName', None, None, ), # 1
    (2, TType.STRING, 'funcArgs', None, None, ), # 2
  )

  def __init__(self, functionName=None, funcArgs=None,):
    self.functionName = functionName
    self.funcArgs = funcArgs

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRING:
          self.functionName = iprot.readString().decode('utf-8')
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.STRING:
          self.funcArgs = iprot.readString().decode('utf-8')
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('execute_args')
    if self.functionName is not None:
      oprot.writeFieldBegin('functionName', TType.STRING, 1)
      oprot.writeString(self.functionName.encode('utf-8'))
      oprot.writeFieldEnd()
    if self.funcArgs is not None:
      oprot.writeFieldBegin('funcArgs', TType.STRING, 2)
      oprot.writeString(self.funcArgs.encode('utf-8'))
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class execute_result:
  """
  Attributes:
   - success
   - e
   - aze
  """

  thrift_spec = (
    (0, TType.STRING, 'success', None, None, ), # 0
    (1, TType.STRUCT, 'e', (DRPCExecutionException, DRPCExecutionException.thrift_spec), None, ), # 1
    (2, TType.STRUCT, 'aze', (AuthorizationException, AuthorizationException.thrift_spec), None, ), # 2
  )

  def __init__(self, success=None, e=None, aze=None,):
    self.success = success
    self.e = e
    self.aze = aze

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 0:
        if ftype == TType.STRING:
          self.success = iprot.readString().decode('utf-8')
        else:
          iprot.skip(ftype)
      elif fid == 1:
        if ftype == TType.STRUCT:
          self.e = DRPCExecutionException()
          self.e.read(iprot)
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.STRUCT:
          self.aze = AuthorizationException()
          self.aze.read(iprot)
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('execute_result')
    if self.success is not None:
      oprot.writeFieldBegin('success', TType.STRING, 0)
      oprot.writeString(self.success.encode('utf-8'))
      oprot.writeFieldEnd()
    if self.e is not None:
      oprot.writeFieldBegin('e', TType.STRUCT, 1)
      self.e.write(oprot)
      oprot.writeFieldEnd()
    if self.aze is not None:
      oprot.writeFieldBegin('aze', TType.STRUCT, 2)
      self.aze.write(oprot)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)
