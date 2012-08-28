package backtype.storm.serialization.types;

import java.nio.ByteBuffer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class ByteBufferSerializer extends Serializer<ByteBuffer> {

		public void write (Kryo kryo, Output output, ByteBuffer object) {
			int position = object.position();
			int limit = object.limit();

			byte[] array;
			if (object.hasArray()) {
			  array = object.array();
			}
			else {
			  object.clear();
			  array = new byte[object.capacity()];
			  object.get(array, 0, array.length);
			  object.position(position);
			  object.limit(limit);
			}

			output.writeInt(array.length + 3, true);
			output.writeInt(position, true);
			output.writeInt(limit, true);
			output.writeBytes(array);
		}

		public ByteBuffer create (Kryo kryo, Input input, Class<ByteBuffer> type) {
			int length = input.readInt(true);
			int position = input.readInt(true);
			int limit = input.readInt(true);
			byte[] array = input.readBytes(length - 3);
			ByteBuffer bb = ByteBuffer.wrap(array);
			bb.limit(limit);
			bb.position(position);
			return bb;
  	}

}