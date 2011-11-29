package org.zeromq;

import static org.junit.Assert.assertArrayEquals;

import org.junit.Test;

public class ZMQForwarderTest {

	@Test
	public void testQueue() throws InterruptedException {
		ZMQ.Context context = ZMQ.context(1);

		ZMQ.Socket clients = context.socket(ZMQ.PAIR);
		clients.bind("inproc://fw_clients");

		ZMQ.Socket client = context.socket(ZMQ.PAIR);
		client.connect("inproc://fw_clients");

		ZMQ.Socket workers = context.socket(ZMQ.PAIR);
		workers.bind("inproc://fw_workers");

		ZMQ.Socket worker = context.socket(ZMQ.PAIR);
		worker.connect("inproc://fw_workers");

		Thread t = new Thread(new ZMQForwarder(context, clients, workers));
		t.start();

		for (int i = 0; i < 10; i++) {
			byte[] req = ("request" + i).getBytes();

			client.send(req, 0);

			// worker receives request
			byte[] reqTmp = worker.recv(0);

			assertArrayEquals(req, reqTmp);
		}
		
		t.interrupt();
	}
}
