package org.zeromq;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;


/**
 * @author Cliff Evans
 */
public class ZMQTest
{

  /**
   * Test method for {@link org.zeromq.ZMQ#makeVersion(int, int, int)}.
   */
  @Test
  public void testMakeVersion ()
  {
    assertEquals ( ZMQ.getFullVersion (),
                   ZMQ.makeVersion ( ZMQ.getMajorVersion (),
                                     ZMQ.getMinorVersion (),
                                     ZMQ.getPatchVersion () ) );
  }


  /**
   * Test method for {@link org.zeromq.ZMQ#getVersionString()}.
   */
  @Test
  public void testGetVersion ()
  {
    assertEquals ( ZMQ.getMajorVersion () + "." + ZMQ.getMinorVersion () + "." + ZMQ.getPatchVersion (),
                   ZMQ.getVersionString () );
  }
  
  @Test
  public void testReqRep ()
  {
	  	ZMQ.Context context = ZMQ.context(1);
	  
		ZMQ.Socket in = context.socket(ZMQ.REQ);
		in.bind("inproc://reqrep");

		ZMQ.Socket out = context.socket(ZMQ.REP);
		out.connect("inproc://reqrep");
	  
		for (int i = 0; i < 10; i++) {
			byte[] req = ("request" + i).getBytes();
			byte[] rep = ("reply" + i).getBytes();

			assertTrue(in.send(req, 0));
			byte[] reqTmp = out.recv(0);
			assertArrayEquals(req, reqTmp);
			
			assertTrue(out.send(rep, 0));
			byte[] repTmp = in.recv(0);
			assertArrayEquals(rep, repTmp);
		}		
  }

}
