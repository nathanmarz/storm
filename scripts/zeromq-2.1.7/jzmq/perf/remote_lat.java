/*
  Copyright (c) 2007-2010 iMatix Corporation

  This file is part of 0MQ.

  0MQ is free software; you can redistribute it and/or modify it under
  the terms of the Lesser GNU General Public License as published by
  the Free Software Foundation; either version 3 of the License, or
  (at your option) any later version.

  0MQ is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  Lesser GNU General Public License for more details.

  You should have received a copy of the Lesser GNU General Public License
  along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

import org.zeromq.ZMQ;

class remote_lat
{
    public static void main (String [] args)
    {
        if (args.length != 3) {
            System.out.println ("usage: remote_lat <connect-to> " +
                                "<message size> <roundtrip count>");
            return;
        }

        String connectTo = args [0];
        int messageSize = Integer.parseInt (args [1]);
        int roundtripCount = Integer.parseInt (args [2]);

        ZMQ.Context ctx = ZMQ.context (1);
        ZMQ.Socket s = ctx.socket (ZMQ.REQ);

        //  Add your socket options here.
        //  For example ZMQ_RATE, ZMQ_RECOVERY_IVL and ZMQ_MCAST_LOOP for PGM.

        s.connect (connectTo);

        long start = System.currentTimeMillis ();

        byte data [] = new byte [messageSize];
        for (int i = 0; i != roundtripCount; i ++) {
            s.send (data, 0);
            data = s.recv (0);
            assert (data.length == messageSize);
        }

        long end = System.currentTimeMillis ();

        long elapsed = (end - start) * 1000;
        double latency = (double) elapsed / roundtripCount / 2;

        System.out.println ("message size: " + messageSize + " [B]");
        System.out.println ("roundtrip count: " + roundtripCount);
        System.out.println ("mean latency: " + latency + " [us]");
    }
}

