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

class remote_thr
{
    public static void main (String [] args)
    {
        if (args.length != 3) {
            System.out.println ("usage: remote_thr <connect-to> " +
                                "<message-size> <message-count>");
            return;
        }

        //  Parse the command line arguments.
        String connectTo = args [0];
        int messageSize = Integer.parseInt (args [1]);
        int messageCount = Integer.parseInt (args [2]);

        ZMQ.Context ctx = ZMQ.context (1);
        ZMQ.Socket s = ctx.socket (ZMQ.PUB);

        //  Add your socket options here.
        //  For example ZMQ_RATE, ZMQ_RECOVERY_IVL and ZMQ_MCAST_LOOP for PGM.

        s.connect (connectTo);

        byte msg [] = new byte [messageSize];
        for (int i = 0; i != messageCount; i++)
            s.send (msg, 0);

        try {
            Thread.sleep (10000);
        }
        catch (InterruptedException e) {
            e.printStackTrace ();
        }
    }
}
