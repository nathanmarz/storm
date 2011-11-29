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
package org.zeromq;

import java.util.LinkedList;

/**
 * ZeroMQ JNI Bindings.
 * 
 * @author Gonzalo Diethelm
 * 
 */
public class ZMQ {
	
	static {
		// if no embedded native library, revert to loading from java.library.path
		if (!EmbeddedLibraryTools.LOADED_EMBEDDED_LIBRARY)
			System.loadLibrary ("jzmq");
	}

    // Values for flags in Socket's send and recv functions.
    /**
     * Socket flag to indicate a nonblocking send or recv mode.
     */
    public static final int NOBLOCK = 1;
    public static final int DONTWAIT = 1;
    /**
     * Socket flag to indicate that more message parts are coming.
     */
    public static final int SNDMORE = 2;

    // Socket types, used when creating a Socket.
    /**
     * Flag to specify a exclusive pair of sockets.
     */
    public static final int PAIR = 0;
    /**
     * Flag to specify a PUB socket, receiving side must be a SUB.
     */
    public static final int PUB = 1;
    /**
     * Flag to specify the receiving part of the PUB socket.
     */
    public static final int SUB = 2;
    /**
     * Flag to specify a REQ socket, receiving side must be a REP.
     */
    public static final int REQ = 3;
    /**
     * Flag to specify the receiving part of a REQ socket.
     */
    public static final int REP = 4;
    /**
     * Flag to specify a XREQ socket, receiving side must be a XREP.
     */
    public static final int XREQ = 5;
    /**
     * Flag to specify the receiving part of a XREQ socket.
     */
    public static final int XREP = 6;
    /**
     * Flag to specify the receiving part of a PUSH socket.
     */
    public static final int PULL = 7;
    /**
     * Flag to specify a PUSH socket, receiving side must be a PULL.
     */
    public static final int PUSH = 8;

    /**
     * Flag to specify a STREAMER device.
     */
    public static final int STREAMER = 1;

    /**
     * Flag to specify a FORWARDER device.
     */
    public static final int FORWARDER = 2;

    /**
     * Flag to specify a QUEUE device.
     */
    public static final int QUEUE = 3;

    /**
     * @see ZMQ#PULL
     */
    @Deprecated
        public static final int UPSTREAM = PULL;
    /**
     * @see ZMQ#PUSH
     */
    @Deprecated
        public static final int DOWNSTREAM = PUSH;


    /**
     * @return Major version number of the ZMQ library.
     */
    public static int getMajorVersion ()
    {
      return version_major ();
    }
  
  
    /**
     * @return Major version number of the ZMQ library.
     */
    public static int getMinorVersion ()
    {
      return version_minor ();
    }
  
  
    /**
     * @return Major version number of the ZMQ library.
     */
    public static int getPatchVersion ()
    {
      return version_patch ();
    }
  
  
    /**
     * @return Full version number of the ZMQ library used for comparing versions.
     */
    public static int getFullVersion ()
    {
      return version_full ();
    }
  
  
    /**
     * @param major Version major component.
     * @param minor Version minor component.
     * @param patch Version patch component.
     * 
     * @return Comparible single int version number.
     */
    public static int makeVersion ( final int major,
                                    final int minor,
                                    final int patch )
    {
      return make_version ( major, minor, patch );
    }
  
  
    /**
     * @return String version number in the form major.minor.patch.
     */
    public static String getVersionString ()
    {
      return String.format ( "%d.%d.%d",
                             version_major (),
                             version_minor (),
                             version_patch () );
    }
  

    protected static native int version_full();
    protected static native int version_major();
    protected static native int version_minor();
    protected static native int version_patch();
    protected static native int make_version(int major, int minor, int patch);

    protected static native long ENOTSUP();
    protected static native long EPROTONOSUPPORT();
    protected static native long ENOBUFS();
    protected static native long ENETDOWN();
    protected static native long EADDRINUSE();
    protected static native long EADDRNOTAVAIL();
    protected static native long ECONNREFUSED();
    protected static native long EINPROGRESS();
    protected static native long EMTHREAD();
    protected static native long EFSM();
    protected static native long ENOCOMPATPROTO();
    protected static native long ETERM();         
    
    /**
     * Inner class: Error.
     */
    public enum Error {
        
        ENOTSUP(ENOTSUP()),
            
            EPROTONOSUPPORT(EPROTONOSUPPORT()),
		
            ENOBUFS(ENOBUFS()),
		
            ENETDOWN(ENETDOWN()),
		
            EADDRINUSE(EADDRINUSE()),

            EADDRNOTAVAIL(EADDRNOTAVAIL()),
		
            ECONNREFUSED(ECONNREFUSED()),
		
            EINPROGRESS(EINPROGRESS()),
		
            EMTHREAD(EMTHREAD()),
		
            EFSM(EFSM()),
		
            ENOCOMPATPROTO(ENOCOMPATPROTO()),
		
            ETERM(ETERM());

        private final long code;

        Error(long code) {
            this.code = code;
        }

        public long getCode() {
            return code;
        }

        public static Error findByCode(int code) {
            for (Error e : Error.class.getEnumConstants()) {
                if (e.getCode() == code) {
                    return e;
                }
            }
            throw new IllegalArgumentException("Unknown " + Error.class.getName() + " enum code:" + code);
        }
    }
	
    /**
     * Create a new Context.
     * 
     * @param ioThreads
     *            Number of threads to use, usually 1 is sufficient for most use cases.
     * @return the Context
     */
    public static Context context (int ioThreads) {
        return new Context (ioThreads);
    }

    /**
     * Inner class: Context.
     */
    public static class Context {

        /**
         * This is an explicit "destructor". It can be called to ensure the corresponding 0MQ
         * Context has been disposed of.
         */
        public void term () {
            finalize ();
        }

        /**
         * Create a new Socket within this context.
         * 
         * @param type
         *            the socket type.
         * @return the newly created Socket.
         */
        public Socket socket (int type) {
            return new Socket (this, type);
        }

        /**
         * Create a new Poller within this context, with a default size.
         * 
         * @return the newly created Poller.
         */
        public Poller poller () {
            return new Poller (this);
        }

        /**
         * Create a new Poller within this context, with a specified initial size.
         * 
         * @param size
         *            the poller initial size.
         * @return the newly created Poller.
         */
        public Poller poller (int size) {
            return new Poller (this, size);
        }

        /**
         * Class constructor.
         * 
         * @param ioThreads
         *            size of the threads pool to handle I/O operations.
         */
        protected Context (int ioThreads) {
            construct (ioThreads);
        }

        /** Initialize the JNI interface */
        protected native void construct (int ioThreads);

        /** Free all resources used by JNI interface. */
        @Override
            protected native void finalize ();

        /**
         * Get the underlying context handle. This is private because it is only accessed from JNI,
         * where Java access controls are ignored.
         * 
         * @return the internal 0MQ context handle.
         */
        private long getContextHandle () {
            return this.contextHandle;
        }

        /** Opaque data used by JNI driver. */
        private long contextHandle;
    }

    /**
     * Inner class: Socket.
     */
    public static class Socket {
        /**
         * This is an explicit "destructor". It can be called to ensure the corresponding 0MQ Socket
         * has been disposed of.
         */
        public void close () {
            finalize ();
        }

        /**
         * The 'ZMQ_TYPE option shall retrieve the socket type for the specified
         * 'socket'.  The socket type is specified at socket creation time and
         * cannot be modified afterwards.
         *
         * @return the socket type.
         * @since 2.1.0
         */
        public long getType () {
            if (ZMQ.version_full() < ZMQ.make_version(2, 1, 0))
                return -1;

            return getLongSockopt (TYPE);
        }

        /**
         * @see #setLinger(long)
         *
         * @return the linger period.
         * @since 2.1.0
         */
        public long getLinger () {
            if (ZMQ.version_full() < ZMQ.make_version(2, 1, 0))
                return -1;

            return getLongSockopt (LINGER);
        }

        /**
         * @see #setReconnectIVL(long)
         *
         * @return the reconnectIVL.
         * @since 3.0.0
         */
        public long getReconnectIVL () {
            if (ZMQ.version_full() < ZMQ.make_version(3, 0, 0))
                return -1;

            return getLongSockopt (RECONNECT_IVL);
        }

        /**
         * @see #setBacklog(long)
         *
         * @return the backlog.
         * @since 3.0.0
         */
        public long getBacklog () {
            if (ZMQ.version_full() < ZMQ.make_version(3, 0, 0))
                return -1;

            return getLongSockopt (BACKLOG);
        }

        /**
         * @see #setReconnectIVLMax(long)
         *
         * @return the reconnectIVLMax.
         * @since 3.0.0
         */
        public long getReconnectIVLMax () {
            if (ZMQ.version_full() < ZMQ.make_version(3, 0, 0))
                return -1;

            return getLongSockopt (RECONNECT_IVL_MAX);
        }

        /**
         * @see #setMaxMsgSize(long)
         *
         * @return the maxMsgSize.
         * @since 3.0.0
         */
        public long getMaxMsgSize () {
            if (ZMQ.version_full() < ZMQ.make_version(3, 0, 0))
                return -1;

            return getLongSockopt (MAXMSGSIZE);
        }

        /**
         * @see #setSndHWM(long)
         *
         * @return the SndHWM.
         * @since 3.0.0
         */
        public long getSndHWM () {
            if (ZMQ.version_full() < ZMQ.make_version(3, 0, 0))
                return -1;

            return getLongSockopt (SNDHWM);
        }

        /**
         * @see #setRcvHWM(long)
         *
         * @return the recvHWM period.
         * @since 3.0.0
         */
        public long getRcvHWM () {
            if (ZMQ.version_full() < ZMQ.make_version(3, 0, 0))
                return -1;

            return getLongSockopt (RCVHWM);
        }

        /**
         * @see #setHWM(long)
         * 
         * @return the High Water Mark.
         */
        public long getHWM () {
            if (ZMQ.version_full() >= ZMQ.make_version(3, 0, 0))
                return -1;

            return getLongSockopt (HWM);
        }

        /**
         * @see #setSwap(long)
         * 
         * @return the number of messages to swap at most.
         */
        public long getSwap () {
            if (ZMQ.version_full() >= ZMQ.make_version(3, 0, 0))
                return -1;

            return getLongSockopt (SWAP);
        }

        /**
         * @see #setAffinity(long)
         * 
         * @return the affinity.
         */
        public long getAffinity () {
            return getLongSockopt (AFFINITY);
        }

        /**
         * @see #setIdentity(byte[])
         * 
         * @return the Identitiy.
         */
        public byte [] getIdentity () {
            return getBytesSockopt (IDENTITY);
        }

        /**
         * @see #setRate(long)
         * 
         * @return the Rate.
         */
        public long getRate () {
            return getLongSockopt (RATE);
        }

        /**
         * @see #setRecoveryInterval(long)
         * 
         * @return the RecoveryIntervall.
         */
        public long getRecoveryInterval () {
            return getLongSockopt (RECOVERY_IVL);
        }

        /**
         * @see #setMulticastLoop(boolean)
         * 
         * @return the Multicast Loop.
         */
        public boolean hasMulticastLoop () {
            if (ZMQ.version_full() >= ZMQ.make_version(3, 0, 0))
                return false;

            return getLongSockopt (MCAST_LOOP) != 0;
        }

        /**
         * @see #setSendBufferSize(long)
         * 
         * @return the kernel send buffer size.
         */
        public long getSendBufferSize () {
            return getLongSockopt (SNDBUF);
        }

        /**
         * @see #setReceiveBufferSize(long)
         * 
         * @return the kernel receive buffer size.
         */
        public long getReceiveBufferSize () {
            return getLongSockopt (RCVBUF);
        }

        /**
         * The 'ZMQ_RCVMORE' option shall return a boolean value indicating if the multi-part
         * message currently being read from the specified 'socket' has more message parts to
         * follow. If there are no message parts to follow or if the message currently being read is
         * not a multi-part message a value of zero shall be returned. Otherwise, a value of 1 shall
         * be returned.
         * 
         * @return true if there are more messages to receive.
         */
        public boolean hasReceiveMore () {
            return getLongSockopt (RCVMORE) != 0;
        }

        /**
         * The 'ZMQ_FD' option shall retrieve file descriptor associated with the 0MQ
         * socket. The descriptor can be used to integrate 0MQ socket into an existing
         * event loop. It should never be used for anything else than polling -- such as
         * reading or writing. The descriptor signals edge-triggered IN event when
         * something has happened within the 0MQ socket. It does not necessarily mean that
         * the messages can be read or written. Check ZMQ_EVENTS option to find out whether
         * the 0MQ socket is readable or writeable.
         * 
         * @return the underlying file descriptor.
         * @since 2.1.0
         */
        public long getFD () {
            if (ZMQ.version_full() < ZMQ.make_version(2, 1, 0))
                return -1;

            return getLongSockopt (FD);
        }

        /**
         * The 'ZMQ_EVENTS' option shall retrieve event flags for the specified socket.
         * If a message can be read from the socket ZMQ_POLLIN flag is set. If message can
         * be written to the socket ZMQ_POLLOUT flag is set.
         * 
         * @return the mask of outstanding events.
         * @since 2.1.0
         */
        public long getEvents () {
            if (ZMQ.version_full() < ZMQ.make_version(2, 1, 0))
                return -1;

            return getLongSockopt (EVENTS);
        }

        /**
         * The 'ZMQ_LINGER' option shall retrieve the period for pending outbound
         * messages to linger in memory after closing the socket. Value of -1 means
         * infinite. Pending messages will be kept until they are fully transferred to
         * the peer. Value of 0 means that all the pending messages are dropped immediately
         * when socket is closed. Positive value means number of milliseconds to keep
         * trying to send the pending messages before discarding them.
         *
         * @param linger
         *            the linger period.
         * @since 2.1.0
         */
        public void setLinger (long linger) {
            if (ZMQ.version_full() < ZMQ.make_version(2, 1, 0))
                return;

            setLongSockopt (LINGER, linger);
        }

        /**
         * @since 3.0.0
         */
        public void setReconnectIVL (long reconnectIVL) {
            if (ZMQ.version_full() < ZMQ.make_version(3, 0, 0))
                return;

            setLongSockopt (RECONNECT_IVL, reconnectIVL);
        }

        /**
         * @since 3.0.0
         */
        public void setBacklog (long backlog) {
            if (ZMQ.version_full() < ZMQ.make_version(3, 0, 0))
                return;

            setLongSockopt (BACKLOG, backlog);
        }

        /**
         * @since 3.0.0
         */
        public void setReconnectIVLMax (long reconnectIVLMax) {
            if (ZMQ.version_full() < ZMQ.make_version(3, 0, 0))
                return;

            setLongSockopt (RECONNECT_IVL_MAX, reconnectIVLMax);
        }

        /**
         * @since 3.0.0
         */
        public void setMaxMsgSize (long maxMsgSize) {
            if (ZMQ.version_full() < ZMQ.make_version(3, 0, 0))
                return;

            setLongSockopt (MAXMSGSIZE, maxMsgSize);
        }

        /**
         * @since 3.0.0
         */
        public void setSndHWM (long sndHWM) {
            if (ZMQ.version_full() < ZMQ.make_version(3, 0, 0))
                return;

            setLongSockopt (SNDHWM, sndHWM);
        }

        /**
         * @since 3.0.0
         */
        public void setRcvHWM (long rcvHWM) {
            if (ZMQ.version_full() < ZMQ.make_version(3, 0, 0))
                return;

            setLongSockopt (RCVHWM, rcvHWM);
        }

        /**
         * The 'ZMQ_HWM' option shall set the high water mark for the specified 'socket'. The high
         * water mark is a hard limit on the maximum number of outstanding messages 0MQ shall queue
         * in memory for any single peer that the specified 'socket' is communicating with.
         * 
         * If this limit has been reached the socket shall enter an exceptional state and depending
         * on the socket type, 0MQ shall take appropriate action such as blocking or dropping sent
         * messages. Refer to the individual socket descriptions in the man page of zmq_socket[3] for
         * details on the exact action taken for each socket type.
         * 
         * @param hwm
         *            the number of messages to queue.
         */
        public void setHWM (long hwm) {
            if (ZMQ.version_full() >= ZMQ.make_version(3, 0, 0))
                return;

            setLongSockopt (HWM, hwm);
        }

        /**
         * Get the Swap. The 'ZMQ_SWAP' option shall set the disk offload (swap) size for the
         * specified 'socket'. A socket which has 'ZMQ_SWAP' set to a non-zero value may exceed its
         * high water mark; in this case outstanding messages shall be offloaded to storage on disk
         * rather than held in memory.
         * 
         * @param swap
         *            The value of 'ZMQ_SWAP' defines the maximum size of the swap space in bytes.
         */
        public void setSwap (long swap) {
            if (ZMQ.version_full() >= ZMQ.make_version(3, 0, 0))
                return;

            setLongSockopt (SWAP, swap);
        }

        /**
         * Get the Affinity. The 'ZMQ_AFFINITY' option shall set the I/O thread affinity for newly
         * created connections on the specified 'socket'.
         * 
         * Affinity determines which threads from the 0MQ I/O thread pool associated with the
         * socket's _context_ shall handle newly created connections. A value of zero specifies no
         * affinity, meaning that work shall be distributed fairly among all 0MQ I/O threads in the
         * thread pool. For non-zero values, the lowest bit corresponds to thread 1, second lowest
         * bit to thread 2 and so on. For example, a value of 3 specifies that subsequent
         * connections on 'socket' shall be handled exclusively by I/O threads 1 and 2.
         * 
         * See also  in the man page of zmq_init[3] for details on allocating the number of I/O threads for a
         * specific _context_.
         * 
         * @param affinity
         *            the affinity.
         */
        public void setAffinity (long affinity) {
            setLongSockopt (AFFINITY, affinity);
        }

        /**
         * The 'ZMQ_IDENTITY' option shall set the identity of the specified 'socket'. Socket
         * identity determines if existing 0MQ infastructure (_message queues_, _forwarding
         * devices_) shall be identified with a specific application and persist across multiple
         * runs of the application.
         * 
         * If the socket has no identity, each run of an application is completely separate from
         * other runs. However, with identity set the socket shall re-use any existing 0MQ
         * infrastructure configured by the previous run(s). Thus the application may receive
         * messages that were sent in the meantime, _message queue_ limits shall be shared with
         * previous run(s) and so on.
         * 
         * Identity should be at least one byte and at most 255 bytes long. Identities starting with
         * binary zero are reserved for use by 0MQ infrastructure.
         * 
         * @param identity
         */
        public void setIdentity (byte [] identity) {
            setBytesSockopt (IDENTITY, identity);
        }

        /**
         * The 'ZMQ_SUBSCRIBE' option shall establish a new message filter on a 'ZMQ_SUB' socket.
         * Newly created 'ZMQ_SUB' sockets shall filter out all incoming messages, therefore you
         * should call this option to establish an initial message filter.
         * 
         * An empty 'option_value' of length zero shall subscribe to all incoming messages. A
         * non-empty 'option_value' shall subscribe to all messages beginning with the specified
         * prefix. Mutiple filters may be attached to a single 'ZMQ_SUB' socket, in which case a
         * message shall be accepted if it matches at least one filter.
         * 
         * @param topic
         */
        public void subscribe (byte [] topic) {
            setBytesSockopt (SUBSCRIBE, topic);
        }

        /**
         * The 'ZMQ_UNSUBSCRIBE' option shall remove an existing message filter on a 'ZMQ_SUB'
         * socket. The filter specified must match an existing filter previously established with
         * the 'ZMQ_SUBSCRIBE' option. If the socket has several instances of the same filter
         * attached the 'ZMQ_UNSUBSCRIBE' option shall remove only one instance, leaving the rest in
         * place and functional.
         * 
         * @param topic
         */
        public void unsubscribe (byte [] topic) {
            setBytesSockopt (UNSUBSCRIBE, topic);
        }

        /**
         * The 'ZMQ_RATE' option shall set the maximum send or receive data rate for multicast
         * transports such as  in the man page of zmq_pgm[7] using the specified 'socket'.
         * 
         * @param rate
         */
        public void setRate (long rate) {
            setLongSockopt (RATE, rate);
        }

        /**
         * The 'ZMQ_RECOVERY_IVL' option shall set the recovery interval for multicast transports
         * using the specified 'socket'. The recovery interval determines the maximum time in
         * seconds that a receiver can be absent from a multicast group before unrecoverable data
         * loss will occur.
         * 
         * CAUTION: Excersize care when setting large recovery intervals as the data needed for
         * recovery will be held in memory. For example, a 1 minute recovery interval at a data rate
         * of 1Gbps requires a 7GB in-memory buffer. {Purpose of this Method}
         * 
         * @param recovery_ivl
         */
        public void setRecoveryInterval (long recovery_ivl) {
            setLongSockopt (RECOVERY_IVL, recovery_ivl);
        }

        /**
         * The 'ZMQ_MCAST_LOOP' option shall control whether data sent via multicast transports
         * using the specified 'socket' can also be received by the sending host via loopback. A
         * value of zero disables the loopback functionality, while the default value of 1 enables
         * the loopback functionality. Leaving multicast loopback enabled when it is not required
         * can have a negative impact on performance. Where possible, disable 'ZMQ_MCAST_LOOP' in
         * production environments.
         * 
         * @param mcast_loop
         */
        public void setMulticastLoop (boolean mcast_loop) {
            if (ZMQ.version_full() >= ZMQ.make_version(3, 0, 0))
                return;

            setLongSockopt (MCAST_LOOP, mcast_loop ? 1 : 0);
        }

        /**
         * The 'ZMQ_SNDBUF' option shall set the underlying kernel transmit buffer size for the
         * 'socket' to the specified size in bytes. A value of zero means leave the OS default
         * unchanged. For details please refer to your operating system documentation for the
         * 'SO_SNDBUF' socket option.
         * 
         * @param sndbuf
         */
        public void setSendBufferSize (long sndbuf) {
            setLongSockopt (SNDBUF, sndbuf);
        }

        /**
         * The 'ZMQ_RCVBUF' option shall set the underlying kernel receive buffer size for the
         * 'socket' to the specified size in bytes. A value of zero means leave the OS default
         * unchanged. For details refer to your operating system documentation for the 'SO_RCVBUF'
         * socket option.
         * 
         * @param rcvbuf
         */
        public void setReceiveBufferSize (long rcvbuf) {
            setLongSockopt (RCVBUF, rcvbuf);
        }

        /**
         * Bind to network interface. Start listening for new connections.
         * 
         * @param addr
         *            the endpoint to bind to.
         */
        public native void bind (String addr);

        /**
         * Connect to remote application.
         * 
         * @param addr
         *            the endpoint to connect to.
         */
        public native void connect (String addr);

        /**
         * Send a message.
         * 
         * @param msg
         *            the message to send, as an array of bytes.
         * @param flags
         *            the flags to apply to the send operation.
         * @return true if send was successful, false otherwise.
         */
        public native boolean send (byte [] msg, int flags);

        /**
         * Receive a message.
         * 
         * @param flags
         *            the flags to apply to the receive operation.
         * @return the message received, as an array of bytes; null on error.
         */
        public native byte [] recv (int flags);

        /**
         * Class constructor.
         * 
         * @param context
         *            a 0MQ context previously created.
         * @param type
         *            the socket type.
         */
        protected Socket (Context context, int type) {
            // We keep a local handle to context so that
            // garbage collection won't be too greedy on it.
            this.context = context;
            construct (context, type);
        }

        /** Initialize the JNI interface */
        protected native void construct (Context ctx, int type);

        /** Free all resources used by JNI interface. */
        @Override
            protected native void finalize ();

        /**
         * Get the socket option value, as a long.
         * 
         * @param option
         *            ID of the option to set.
         * @return The socket option value (as a long).
         */
        protected native long getLongSockopt (int option);

        /**
         * Get the socket option value, as a byte array.
         * 
         * @param option
         *            ID of the option to set.
         * @return The socket option value (as a byte array).
         */
        protected native byte [] getBytesSockopt (int option);

        /**
         * Set the socket option value, given as a long.
         * 
         * @param option
         *            ID of the option to set.
         * @param optval
         *            value (as a long) to set the option to.
         */
        protected native void setLongSockopt (int option, long optval);

        /**
         * Set the socket option value, given as a byte array.
         * 
         * @param option
         *            ID of the option to set.
         * @param optval
         *            value (as a byte array) to set the option to.
         */
        protected native void setBytesSockopt (int option, byte [] optval);

        /**
         * Get the underlying socket handle. This is private because it is only accessed from JNI,
         * where Java access controls are ignored.
         * 
         * @return the internal 0MQ socket handle.
         */
        private long getSocketHandle () {
            return this.socketHandle;
        }

        /** Opaque data used by JNI driver. */
        private long socketHandle;
        private Context context = null;
        // private Constants use the appropriate setter instead.
        private static final int HWM = 1;
        // public static final int LWM = 2; // No longer supported
        private static final int SWAP = 3;
        private static final int AFFINITY = 4;
        private static final int IDENTITY = 5;
        private static final int SUBSCRIBE = 6;
        private static final int UNSUBSCRIBE = 7;
        private static final int RATE = 8;
        private static final int RECOVERY_IVL = 9;
        private static final int MCAST_LOOP = 10;
        private static final int SNDBUF = 11;
        private static final int RCVBUF = 12;
        private static final int RCVMORE = 13;
        private static final int FD = 14;
        private static final int EVENTS = 15;
        private static final int TYPE = 16;
        private static final int LINGER = 17;
        private static final int RECONNECT_IVL = 18;
        private static final int BACKLOG = 19;
        private static final int RECONNECT_IVL_MAX = 21;
        private static final int MAXMSGSIZE = 22;
        private static final int SNDHWM = 23;
        private static final int RCVHWM = 24;
    }

    /**
     * Inner class: Poller.
     */
    public static class Poller {
        /**
         * These values can be ORed to specify what we want to poll for.
         */
        public static final int POLLIN = 1;
        public static final int POLLOUT = 2;
        public static final int POLLERR = 4;

        /**
         * Register a Socket for polling on all events.
         * 
         * @param socket
         *            the Socket we are registering.
         * @return the index identifying this Socket in the poll set.
         */
        public int register (Socket socket) {
            return register (socket, POLLIN | POLLOUT | POLLERR);
        }

        /**
         * Register a Socket for polling on the specified events.
         *
         * Automatically grow the internal representation if needed.
         * 
         * @param socket
         *            the Socket we are registering.
         * @param events
         *            a mask composed by XORing POLLIN, POLLOUT and POLLERR.
         * @return the index identifying this Socket in the poll set.
         */
        public int register (Socket socket, int events) {
            int pos = -1;

            if (! this.freeSlots.isEmpty()) {
                // If there are free slots in our array, remove one
                // from the free list and use it.
                pos = this.freeSlots.remove();
            } else {
                if (this.next >= this.size) {
                    // It is necessary to grow the arrays.

                    // Compute new size for internal arrays.
                    int nsize = this.size + SIZE_INCREMENT;

                    // Create new internal arrays.
                    Socket [] ns = new Socket [nsize];
                    short [] ne = new short [nsize];
                    short [] nr = new short [nsize];
                    
                    // Copy contents of current arrays into new arrays.
                    for (int i = 0; i < this.next; ++i) {
                        ns[i] = this.sockets[i];
                        ne[i] = this.events[i];
                        nr[i] = this.revents[i];
                    }
                    
                    // Swap internal arrays and size to new values.
                    this.size = nsize;
                    this.sockets = ns;
                    this.events = ne;
                    this.revents = nr;
                }
                pos = this.next++;
            }

            this.sockets[pos] = socket;
            this.events[pos] = (short) events;
            this.used++;
            return pos;
        }

        /**
         * Unregister a Socket for polling on the specified events.
         *
         * @param socket 
         *          the Socket to be unregistered
         */
        public void unregister (Socket socket) {
            for (int i = 0; i < this.next; ++i) {
                if (this.sockets[i] == socket) {
                    this.sockets[i] = null;
                    this.events[i] = 0;
                    this.revents[i] = 0;
                    
                    this.freeSlots.add(i);
                    --this.used;

                    break;
                }
            }
        }

        /**
         * Get the socket associated with an index.
         * 
         * @param index
         *            the desired index.
         * @return the Socket associated with that index (or null).
         */
        public Socket getSocket (int index) {
            if (index < 0 || index >= this.next)
                return null;
            return this.sockets [index];
        }

        /**
         * Get the current poll timeout.
         * 
         * @return the current poll timeout in microseconds.
         * @deprecated Timeout handling has been moved to the poll() methods.
         */
        public long getTimeout () {
            return this.timeout;
        }

        /**
         * Set the poll timeout.
         * 
         * @param timeout
         *            the desired poll timeout in microseconds.
         * @deprecated Timeout handling has been moved to the poll() methods.
         */
        public void setTimeout (long timeout) {
            if (timeout < -1)
                return;

            this.timeout = timeout;
        }

        /**
         * Get the current poll set size.
         * 
         * @return the current poll set size.
         */
        public int getSize () {
            return this.size;
        }

        /**
         * Get the index for the next position in the poll set size.
         * 
         * @return the index for the next position in the poll set size.
         */
        public int getNext () {
            return this.next;
        }

        /**
         * Issue a poll call. If the poller's internal timeout value
         * has been set, use that value as timeout; otherwise, block
         * indefinitely.
         * 
         * @return how many objects where signalled by poll ().
         */
        public long poll () {
            long tout = -1;
            if (this.timeout > -1) {
                tout = this.timeout;
            }
            return poll(tout);
        }

        /**
         * Issue a poll call, using the specified timeout value.
         * 
         * @param tout
         *            the timeout in microseconds, as per zmq_poll ();
         *            if -1, it will block indefinitely until an event
         *            happens; if 0, it will return immediately;
         *            otherwise, it will wait for at most that many
         *            microseconds.
         *
         * @return how many objects where signalled by poll ()
         */
        public long poll (long tout) {
            if (tout < -1) {
                return 0;
            }

            if (this.size <= 0 || this.next <= 0) {
                return 0;
            }

            for (int i = 0; i < this.next; ++i) {
                this.revents [i] = 0;
            }
            return run_poll (this.used, this.sockets, this.events, this.revents, tout);
        }

        /**
         * Check whether the specified element in the poll set was signalled for input.
         * 
         * @param index
         * 
         * @return true if the element was signalled.
         */
        public boolean pollin (int index) {
            return poll_mask (index, POLLIN);
        }

        /**
         * Check whether the specified element in the poll set was signalled for output.
         * 
         * @param index
         * 
         * @return true if the element was signalled.
         */
        public boolean pollout (int index) {
            return poll_mask (index, POLLOUT);
        }

        /**
         * Check whether the specified element in the poll set was signalled for error.
         * 
         * @param index
         * 
         * @return true if the element was signalled.
         */
        public boolean pollerr (int index) {
            return poll_mask (index, POLLERR);
        }

        /**
         * Class constructor.
         * 
         * @param context
         *            a 0MQ context previously created.
         */
        protected Poller (Context context) {
            this(context, SIZE_DEFAULT);
        }

        /**
         * Class constructor.
         * 
         * @param context
         *            a 0MQ context previously created.
         * @param size
         *            the number of Sockets this poller will contain.
         */
        protected Poller (Context context, int size) {
            this.context = context;
            this.size = size;
            this.next = 0;

            this.sockets = new Socket [this.size];
            this.events = new short [this.size];
            this.revents = new short [this.size];

            freeSlots = new LinkedList<Integer>();
        }

        /**
         * Issue a poll call on the specified 0MQ sockets.
         * 
         * @param sockets
         *            an array of 0MQ Socket objects to poll.
         * @param events
         *            an array of short values specifying what to poll for.
         * @param revents
         *            an array of short values with the results.
         * @param timeout
         *            the maximum timeout in microseconds.
         * @return how many objects where signalled by poll ().
         */
        private native long run_poll (int count, Socket [] sockets, short [] events, short [] revents, long timeout);

        /**
         * Check whether a specific mask was signalled by latest poll call.
         * 
         * @param index
         *            the index indicating the socket.
         * @param mask
         *            a combination of POLLIN, POLLOUT and POLLERR.
         * @return true if specific socket was signalled as specified.
         */
        private boolean poll_mask (int index, int mask) {
            if (mask <= 0 || index < 0 || index >= this.next) {
                return false;
            }
            return (this.revents [index] & mask) > 0;
        }

        private Context context = null;
        private long timeout = -2; // mark as uninitialized
        private int size = 0;
        private int next = 0;
        private int used = 0;
        private Socket [] sockets = null;
        private short [] events = null;
        private short [] revents = null;
        // When socket is removed from polling, store free slots here
        private LinkedList<Integer> freeSlots = null;

        private static final int SIZE_DEFAULT = 32;
        private static final int SIZE_INCREMENT = 16;
    }
}
