package backtype.storm.security.auth;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslServer;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.Configuration;
import javax.security.auth.Subject;
import java.io.IOException;
import java.net.Socket;
import java.security.Principal;
import java.security.PrivilegedExceptionAction;
import java.security.PrivilegedActionException;
import org.apache.zookeeper.Login;
import org.apache.zookeeper.server.auth.KerberosName;
import org.apache.thrift7.TException;
import org.apache.thrift7.TProcessor;
import org.apache.thrift7.server.TServer;
import org.apache.thrift7.server.TThreadPoolServer;
import org.apache.thrift7.protocol.TBinaryProtocol;
import org.apache.thrift7.protocol.TProtocol;
import org.apache.thrift7.transport.TSaslServerTransport;
import org.apache.thrift7.transport.TServerSocket;
import org.apache.thrift7.transport.TServerTransport;
import org.apache.thrift7.transport.TSocket;
import org.apache.thrift7.transport.TTransport;
import org.apache.thrift7.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import backtype.storm.security.auth.*;
import backtype.storm.utils.Utils;

public class ThriftServer {
	static {
		java.security.Security.addProvider(new AnonymousAuthenticationProvider());
	}

	private TProcessor _processor = null;
	private int _port = 0;
	private TServer _server;
	private static final Logger LOG = LoggerFactory.getLogger(ThriftServer.class);
	private String _loginConfigurationFile;

	public ThriftServer(TProcessor processor, int port) {
		try {
			_processor = processor;
			_port = port;

			_loginConfigurationFile = System.getProperty("java.security.auth.login.config");
			if ((_loginConfigurationFile==null) || (_loginConfigurationFile.length()==0)) {
				//apply Storm configuration for JAAS login 
				Map conf = Utils.readStormConfig();
				_loginConfigurationFile = (String)conf.get("java.security.auth.login.config");
			}
		} catch (Exception x) {
			x.printStackTrace();
		}
	}

	public void stop() {
		if (_server != null)
			_server.stop();
	}

	public void serve()  {
		TServerTransport serverTransport = null;

		try {
			TSaslServerTransport.Factory factory = new TSaslServerTransport.Factory();
			serverTransport = new TServerSocket(_port);

			if ((_loginConfigurationFile==null) || (_loginConfigurationFile.length()==0)) { //ANONYMOUS
				factory.addServerDefinition(AuthUtils.ANONYMOUS, AuthUtils.SERVICE, "localhost", null, null);			

				LOG.info("Starting SASL ANONYMOUS server at port:" + _port);
				_server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).
						processor(new SaslProcessor(_processor)).
						transportFactory(factory).
						minWorkerThreads(64).
						maxWorkerThreads(64).
						protocolFactory(new TBinaryProtocol.Factory()));
			} else {	
				//retrieve authentication configuration from java.security.auth.login.config
				Configuration auth_conf =  AuthUtils.getConfiguration(_loginConfigurationFile);

				//login our user
				CallbackHandler auth_callback_handler = new SaslServerCallbackHandler(auth_conf);
				Login login = new Login(AuthUtils.LoginContextServer, auth_callback_handler);
				Subject subject = login.getSubject();

				if (!subject.getPrivateCredentials(KerberosTicket.class).isEmpty()) { //KERBEROS
					String principal = AuthUtils.get(auth_conf, AuthUtils.LoginContextServer, "principal"); 
					LOG.debug("principal:"+principal);
					KerberosName serviceKerberosName = new KerberosName(principal);
					String serviceName = serviceKerberosName.getServiceName();
					String hostName = serviceKerberosName.getHostName();
					Map<String, String> props = new TreeMap<String,String>();
					props.put(Sasl.QOP, "auth");
					props.put(Sasl.SERVER_AUTH, "false");
					factory.addServerDefinition(AuthUtils.KERBEROS, serviceName, hostName, props, auth_callback_handler);
					LOG.info("Starting KERBEROS server at port:" + _port);
					//create a wrap transport factory so that we could apply user credential during connections
					TUGIAssumingTransportFactory wrapFactory = new TUGIAssumingTransportFactory(factory, subject); 
					_server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).
							processor(new SaslProcessor(_processor)).
							minWorkerThreads(64).
							maxWorkerThreads(64).
							transportFactory(wrapFactory).
							protocolFactory(new TBinaryProtocol.Factory()));
				} else { //DIGEST
					factory.addServerDefinition(AuthUtils.DIGEST, AuthUtils.SERVICE, "localhost", null, auth_callback_handler);
					LOG.info("Starting DIGEST server at port:" + _port);
					_server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).
							processor(new SaslProcessor(_processor)).
							minWorkerThreads(64).
							maxWorkerThreads(64).
							transportFactory(factory).
							protocolFactory(new TBinaryProtocol.Factory()));
				} 
			}

			_server.serve();
		} catch (Exception ex) {
			LOG.error("ThriftServer is being stopped due to: " + ex, ex);
			if (_server != null) _server.stop();
			System.exit(1); //shutdown server process since we could not handle Thrift requests any more
		}
	}

	/**                                                                                                                                                                             
	 * Processor that pulls the SaslServer object out of the transport, and                                                                                                         
	 * assumes the remote user's UGI before calling through to the original                                                                                                         
	 * processor.                                                                                                                                                                   
	 *                                                                                                                                                                              
	 * This is used on the server side to set the UGI for each specific call.                                                                                                       
	 */
	private class SaslProcessor implements TProcessor {
		final TProcessor wrapped;

		SaslProcessor(TProcessor wrapped) {
			this.wrapped = wrapped;
		}

		public boolean process(final TProtocol inProt, final TProtocol outProt) throws TException {
			TTransport trans = inProt.getTransport();
			if (!(trans instanceof TSaslServerTransport)) {
				throw new TException("Unexpected non-SASL transport " + trans.getClass());
			}
			TSaslServerTransport saslTrans = (TSaslServerTransport)trans;

			//populating request context 
			ReqContext req_context = ReqContext.context();

			//remote address
			TSocket tsocket = (TSocket)saslTrans.getUnderlyingTransport();
			Socket socket = tsocket.getSocket();
			req_context.setRemoteAddress(socket.getInetAddress());

			//remote subject 
			SaslServer saslServer = saslTrans.getSaslServer();
			String authId = saslServer.getAuthorizationID();
			LOG.debug("AUTH ID ======>" + authId);
			Subject remoteUser = new Subject();
			remoteUser.getPrincipals().add(new User(authId));
			req_context.setSubject(remoteUser);

			//invoke application logic
			return wrapped.process(inProt, outProt);
		}
	}

	static class User implements Principal {
		private final String name;

		public User(String name) {
			this.name =  name;
		}

		/**                                                                                                                                                                                
		 * Get the full name of the user.                                                                                                                                                  
		 */
		public String getName() {
			return name;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			} else if (o == null || getClass() != o.getClass()) {
				return false;
			} else {
				return (name.equals(((User) o).name));
			}
		}

		@Override
		public int hashCode() {
			return name.hashCode();
		}

		@Override
		public String toString() {
			return name;
		}
	}

	/** A TransportFactory that wraps another one, but assumes a specified UGI
	 * before calling through.                                                                                                                                                      
	 *                                                                                                                                                                              
	 * This is used on the server side to assume the server's Principal when accepting                                                                                              
	 * clients.                                                                                                                                                                     
	 */
	static class TUGIAssumingTransportFactory extends TTransportFactory {
		private final Subject subject;
		private final TTransportFactory wrapped;

		public TUGIAssumingTransportFactory(TTransportFactory wrapped, Subject subject) {
			this.wrapped = wrapped;
			this.subject = subject;

			Set<Principal> principals = (Set<Principal>)subject.getPrincipals();
			if (principals.size()>0) 
				LOG.info("Service principal:"+ ((Principal)(principals.toArray()[0])).getName());
		}

		@Override
		public TTransport getTransport(final TTransport trans) {
			try {
				return Subject.doAs(subject,
						new PrivilegedExceptionAction<TTransport>() {
					public TTransport run() {
						try {
							return wrapped.getTransport(trans);
						}
						catch (Exception e) {
							LOG.error("Storm server failed to open transport to interact with a client during session initiation: " + e, e);
							return null;
						}
					}
				});
			} catch (PrivilegedActionException e) {
				LOG.error("Storm server experienced a PrivilegedActionException exception while creating a transport using a JAAS principal context:" + e, e);
				return null;
			}
		}
	}
}
