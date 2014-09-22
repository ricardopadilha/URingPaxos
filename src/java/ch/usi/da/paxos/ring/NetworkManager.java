package ch.usi.da.paxos.ring;
/* 
 * Copyright (c) 2013 Università della Svizzera italiana (USI)
 * 
 * This file is part of URingPaxos.
 *
 * URingPaxos is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * URingPaxos is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with URingPaxos.  If not, see <http://www.gnu.org/licenses/>.
 */

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;

import net.dsys.commons.api.lang.Factory;
import net.dsys.commons.impl.lang.ByteBufferFactory;
import net.dsys.snio.api.buffer.MessageBufferConsumer;
import net.dsys.snio.api.buffer.MessageBufferProducer;
import net.dsys.snio.api.channel.MessageChannel;
import net.dsys.snio.api.channel.MessageServerChannel;
import net.dsys.snio.api.codec.MessageCodec;
import net.dsys.snio.api.handler.MessageConsumer;
import net.dsys.snio.api.handler.MessageHandler;
import net.dsys.snio.api.pool.SelectorPool;
import net.dsys.snio.impl.buffer.RingBufferProvider;
import net.dsys.snio.impl.channel.MessageChannels;
import net.dsys.snio.impl.channel.MessageServerChannels;
import net.dsys.snio.impl.codec.Codecs;
import net.dsys.snio.impl.handler.MessageHandlers;
import net.dsys.snio.impl.pool.SelectorPools;

import org.apache.log4j.Logger;

import ch.usi.da.paxos.api.ConfigKey;
import ch.usi.da.paxos.api.PaxosRole;
import ch.usi.da.paxos.message.Message;
import ch.usi.da.paxos.message.MessageType;

/**
 * Name: NetworkManager<br>
 * Description: <br>
 * 
 * Creation date: Aug 14, 2012<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class NetworkManager {

	public final static int MAGIC_NUMBER = 0x756d7270; // ASCII for "umrp"
	
	private final static Logger logger = Logger.getLogger(NetworkManager.class);

	private final static Logger stats = Logger.getLogger("ch.usi.da.paxos.Stats");

	private final RingManager ring;

	private SelectorPool pool;

	private MessageServerChannel<ByteBuffer> server;
	
	private MessageChannel<ByteBuffer> client;
	
	private MessageBufferProducer<ByteBuffer> send_queue;
	
	private Role acceptor = null;

	private Role leader = null;

	private Role learner = null;

	private Role proposer = null;

	public boolean crc_32 = false;
	
	public int buf_size = 131071;

	public long recv_count = 0;

	public long recv_bytes = 0;

	public long send_count = 0;

	public long send_bytes = 0;

	public final long[] messages_distribution = new long[MessageType.values().length];

	public final long[] messages_size = new long[MessageType.values().length];

	//private final Random random = new Random();
	
	/**
	 * @param ring the ring manager
	 * @throws IOException
	 */
	public NetworkManager(RingManager ring) throws IOException {
		this.ring = ring;
		// use as many threads as possible
		this.pool = SelectorPools.open("NetworkManager-pool");

		if(stats.isDebugEnabled()){
			for(MessageType t : MessageType.values()){
				messages_distribution[t.getId()] = 0;
				messages_size[t.getId()] = 0;
			}
		}
	}
	
	/**
	 * Start the TCP listener
	 * 
	 * @throws IOException
	 */
	public void startServer() throws IOException {

		// see the maximum Adler32 checksum message length -- should be a config option
		final int messageLength = 65521;
		// fastest checksum algorithm
		final Factory<MessageCodec> codecs = Codecs.getAdler32Factory(messageLength);

		// message queue shared by snio and URingPaxos:
		// heap byte buffers
		final Factory<ByteBuffer> factory = new ByteBufferFactory(messageLength);
		// uses disruptor library
		final MessageBufferConsumer<ByteBuffer> input = RingBufferProvider.createConsumer(256, factory);

		server = MessageServerChannels.newTCPServerChannel()
				.setPool(pool)
				.setMessageCodec(codecs)
				.useRingBuffer() // uses disruptor library
				.useSingleInputBuffer(input)
				.open();

		// consumer of incoming messages
		final MessageConsumer<ByteBuffer> consumer = new MessageConsumer<ByteBuffer>() {
			@Override
			public void consume(final ByteBuffer buffer, final Object attachment) {
				try {
					final Message msg = Message.fromBuffer(buffer);
					receive(msg);
				} catch (final Exception e) {
					// I've looked inside Message.fromBuffer()
					// and I cannot see how an Exception could come from there...
					e.printStackTrace();
				}
			}
		};

		// single threaded consumer
		final MessageHandler<ByteBuffer> handler = MessageHandlers.buildHandler()
				.setName("NetworkManager-consumer")
				.useSingleConsumer(consumer)
				.build();

		// ties the consumer and channel together
		server.onAccept(handler.getAcceptListener());

		// binds the channel
		server.bind(ring.getNodeAddress());
		try {
			server.getBindFuture().get(); // wait for bind to complete
		} catch (final InterruptedException | ExecutionException e) {
			throw new IOException(e);
		}
		logger.debug("NetworkManager listener started " + server.getLocalAddress());
		
		Thread t2 = new Thread(new NetworkStatsWriter(ring));
		t2.setName("NetworkStatsWriter");
		t2.start();
	}
	
	/**
	 * Called from the server listener when a packet arrives
	 * 
	 * @param m the received message
	 */
	public synchronized void receive(Message m){
		/*if(logger.isDebugEnabled()){
			logger.debug("receive network message (ring:" + ring.getRingID() + ") : " + m);
		}*/

		/*if(random.nextInt(100) == 1){
			logger.debug("!! drop: " + m);
			return;
		}*/
		
		if(stats.isDebugEnabled()){
			messages_distribution[m.getType().getId()]++;
			messages_size[m.getType().getId()] = messages_size[m.getType().getId()] + Message.length(m);
		}
		
		// network forwarding
		if(m.getType() == MessageType.Relearn || m.getType() == MessageType.Latency){
			if(leader == null){
				send(m);
			}
		}else if(m.getType() == MessageType.Value){
			if(ring.getRingSuccessor(ring.getNodeID()) != m.getSender()){ // D,v -> until predecessor(P0)
				send(m);
			}
		}else if(m.getType() == MessageType.Phase2){
			if(acceptor == null && ring.getNodeID() != ring.getLastAcceptor()){ // network -> until last_accept
				send(m);
			}	
		}else if(m.getType() == MessageType.Decision){
			// network -> predecessor(deciding acceptor)
			if(ring.getRingSuccessor(ring.getNodeID()) != m.getSender()){
				send(m);
			}
		}else if(m.getType() == MessageType.Phase1 || m.getType() == MessageType.Phase1Range){
			if(m.getReceiver() == PaxosRole.Leader){
				if(leader == null){
					send(m);
				}
			}else if(m.getReceiver() == PaxosRole.Acceptor){
				if(acceptor == null){
					send(m);
				}
			}
		}else if(m.getType() == MessageType.Safe){
			if(learner == null && ring.getNodeID() != ring.getCoordinatorID()){ // network -> until coordinator
				send(m);
			}	
		}else if(m.getType() == MessageType.Trim){
			if(acceptor == null && ring.getNodeID() != ring.getCoordinatorID()){ // network -> until coordinator
				send(m);
			}				
		}

		// local delivery
		if(m.getType() == MessageType.Relearn || m.getType() == MessageType.Latency){
			if(leader != null){
				leader.deliver(ring,m);
			}
		}else if(m.getType() == MessageType.Value){
			if(learner != null){
				learner.deliver(ring,m);
			}
			if(acceptor != null){
				acceptor.deliver(ring,m);
			}
			if(leader != null){
				leader.deliver(ring,m);
			}
		}else if(m.getType() == MessageType.Phase2){
			if(learner != null){
				learner.deliver(ring,m);
			}			
			if(acceptor != null){
				acceptor.deliver(ring,m);
			}	
		}else if(m.getType() == MessageType.Decision){
			if(leader != null){
				leader.deliver(ring,m);
			}
			if(acceptor != null){
				acceptor.deliver(ring,m);
			}
			if(learner != null){
				learner.deliver(ring,m);
			}			
			if(proposer != null){
				proposer.deliver(ring,m);
			}
		}else if(m.getType() == MessageType.Phase1 || m.getType() == MessageType.Phase1Range){
			if(m.getReceiver() == PaxosRole.Leader){
				if(leader != null){
					leader.deliver(ring,m);
				}
			}else if(m.getReceiver() == PaxosRole.Acceptor){
				if(acceptor != null){
					acceptor.deliver(ring,m);
				}
			}
		}else if(m.getType() == MessageType.Safe){
			if(leader != null){
				leader.deliver(ring,m);
			}else if(learner != null){
				learner.deliver(ring,m);
			}
		}else if(m.getType() == MessageType.Trim){
			if(learner != null){
				learner.deliver(ring,m);
			}
			if(leader != null){
				leader.deliver(ring,m);
			}else if(acceptor != null){
				acceptor.deliver(ring,m);
			}
		}
	}
	
	/**
	 * close the server listener
	 */
	public void closeServer(){
		try {
			server.close();
			server.getCloseFuture().get(); // wait for close to finish
			pool.close();
			pool.getCloseFuture().get(); // wait for close to finish
		} catch (IOException | InterruptedException | ExecutionException e) {
			logger.error("NetworkManager server close error",e);
		}
	}
		
	/**
	 * connect to the ring successor
	 * 
	 * @param addr
	 */
	public void connectClient(InetSocketAddress addr) {
		try {
			Thread.sleep(1000); // give node time to start (zookeeper is fast!)
		} catch (final InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new RuntimeException(e);
		}

		// see the maximum Adler32 checksum message length -- should be a config option
		final int messageLength = 65521;
		// fastest checksum algorithm
		final MessageCodec codec = Codecs.getAdler32Checksum(messageLength);

		try {
			client = MessageChannels.newTCPChannel()
					.setPool(pool)
					.setMessageCodec(codec)
					.useRingBuffer() // uses the disruptor library
					.open();
			client.connect(addr);
			client.getConnectFuture().get(); // wait for connection to complete
			send_queue = client.getOutputBuffer();
			logger.debug("NetworkManager create connection " + addr + " (" + client.getLocalAddress() + ")");
		} catch (final IOException | InterruptedException | ExecutionException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * disconnect client (ring successor)
	 */
	public void disconnectClient(){
		try {
			if(client != null){
				client.close();
				client.getCloseFuture().get(); // wait for close to finish
				logger.debug("NetworkManager close connection");
			}
		} catch (IOException | InterruptedException | ExecutionException e) {
			logger.error("NetworkManager client close error",e);
		}
	}	
	
	/**
	 * @param m the message to send
	 */
	public void send(Message m) {
		try {
			final long sequence = send_queue.acquire(); // (blocking call)
			try {
				final ByteBuffer buffer = send_queue.get(sequence);
				Message.toBuffer(buffer, m);
				buffer.flip();
			} finally {
				send_queue.release(sequence);
			}
		} catch (final InterruptedException e) {
			logger.error("NetworkManager client send error",e);
		}
	}
	
	/**
	 * @return the acceptor
	 */
	public Role getAcceptor() {
		return acceptor;
	}

	/**
	 * @param acceptor the acceptor to set
	 */
	public void setAcceptor(Role acceptor) {
		this.acceptor = acceptor;
	}

	/**
	 * @return the leader
	 */
	public Role getLeader() {
		return leader;
	}

	/**
	 * @param leader the leader to set
	 */
	public void setLeader(Role leader) {
		this.leader = leader;
	}

	/**
	 * @return the learner
	 */
	public Role getLearner() {
		return learner;
	}

	/**
	 * @param learner the learner to set
	 */
	public void setLearner(Role learner) {
		this.learner = learner;
	}

	/**
	 * @return the proposer
	 */
	public Role getProposer() {
		return proposer;
	}

	/**
	 * @param proposer the proposer to set
	 */
	public void setProposer(Role proposer) {
		this.proposer = proposer;
	}

	/**
	 * @param role
	 */
	public synchronized void registerCallback(Role role){
		if(role instanceof AcceptorRole){
			this.acceptor = role;			
		}else if(role instanceof CoordinatorRole){
			this.leader = role;
		}else if(role instanceof LearnerRole){
			this.learner = role;
		}else if(role instanceof ProposerRole){
			this.proposer = role;
		}
	}
	
	/**
	 * @param value
	 * @return a byte[]
	 */
	public static synchronized final byte[] intToByte(int value) {
	    return new byte[] {
	            (byte)(value >>> 24),
	            (byte)(value >>> 16),
	            (byte)(value >>> 8),
	            (byte)value};
	}
	
	/**
	 * @param b
	 * @return the int
	 */
	public static synchronized final int byteToInt(byte [] b) { 
		return (b[0] << 24) + ((b[1] & 0xFF) << 16) + ((b[2] & 0xFF) << 8) + (b[3] & 0xFF); 
	}

}
