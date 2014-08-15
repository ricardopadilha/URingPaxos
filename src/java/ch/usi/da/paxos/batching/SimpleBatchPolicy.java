package ch.usi.da.paxos.batching;
/* 
 * Copyright (c) 2014 Università della Svizzera italiana (USI)
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

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import ch.usi.da.paxos.api.BatchPolicy;
import ch.usi.da.paxos.api.PaxosRole;
import ch.usi.da.paxos.message.Message;
import ch.usi.da.paxos.message.MessageType;
import ch.usi.da.paxos.message.Value;
import ch.usi.da.paxos.ring.ProposerRole;

/**
 * Name: SimpleBatchPolicy<br>
 * Description: <br>
 * 
 * Creation date: Aug 06, 2014<br>
 * $Id$
 * 
 * @author Samuel Benz benz@geoid.ch
 */
public class SimpleBatchPolicy implements BatchPolicy {

	private final static Logger logger = Logger.getLogger(BatchPolicy.class);
	
	private final int batch_size = 32000;
	
	private final int timeout = 500; // micro seconds
	
	private ProposerRole proposer;
	
	@Override
	public void run() {
		ByteBuffer buffer = ByteBuffer.allocate(524288);
		while(true){
			try {
				buffer.clear();
				Message m = proposer.getSendQueue().take();
				if(Message.length(m) < batch_size){
					Message.toBuffer(buffer, m);
					Message bm = null;
					while((bm = proposer.getSendQueue().poll(timeout,TimeUnit.MICROSECONDS)) != null){ // do-batching if possible
						Message.toBuffer(buffer, bm);
						if(buffer.position() >= batch_size){
							break;
						}
					}
					buffer.flip();
					byte[] b = new byte[buffer.limit()];
					buffer.get(b);
					Value batch = new Value(System.nanoTime() + "" + proposer.getRingManager().getNodeID(),b,true);
					m = new Message(0,proposer.getRingManager().getNodeID(),PaxosRole.Leader,MessageType.Value,0,0,batch);
					logger.debug("Proposer sent Value batch of size " + buffer.limit());
				}
				proposer.send(m);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				break;
			}
		}
	}

	@Override
	public void setProposer(ProposerRole proposer) {
		this.proposer = proposer;
	}

}
