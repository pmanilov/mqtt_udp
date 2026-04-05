package ru.dz.mqtt_udp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

import ru.dz.mqtt_udp.io.IPacketAddress;
import ru.dz.mqtt_udp.io.SingleSendSocket;
import ru.dz.mqtt_udp.util.ErrorType;
import ru.dz.mqtt_udp.util.GenericPacket;
import ru.dz.mqtt_udp.util.GlobalErrorHandler;
import ru.dz.mqtt_udp.util.Throttle;
import ru.dz.mqtt_udp.util.mqtt_udp_defs;

public class Engine {

	private static boolean signatureRequired = false;
	private static String signatureKey = "signPassword";


	public static boolean isSignatureRequired() {		return signatureRequired;	}
	public static void setSignatureRequired(boolean req) {		signatureRequired = req;	}


	public static String getSignatureKey() {		return signatureKey;	}
	public static void setSignatureKey(String key) {		signatureKey = key;	}


	private static Throttle t = new Throttle();
	private static int maxReplyQoS = 1; // By default - TODO - add to defs!
	//private static int maxReplyQoS = 3; // for test


	/**
	 * Set packet send rate.
	 * @param msec average time in milliseconds between packets. Set to 0 to turn throttling off.
	 */
	public static void setThrottle(int msec) {		t.setThrottle(msec);	}

	/**
	 * Must be called in packet send code.
	 * Will put caller asleep to make sure packets are sent in a right pace.
	 */
	public static void throttle() {				t.throttle();			}

	public static String getVersionString() {
		return String.format("%d.%d", mqtt_udp_defs.PACKAGE_VERSION_MAJOR, mqtt_udp_defs.PACKAGE_VERSION_MINOR);
	}
	public static int getMaxReplyQoS() { return maxReplyQoS; }
	public static void setMaxReplyQoS(int newMax) { maxReplyQoS = newMax; }


	// ------------------------------------------------------------
	// QoS: outgoing packet resend queue
	// ------------------------------------------------------------

	/**
	 * Maximum number of times an outgoing QoS packet will be (re)sent
	 * before being dropped from the resend queue.
	 */
	private static final int RESEND_COUNT = 3;

	/**
	 * How many PUBACKs with QoS one less than the original we must see
	 * before considering a QoS 2 packet delivered.
	 */
	private static final int MIN_ACK = 3;

	/**
	 * Delay between resend sweeps, milliseconds.
	 */
	private static final int RESEND_INTERVAL_MSEC = 300;

	/** Outgoing QoS packets awaiting acknowledgement, keyed by packet number. */
	private static final Map<Integer, GenericPacket> outgoing = new ConcurrentHashMap<>();

	/** Background thread that sweeps {@link #outgoing} and resends packets. */
	private static volatile Thread resendThread = null;

	// ------------------------------------------------------------
	// QoS: counters for experimental analysis
	// ------------------------------------------------------------

	private static final AtomicLong resendCount = new AtomicLong();
	private static final AtomicLong ackReceived = new AtomicLong();
	private static final AtomicLong droppedAfterMaxRetries = new AtomicLong();
	/** Histogram of final attempt counts for delivered packets (index 0 = 1 attempt, ..., index RESEND_COUNT = RESEND_COUNT+1 attempts). */
	private static final AtomicLongArray attemptsHistogram = new AtomicLongArray(RESEND_COUNT + 1);

	public static long getResendCount() { return resendCount.get(); }
	public static long getAckReceivedCount() { return ackReceived.get(); }
	public static long getDroppedAfterMaxRetriesCount() { return droppedAfterMaxRetries.get(); }
	public static long getAttemptsHistogram(int attempts) {
		int idx = Math.max(1, Math.min(attempts, RESEND_COUNT + 1)) - 1;
		return attemptsHistogram.get(idx);
	}
	public static int getAttemptsHistogramSize() { return RESEND_COUNT + 1; }
	public static void resetQoSCounters() {
		resendCount.set(0);
		ackReceived.set(0);
		droppedAfterMaxRetries.set(0);
		for (int i = 0; i < attemptsHistogram.length(); i++)
			attemptsHistogram.set(i, 0);
	}

	// ------------------------------------------------------------
	// Server-side deduplication of incoming PUBLISH packets.
	//
	// When a PUBACK is lost the sender will retransmit the PUBLISH, so the
	// receiver can see the same (source, packet_id) more than once. Dedup
	// keeps a bounded, time-limited set of recently seen ids and tells the
	// caller whether a given packet has already been delivered.
	// ------------------------------------------------------------

	private static final long DEDUP_TTL_NANOS = 30L * 1000_000_000L;
	private static final int DEDUP_MAX_ENTRIES = 200_000;
	private static final Map<String, Long> dedupSeen = new ConcurrentHashMap<>();

	/**
	 * Check whether this PUBLISH has already been seen recently. Updates the
	 * dedup table as a side effect. Packets with QoS 0 or without a packet
	 * number are never considered duplicates.
	 *
	 * @param p Incoming PUBLISH packet.
	 * @return true if the packet is a retransmission of one already delivered.
	 */
	public static boolean isDuplicatePublish(PublishPacket p) {
		if (p.getQoS() == 0 || !p.getPacketNumber().isPresent())
			return false;

		IPacketAddress from = p.getFrom();
		String key = (from == null ? "?" : from.toString()) + "|" + p.getPacketNumber().get();
		long now = System.nanoTime();

		if (dedupSeen.size() > DEDUP_MAX_ENTRIES)
			pruneDedup(now);

		Long prev = dedupSeen.putIfAbsent(key, now);
		if (prev == null)
			return false;
		if (now - prev > DEDUP_TTL_NANOS) {
			dedupSeen.put(key, now);
			return false;
		}
		return true;
	}

	private static void pruneDedup(long now) {
		dedupSeen.entrySet().removeIf(e -> now - e.getValue() > DEDUP_TTL_NANOS);
	}

	public static void resetDedup() {
		dedupSeen.clear();
	}

	/**
	 * Queue a packet that has just been sent with non-zero QoS so that it
	 * will be retransmitted periodically until either a matching PUBACK is
	 * received or the retry limit is reached.
	 *
	 * @param packet Packet just sent.
	 */
	public static void queueForResend(GenericPacket packet) {
		if (packet.getQoS() == 0)
			return;

		if (!packet.getPacketNumber().isPresent()) {
			GlobalErrorHandler.handleError(ErrorType.Protocol,
					"queueForResend: packet without id, QoS=" + packet.getQoS());
			return;
		}

		outgoing.put(packet.getPacketNumber().get(), packet);
		startResendThread();
	}

	/**
	 * Process a PUBACK that arrived from the network. Matches it against
	 * outgoing queued packets by packet number and removes the packet
	 * (QoS 1 style) or counts acks until {@link #MIN_ACK} is reached
	 * (QoS 2 style).
	 *
	 * @param ack Incoming PUBACK packet.
	 */
	public static void handleIncomingAck(PubAckPacket ack) {
		if (!ack.getReplyToPacketNumber().isPresent())
			return;

		int replyTo = ack.getReplyToPacketNumber().get();
		ackReceived.incrementAndGet();

		GenericPacket found = outgoing.get(replyTo);
		if (found == null)
			return;

		int ackQoS = ack.getQoS();
		int sentQoS = found.getQoS();

		if (ackQoS >= sentQoS) {
			if (outgoing.remove(replyTo) != null)
				recordDelivered(found);
			return;
		}

		if (ackQoS == sentQoS - 1) {
			found.incrementAckCount();
			if (found.getAckCount() >= MIN_ACK) {
				if (outgoing.remove(replyTo) != null)
					recordDelivered(found);
			}
		}
	}

	private static void recordDelivered(GenericPacket p) {
		int attempts = Math.max(1, p.getSentCounter());
		int idx = Math.min(attempts, attemptsHistogram.length()) - 1;
		attemptsHistogram.incrementAndGet(idx);
	}

	private static synchronized void startResendThread() {
		if (resendThread != null && resendThread.isAlive())
			return;

		Thread t = new Thread(Engine::resendLoop, "MQTT UDP Resend");
		t.setDaemon(true);
		resendThread = t;
		t.start();
	}

	private static void resendLoop() {
		while (true) {
			try {
				Thread.sleep(RESEND_INTERVAL_MSEC);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				synchronized (Engine.class) {
					resendThread = null;
				}
				return;
			}

			// Atomic with startResendThread(): if we decide to exit here
			// and a producer enqueues right after, startResendThread will
			// see resendThread==null and spawn a fresh worker.
			synchronized (Engine.class) {
				if (outgoing.isEmpty()) {
					resendThread = null;
					return;
				}
			}

			// Snapshot + drop exhausted entries without blocking senders.
			List<GenericPacket> snapshot = new ArrayList<>(outgoing.size());
			for (Map.Entry<Integer, GenericPacket> e : outgoing.entrySet()) {
				GenericPacket p = e.getValue();
				if (p.getSentCounter() > RESEND_COUNT) {
					if (outgoing.remove(e.getKey(), p))
						droppedAfterMaxRetries.incrementAndGet();
					continue;
				}
				snapshot.add(p);
			}

			for (GenericPacket p : snapshot) {
				if (p.getSentCounter() > RESEND_COUNT)
					continue;
				try {
					p.resend(SingleSendSocket.get());
					resendCount.incrementAndGet();
				} catch (IOException e) {
					GlobalErrorHandler.handleError(ErrorType.IO, e);
				}
			}
		}
	}

}
