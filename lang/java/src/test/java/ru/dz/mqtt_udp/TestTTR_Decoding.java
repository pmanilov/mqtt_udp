package ru.dz.mqtt_udp;

import static org.junit.Assert.*;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import ru.dz.mqtt_udp.proto.TTR_PacketNumber;
import ru.dz.mqtt_udp.proto.TaggedTailRecord;

/**
 * Tests for TTR variable-length decoding and large packet round-trip.
 */
public class TestTTR_Decoding {

    /**
     * Test that fromBytes correctly decodes a TTR with single-byte length (<128).
     */
    @Test
    public void testSingleByteLengthDecode() throws MqttProtocolException {
        // TTR 'n' (packet number) with length 4, value 0x00000042
        byte[] raw = { (byte) 'n', 4, 0, 0, 0, 0x42 };
        TaggedTailRecord ttr = TaggedTailRecord.fromBytes(raw);
        assertTrue("Should decode as TTR_PacketNumber", ttr instanceof TTR_PacketNumber);
        assertEquals(6, ttr.getRawLength()); // 1 tag + 1 length + 4 data
        TTR_PacketNumber pn = (TTR_PacketNumber) ttr;
        assertEquals(0x42, pn.getValue());
    }

    /**
     * Test that fromBytes correctly decodes a TTR with multi-byte variable length
     * (>=128).
     * This is the regression test for the removed `dlen <<= 7` bug.
     * 
     * Length 200 encodes as: first byte = (200 % 128) | 0x80 = 72 | 128 = 0xC8,
     * second byte = 200 / 128 = 1 = 0x01
     */
    @Test
    public void testMultiByteLengthDecode() throws MqttProtocolException {
        int dataLen = 200;
        // Build a TTR with tag 'n', variable-length encoded 200, and 200 bytes of data
        byte[] raw = new byte[1 + 2 + dataLen]; // 1 tag + 2 length bytes + 200 data bytes
        raw[0] = (byte) 'n'; // tag
        raw[1] = (byte) ((dataLen % 128) | 0x80); // first length byte with continuation bit
        raw[2] = (byte) (dataLen / 128); // second length byte
        // Fill data with zeros (default)

        TaggedTailRecord ttr = TaggedTailRecord.fromBytes(raw);
        assertEquals("Raw length should be 1 tag + 2 length + 200 data = 203",
                203, ttr.getRawLength());
    }

    /**
     * Test that fromBytesAll correctly decodes multiple TTRs from a combined byte
     * array.
     */
    @Test
    public void testFromBytesAll() throws MqttProtocolException {
        // Two packet number TTRs back to back
        byte[] ttr1 = { (byte) 'n', 4, 0, 0, 0, 0x01 };
        byte[] ttr2 = { (byte) 'n', 4, 0, 0, 0, 0x02 };
        byte[] combined = new byte[ttr1.length + ttr2.length];
        System.arraycopy(ttr1, 0, combined, 0, ttr1.length);
        System.arraycopy(ttr2, 0, combined, ttr1.length, ttr2.length);

        AtomicReference<Integer> sigPos = new AtomicReference<>(-1);
        Collection<TaggedTailRecord> ttrs = TaggedTailRecord.fromBytesAll(combined, sigPos);
        assertEquals("Should decode 2 TTRs", 2, ttrs.size());
    }

    /**
     * Test that fromBytes throws MqttProtocolException on truncated data.
     */
    @Test(expected = MqttProtocolException.class)
    public void testTruncatedTTRThrows() throws MqttProtocolException {
        // Tag says 4 bytes of data, but only 2 bytes provided after length
        byte[] raw = { (byte) 'n', 4, 0, 0 };
        TaggedTailRecord.fromBytes(raw);
    }

    /**
     * Test that fromBytes throws on a 1-byte input (too short).
     */
    @Test(expected = MqttProtocolException.class)
    public void testTooShortTTRThrows() throws MqttProtocolException {
        byte[] raw = { (byte) 'n' };
        TaggedTailRecord.fromBytes(raw);
    }

    /**
     * Test that fromBytesAll gracefully handles corrupt TTR data without throwing.
     */
    @Test
    public void testFromBytesAllHandlesCorruptData() throws MqttProtocolException {
        // Valid TTR followed by truncated one
        byte[] valid = { (byte) 'n', 4, 0, 0, 0, 0x01 };
        byte[] corrupt = { (byte) 'n', 4, 0 }; // truncated
        byte[] combined = new byte[valid.length + corrupt.length];
        System.arraycopy(valid, 0, combined, 0, valid.length);
        System.arraycopy(corrupt, 0, combined, valid.length, corrupt.length);

        AtomicReference<Integer> sigPos = new AtomicReference<>(-1);
        // Should not throw — corrupt TTR is logged and skipped
        Collection<TaggedTailRecord> ttrs = TaggedTailRecord.fromBytesAll(combined, sigPos);
        assertEquals("Should decode only the valid TTR", 1, ttrs.size());
    }

    /**
     * Test round-trip: create a large PublishPacket, serialize, then deserialize.
     * This tests the full pipeline including MQTT variable-length remaining-length
     * encoding.
     */
    @Test
    public void testLargePacketRoundTrip() throws MqttProtocolException {
        String topic = "test/large";
        // Create a value large enough to push remaining length past 127 (multi-byte)
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 200; i++) {
            sb.append("ABCDEFGHIJ"); // 2000 chars total
        }
        String value = sb.toString();

        PublishPacket original = new PublishPacket(topic, value);
        byte[] serialized = original.toBytes();

        // Serialized packet should be significantly larger than 127 bytes
        assertTrue("Serialized packet should be > 127 bytes", serialized.length > 127);

        // Deserialize
        IPacket decoded = IPacket.fromBytes(serialized, null);
        assertTrue("Should decode as PublishPacket", decoded instanceof PublishPacket);

        PublishPacket decodedPub = (PublishPacket) decoded;
        assertEquals(topic, decodedPub.getTopic());
        assertEquals(value, decodedPub.getValueString());
    }
}
