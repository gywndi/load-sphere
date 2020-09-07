package net.gywn.algorithm;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

public class IDGeneratorHandlerImpl implements IDGeneratorHandler {
	private static final int TIMESTAMP_BIT_LENGTH = 41;
	private static final int NODE_ID_BIT_LENGTH = 12;
	private static final int SEQUENCE_BIT_LENGTH = 11;
	private static final int SUFFIX_BIT_LENGTH = 64 - TIMESTAMP_BIT_LENGTH;
	private static final long BASE_TIMESTAMP = new Date(1451574000000L).getTime();

	private static final long MAX_FILL_BITS = 0xffffffffffffffffL;
	private static final long TIMESTAMP_BITMAP = MAX_FILL_BITS << (NODE_ID_BIT_LENGTH + SEQUENCE_BIT_LENGTH);
	private static final long SEQUENCE_BITMAP = ~(MAX_FILL_BITS << SEQUENCE_BIT_LENGTH);
	private static final long NODE_ID_BITMAP = ~(MAX_FILL_BITS << (NODE_ID_BIT_LENGTH + SEQUENCE_BIT_LENGTH)
			| SEQUENCE_BITMAP);

	private int _nodeId = 0;
	private AtomicLong _sequence = new AtomicLong();

	public String generate(final String[] params) {
		String[] time = params[0].split("\\.");
		String frac = "000";
		try {
			time[1] += "000";
			frac = frac.substring(0, 3);
		} catch (Exception e) {
		}

		time[0] += "." + frac;
		SimpleDateFormat transFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		try {
			Date date = transFormat.parse(time[0]);
			return generate(date.getTime()).toString();
		} catch (ParseException e) {
			System.out.println(e);
		}

		return null;
	}

	private Long generate(long milis) {
		long seq = _sequence.incrementAndGet();
		long timestamp = milis - BASE_TIMESTAMP;
		long id = ((timestamp << SUFFIX_BIT_LENGTH) & TIMESTAMP_BITMAP) | _nodeId | (seq & SEQUENCE_BITMAP);
		return id;
	}
}
