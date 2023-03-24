package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.MaxwellConfig;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.monitoring.NoOpMetrics;
import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.row.RowMap;
import org.junit.Test;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HttpTest {
	@Test
	public void testCount() throws Exception {
		MaxwellContext context = mock(MaxwellContext.class);
		MaxwellConfig config = new MaxwellConfig();
		config.postUrl="http://localhost:8088/data/receive";
		config.encodeSeed="encodes";
		when(context.getConfig()).thenReturn(config);
		when(context.getMetrics()).thenReturn(new NoOpMetrics());
		HttpProducer producer = new HttpProducer(context);
		long startTime = System.currentTimeMillis();
		Random random = new Random();
//		for (int j = 0; j < 3; j++) {
			for (int i = 0; i < 1000000; i++) {
				producer.doPush((new RowMap("insert", "foo", "bar", random.nextLong(), new ArrayList<String>(), new Position(new BinlogPosition(3, "mysql.1"), 0L))));
//			}
		}

		long endTime = System.currentTimeMillis();
		System.out.println(endTime- startTime);
		Thread.sleep(10000);



	}
}
