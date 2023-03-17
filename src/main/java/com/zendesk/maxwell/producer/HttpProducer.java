package com.zendesk.maxwell.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.util.AesEncode;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class HttpProducer extends AbstractProducer {
	final Object lock = new Object();
	private final String encodeSeed;
	int bufferPoolSize = 2 << 15;
	ScheduledExecutorService service = new ScheduledThreadPoolExecutor(2);
	volatile List<SendData> sendBuffer = new ArrayList<>(bufferPoolSize);
	AtomicInteger atomicInteger = new AtomicInteger(0);
	AtomicInteger total = new AtomicInteger(0);
	AesEncode aesEncode;
	RowMap r;
	private String postUrl;
	private ObjectMapper mapper = new ObjectMapper();

	public HttpProducer(MaxwellContext context) {
		super(context);
		this.postUrl = context.getConfig().postUrl;
		this.encodeSeed = context.getConfig().encodeSeed;
		service.scheduleAtFixedRate(new ScheduleTask(), 0, 1000, TimeUnit.MILLISECONDS);
		aesEncode = new AesEncode("encodeSeed");
	}

	@Override
	public void push(RowMap r) throws Exception {
		doPush(r);

	}

	public void doPush(RowMap r) throws Exception {
		String output = r.toJSON(outputConfig);
		if (output != null && r.shouldOutput(outputConfig)) {
			String database = r.getDatabase();
			String encodeData = aesEncode.encode(output);
			SendData sendData = new SendData(database, encodeData);
			this.r = r;
			synchronized (lock) {
				if (sendBuffer.size() >= bufferPoolSize) {
					sendData();
					// 在这里设置偏移量，context 中持有一个 PositionStoreThread线程，PositionStoreThread线程负责后台更新偏移量
					setPosition(r);
				} else {

					sendBuffer.add(sendData);
				}
			}
		}
	}

	private void sendData() throws Exception {
		synchronized (lock) {
			int i = atomicInteger.addAndGet(1);
			System.out.println("loop " + i);
			System.out.println("size " + sendBuffer.size());
			System.out.println("total " + total.addAndGet(sendBuffer.size()));
			sendBuffer = new ArrayList<>(bufferPoolSize);
		}

//				HttpUtil.postJson(postUrl,encodeData);
	}

	private void setPosition(RowMap r) {
		this.context.setPosition(r);
	}

	class ScheduleTask implements Runnable {

		@Override
		public void run() {
			try {
				sendData();
				setPosition(r);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	class SendData {
		private String databaseName;
		private String data;

		public SendData(String databaseName, String data) {
			this.databaseName = databaseName;
			this.data = data;
		}
	}
}
