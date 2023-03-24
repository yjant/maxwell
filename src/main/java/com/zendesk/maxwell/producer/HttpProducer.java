package com.zendesk.maxwell.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.util.AesEncode;
import com.zendesk.maxwell.util.HttpUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class HttpProducer extends AbstractProducer {
	public static final Logger LOGGER = LoggerFactory.getLogger(HttpProducer.class);

	final Object lock = new Object();
	private final String encodeSeed;
	int bufferPoolSize = 2 << 10;
	volatile List<RowMap> sendBuffer = new ArrayList<>(bufferPoolSize);
	ScheduledExecutorService service = new ScheduledThreadPoolExecutor(1);
	AesEncode aesEncode;
	private String postUrl;
	private ObjectMapper mapper = new ObjectMapper();

	public HttpProducer(MaxwellContext context) {
		super(context);
		this.postUrl = context.getConfig().postUrl;
		this.encodeSeed = context.getConfig().encodeSeed;
		service.scheduleAtFixedRate(new ScheduleTask(), 0, 2000, TimeUnit.MILLISECONDS);
		aesEncode = new AesEncode(encodeSeed);
	}

	@Override
	public void push(RowMap r) throws Exception {
		doPush(r);

	}

	public void doPush(RowMap r) throws Exception {
		String output = r.toJSON(outputConfig);
		if (output != null && r.shouldOutput(outputConfig)) {
			if (sendBuffer.size() >= bufferPoolSize) {
				sendData();

			}
			synchronized (lock) {
				sendBuffer.add(r);
			}
		}
	}

	private void sendData() throws Exception {

		synchronized (lock) {
			if (sendBuffer.size() < 1) {
				return;
			}
			String batchId = UUID.randomUUID().toString();
			List<SendData> dataList = new ArrayList<>(sendBuffer.size());
			RowMap last = sendBuffer.get(sendBuffer.size() - 1);
			for (RowMap rowMap : sendBuffer) {
				String output = rowMap.toJSON(outputConfig);
				String encode = aesEncode.encode(output);
				dataList.add(new SendData(rowMap.getDatabase(), rowMap.getTable(), "ys", encode, batchId));
			}
			String encodeData = mapper.writeValueAsString(dataList);
			String response = HttpUtil.postJson(postUrl, encodeData);
			HashMap hashMap = mapper.readValue(response, HashMap.class);
			String responseCode = hashMap.get("code").toString();
			if (!Objects.equals(responseCode, "0")) {
				LOGGER.error("Http report data error!!! response is {}", response);
				System.exit(0);
			}
			sendBuffer = new ArrayList<>(bufferPoolSize);
			// 在这里设置偏移量，context 中持有一个 PositionStoreThread线程，PositionStoreThread线程负责后台更新偏移量
			setPosition(last);
		}

	}


	private void setPosition(RowMap r) {
		this.context.setPosition(r);
	}

	class ScheduleTask implements Runnable {

		@Override
		public void run() {
			try {
				sendData();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	class SendData {
		private String databaseName;
		private String tableName;
		private String locode;
		private String data;
		private String batchId;

		public SendData() {
		}

		public SendData(String databaseName, String tableName, String locode, String data, String batchId) {
			this.databaseName = databaseName;
			this.tableName = tableName;
			this.locode = locode;
			this.data = data;
			this.batchId = batchId;
		}

		public String getTableName() {
			return tableName;
		}

		public void setTableName(String tableName) {
			this.tableName = tableName;
		}

		public String getLocode() {
			return locode;
		}

		public void setLocode(String locode) {
			this.locode = locode;
		}

		public String getDatabaseName() {
			return databaseName;
		}

		public void setDatabaseName(String databaseName) {
			this.databaseName = databaseName;
		}

		public String getData() {
			return data;
		}

		public void setData(String data) {
			this.data = data;
		}

		public String getBatchId() {
			return batchId;
		}

		public void setBatchId(String batchId) {
			this.batchId = batchId;
		}
	}
}
