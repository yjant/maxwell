package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.util.AesEncode;
import com.zendesk.maxwell.util.HttpUtil;

public class HttpProducer extends AbstractProducer{
	private String postUrl;
	private String encodeSeed;
	public HttpProducer(MaxwellContext context) {
		super(context);
		this.postUrl = context.getConfig().postUrl;
		this.encodeSeed = context.getConfig().encodeSeed;
	}

	@Override
	public void push(RowMap r) throws Exception {
		String output = r.toJSON(outputConfig);

		if ( output != null && r.shouldOutput(outputConfig) ) {
			AesEncode aesEncode = new AesEncode(encodeSeed);
			String encodeData = aesEncode.encode(output);
//			HttpUtil.postJson(postUrl,encodeData);
			System.out.println(output);
			System.out.println(encodeData);
		}


		// 在这里设置偏移量，context 中持有一个 PositionStoreThread线程，PositionStoreThread线程负责后台更新偏移量
		this.context.setPosition(r);

	}
}
