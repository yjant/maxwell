package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.row.RowMap;

public class StdoutProducer extends AbstractProducer {
	public StdoutProducer(MaxwellContext context) {
		super(context);
	}

	@Override
	public void push(RowMap r) throws Exception {
		String output = r.toJSON(outputConfig);

		if ( output != null && r.shouldOutput(outputConfig) )
			System.out.println(output);

		// 在这里设置偏移量，context 中持有一个 PositionStoreThread线程，PositionStoreThread线程负责后台更新偏移量
		this.context.setPosition(r);
	}
}
