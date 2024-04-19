package telran.aws;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;

public class KinesisHandler implements RequestHandler<KinesisEvent, String> {

	@Override
	public String handleRequest(KinesisEvent input, Context context) {
		var logger = context.getLogger();
		input.getRecords().stream().map(r -> new String(r.getKinesis().getData().array()))
		.forEach(s ->logger.log(s));
		return null;
	}

}
