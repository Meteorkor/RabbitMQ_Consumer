import com.meteor.module.Receiver;


public class Tester {

	public static void main(String[] args) {
		
		
		String host=null;
		
		Receiver receiver = new Receiver(host);
		
		//receiver.basic_receiver();
		//receiver.acknowledged_receiver();
		
		//receiver.durable_acknowledged_receiver();
		//receiver.fanout_durable_acknowledged_receiver();
		//receiver.direct_durable_acknowledged_receiver();
		
		receiver.topic_durable_acknowledged_receiver();
	}

}
