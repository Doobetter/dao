package learn;



import org.apache.storm.generated.DRPCExecutionException;
import org.apache.storm.thrift.TException;
import org.apache.storm.utils.DRPCClient;

public class DRPCClientTest {

	public static void main(String []args) throws TException, DRPCExecutionException{
		DRPCClient client = new DRPCClient(null, "10.100.211.232", 3772);
		String result = client.execute("exclamation","test");
	}
}
