import java.io.IOException;

import org.apache.zookeeper.KeeperException;

import com.jlen.utils.coordination.LeaderElectionAccess;
import com.jlen.utils.coordination.ZookeeperConfig;
import com.jlen.utils.coordination.ZookeeperConnection;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class Test {
	
	public static void main(String[] args) {
		
		Config config = ConfigFactory.defaultApplication();
		
		try {
			ZookeeperConnection conn = ZookeeperConnection.getConnection(new ZookeeperConfig(config.getConfig("jlen.cfg.util.coordiation")));
			LeaderElectionAccess access = conn.getZkpLeaderElectionAccess("my_test_service");
			System.out.println(access.isLeader());
			access.subscribe(System.out::println);
			while(true);
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
	}

}
