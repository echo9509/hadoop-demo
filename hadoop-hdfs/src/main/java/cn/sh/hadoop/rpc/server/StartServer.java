package cn.sh.hadoop.rpc.server;

import cn.sh.hadoop.rpc.LoginService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

/**
 * @author sh
 */
public class StartServer {

    public static void main(String[] args) throws IOException {
        RPC.Builder builder = new RPC.Builder(new Configuration());
        builder.setBindAddress("hadoop001").setPort(10000)
                .setProtocol(LoginService.class).setInstance(new LoginServiceImpl());
        RPC.Server server = builder.build();
        server.start();
    }
}
