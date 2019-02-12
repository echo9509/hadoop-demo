package cn.sh.hadoop.rpc.client;

import cn.sh.hadoop.rpc.LoginService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * @author sh
 */
public class LoginController {

    public static void main(String[] args) throws IOException {
        LoginService proxy = RPC.getProxy(LoginService.class, 2L,
                new InetSocketAddress("hadoop001", 10000), new Configuration());
        System.out.println(proxy.login("yangmi", "1234"));
    }
}
