package cn.sh.hadoop.rpc.server;

import cn.sh.hadoop.rpc.LoginService;

/**
 * @author sh
 */
public class LoginServiceImpl implements LoginService {

    public String login(String userName, String password) {
        System.out.println(userName + "申请登录");
        return userName + " logined in successful";
    }
}
