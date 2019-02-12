package cn.sh.hadoop.rpc;

/**
 * @author sh
 */
public interface LoginService {

    long versionID = 1L;

    String login(String userName, String password);
}
