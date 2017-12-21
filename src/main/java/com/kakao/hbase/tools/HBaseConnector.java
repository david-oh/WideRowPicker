package com.kakao.hbase.tools;

import com.sun.security.auth.callback.TextCallbackHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.security.UserGroupInformation;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.util.Map;

import static com.kakao.hbase.tools.Util.SEPARATOR;

public class HBaseConnector {
    private Connection conn = null;
    private final Configuration conf = HBaseConfiguration.create(new Configuration(true));

    public Connection gethConn(String zk) throws IOException {
        conf.set("hbase.zookeeper.quorum", zk);
        conn = ConnectionFactory.createConnection(conf);
        Util.println(SEPARATOR);
        Util.println("* Insecure Cluster (ZK) : " + zk);
        return conn;
    }

    public Connection gethConn(String zk, String jaas, String krb5, String realm)
                throws IOException, LoginException {
        System.setProperty("java.security.auth.login.config", jaas);
        System.setProperty("java.security.krb5.conf", krb5);

        javax.security.auth.login.Configuration conf = javax.security.auth.login.Configuration.getConfiguration();
        AppConfigurationEntry[] appConfs = conf.getAppConfigurationEntry("Client");
        AppConfigurationEntry appConf = appConfs[0];
        Map<String, ?> options = appConf.getOptions();

        String principal = (String) options.get("principal");
        String keyTab = (String) options.get("keyTab");
        String useKeyTab = (String) options.get("useKeyTab");
        String useTicketCache = (String) options.get("useTicketCache");

        this.conf.set("hbase.zookeeper.quorum", zk);
        this.conf.set("hadoop.security.authentication", "Kerberos");
        this.conf.set("hbase.security.authentication", "Kerberos");
        this.conf.set("hbase.master.kerberos.principal", "hbase/_HOST@" + realm);
        this.conf.set("hbase.regionserver.kerberos.principal", "hbase/_HOST@" + realm);

        if (useTicketCache != null && useTicketCache.length() != 0 && useTicketCache.equals("true")) {
            LoginContext lc = new LoginContext("Client", new TextCallbackHandler());
            lc.login();
            UserGroupInformation.setConfiguration(this.conf);
            UserGroupInformation.loginUserFromSubject(lc.getSubject());
            UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
            Util.println(SEPARATOR);
            Util.println("* Secure Cluster (ZK) : " + zk);
            Util.println("* Secure Cluster (ticket cache) :" +currentUser);
            Util.println(SEPARATOR);
        }

        if (useKeyTab != null && useKeyTab.length() != 0 && useKeyTab.equals("true")) {
            UserGroupInformation.setConfiguration(this.conf);
            UserGroupInformation.loginUserFromKeytab(principal, keyTab);
            UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
            Util.println(SEPARATOR);
            Util.println("* Secure Cluster (ZK) : " + zk);
            Util.println("* Secure Cluster (keytab) :" +currentUser);
            Util.println(SEPARATOR);
        }

        conn = ConnectionFactory.createConnection(this.conf);
        return conn;
    }
}
