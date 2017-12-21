package com.kakao.hbase.tools;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import java.util.List;
import java.util.Map;

public class Args {
    private static final String OPTION_ZK = "zk";
    private static final String OPTION_RS = "rs";
    private static final String OPTION_JAAS = "jaas";
    private static final String OPTION_KRB5 = "krb5";
    private static final String OPTION_REALM = "realm";
    private static final String OPTION_TABLE = "table";
    private static final String OPTION_LIMIT = "limit";
    private static final String OPTION_HELP = "help";

    // zk, rs - required
    private String zk = "";
    private String rs = "";
    // jaas, krb5, realm & table, limit - optional
    private String jaas = "";
    private String krb5 = "";
    private String realm = "";
    private String table = "";
    private String limit = "";

    public Args(String[] args) {
        OptionParser parser = new OptionParser();
        // zk, rs - required
        parser.accepts(OPTION_ZK).withRequiredArg().ofType(String.class);
        parser.accepts(OPTION_RS).withRequiredArg().ofType(String.class);
        // jaas, krb5, realm & table, limit - optional
        parser.accepts(OPTION_JAAS).withOptionalArg().ofType(String.class);
        parser.accepts(OPTION_KRB5).withOptionalArg().ofType(String.class);
        parser.accepts(OPTION_REALM).withOptionalArg().ofType(String.class);
        parser.accepts(OPTION_TABLE).withOptionalArg().ofType(String.class);
        parser.accepts(OPTION_LIMIT).withOptionalArg().ofType(String.class);
        parser.accepts(OPTION_HELP);

        OptionSet options = parser.parse(args);
        for (Map.Entry<OptionSpec<?>, List<?>> entry : options.asMap().entrySet()) {
            if (entry.getValue().size() > 0) {
                String keyName = entry.getKey().options().get(0);
                if (keyName.equals(OPTION_ZK)) zk = (String) entry.getValue().get(0);
                if (keyName.equals(OPTION_RS)) rs = (String) entry.getValue().get(0);
                if (keyName.equals(OPTION_TABLE)) table = (String) entry.getValue().get(0);
                if (keyName.equals(OPTION_LIMIT)) limit = (String) entry.getValue().get(0);
                if (keyName.equals(OPTION_JAAS)) jaas = (String) entry.getValue().get(0);
                if (keyName.equals(OPTION_KRB5)) krb5 = (String) entry.getValue().get(0);
                if (keyName.equals(OPTION_REALM)) realm = (String) entry.getValue().get(0);
            }
        }

        if (options.has(OPTION_HELP)
                || (!options.has(OPTION_ZK) || !options.has(OPTION_RS))
                || args.length == 0
                || ((options.has(OPTION_JAAS) || options.has(OPTION_KRB5) || options.has(OPTION_REALM)) && !(options.has(OPTION_JAAS) && options.has(OPTION_KRB5) && options.has(OPTION_REALM)))
                ) {
            System.out.println(getUsage());
            System.exit(0);
        }
    }

    public String getZK() { return zk; }
    public String getRS() { return rs; }
    public String getJaas() { return jaas; }
    public String getKrb5() {
        return krb5;
    }
    public String getRealm() {
        return realm;
    }
    public String getTable() { return table; }
    public String getLimit() { return limit; }

    private String getUsage() {
        return "Usage: java -jar WideRowPicker-xxx-jar-with-dependencies.jar \n"
                + "\t-zk=zk1.xxx.com:2181,zk2.xxx.com:2181,zk3.xxx.com:2181\n"
                + "\t-rs=rs1.xxx.com\n"
                + "\t--jaas=jaas file path\n"
                + "\t--krb5=krb5.conf path\n"
                + "\t--realm=realm\n"
                + "\t--table=table-name\n"
                + "\t--limit=10000";
    }
}