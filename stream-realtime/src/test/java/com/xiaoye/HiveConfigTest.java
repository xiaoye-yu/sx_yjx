package com.xiaoye;
import org.apache.hadoop.hive.conf.HiveConf;
/**
 * @Package com.xiaoye
 * @Author xiaoye
 * @Date 2025/8/15 16:59
 * @description:
 */
public class HiveConfigTest {
    public static void main(String[] args) {
        HiveConf conf = new HiveConf();
        // 读取hive-site.xml中的配置，例如获取metastore地址
        String metastoreUris = conf.get("hive.metastore.uris");
        System.out.println("Hive Metastore URIs: " + metastoreUris);
    }
}
