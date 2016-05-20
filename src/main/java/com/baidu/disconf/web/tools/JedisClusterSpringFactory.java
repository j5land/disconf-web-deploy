package com.baidu.disconf.web.tools;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.Resource;

import redis.clients.jedis.BinaryJedisCluster;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
  
/**
 * 集成spring的JedisClusterFactory<br/>
 * jedis 集群实例获取工厂类,提供JedisCluster实例的获取
 * @author vincent 2015-8-11  
 *
 */
public class JedisClusterSpringFactory implements FactoryBean<JedisCluster>, InitializingBean {  
  
	private static Logger logger=Logger.getLogger(JedisClusterSpringFactory.class);
	
    private Resource addressConfig; //配置文件所在路径 
    
    private String addressKeyPrefix ; //配置文件中集群主机的key值前缀
  
    private BinaryJedisCluster binaryJedisCluster; 
    
    private Integer timeout=2000; //获取redis超时时间
    
    private Integer maxRedirections=100; //最大重连次数
    
    private GenericObjectPoolConfig genericObjectPoolConfig; //连接池 
      
    private Pattern p = Pattern.compile("^.+[:]\\d{1,5}\\s*$");  
    
    public BinaryJedisCluster getObject() throws Exception {  
        return binaryJedisCluster;  
    }  
  
    public Class<? extends JedisCluster> getObjectType() {  
        return (this.binaryJedisCluster != null ? this.binaryJedisCluster.getClass() : BinaryJedisCluster.class);  
    }  
  
    public boolean isSingleton() {  
        return true;  
    }  
  
  
  
    private Set<HostAndPort> parseHostAndPort() throws Exception {  
        try {  
            Properties prop = new Properties();  
            prop.load(this.addressConfig.getInputStream());  
  
            Set<HostAndPort> haps = new HashSet<HostAndPort>();  
            for (Object key : prop.keySet()) {  
  
                if (!((String) key).startsWith(addressKeyPrefix)) {  
                    continue;  
                }  
  
                String val = (String) prop.get(key);  
  
                boolean isIpPort = p.matcher(val).matches();  
  
                if (!isIpPort) {  
                    throw new IllegalArgumentException("ip 或 port 不合法");  
                }  
                String[] ipAndPort = val.split(":");  
  
                HostAndPort hap = new HostAndPort(ipAndPort[0], Integer.parseInt(ipAndPort[1]));  
                haps.add(hap);  
            }  
  
            return haps;  
        } catch (IllegalArgumentException ex) {  
            throw ex;  
        } catch (Exception ex) {  
            throw new Exception("解析 jedis 配置文件失败", ex);  
        }  
    }  
      
    public void afterPropertiesSet() throws Exception {  
    	logger.info("start init redis");
        Set<HostAndPort> haps = this.parseHostAndPort();  
          
        binaryJedisCluster = new BinaryJedisCluster(haps, timeout, maxRedirections,genericObjectPoolConfig);  
          
    }  
    public void setAddressConfig(Resource addressConfig) {  
        this.addressConfig = addressConfig;  
    }  
  
    public void setTimeout(int timeout) {  
        this.timeout = timeout;  
    }  
  
    public void setMaxRedirections(int maxRedirections) {  
        this.maxRedirections = maxRedirections;  
    }  
  
    public void setAddressKeyPrefix(String addressKeyPrefix) {  
        this.addressKeyPrefix = addressKeyPrefix;  
    }  
  
    public void setGenericObjectPoolConfig(GenericObjectPoolConfig genericObjectPoolConfig) {  
        this.genericObjectPoolConfig = genericObjectPoolConfig;  
    }  
  
}  