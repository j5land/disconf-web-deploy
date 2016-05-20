package com.baidu.disconf.web.tools;
import java.io.File;
import java.io.FileInputStream;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import redis.clients.jedis.BinaryJedisCluster;
import redis.clients.jedis.HostAndPort;
  
/**
 * 集成spring的JedisClusterFactory<br/>
 * jedis 集群实例获取工厂类,提供JedisCluster实例的获取
 * @author vincent 2015-8-11  
 *
 */
public class JedisClusterFactory {  
	
	private static final Logger logger=Logger.getLogger(JedisClusterFactory.class);
  
    private String addressConfig; //配置文件所在路径 
    
    private String addressKeyPrefix ; //配置文件中集群主机的key值前缀
  
    private static BinaryJedisCluster binaryJedisCluster; 
    
    private Integer timeout=10000; //获取redis超时时间
    
    private Integer maxRedirections=6; //最大重连次数
    
    private Pattern p = Pattern.compile("^.+[:]\\d{1,5}\\s*$");  
    
    private static JedisClusterFactory instance;
    
    public static JedisClusterFactory getInstance(){
    	if(instance==null){
    		instance=new JedisClusterFactory();
    	}
    	return instance;
    }
    
    private JedisClusterFactory(){
    	//单例
    }
    
    public static BinaryJedisCluster getBinaryJedisCluster() {  
    	if(binaryJedisCluster==null){
    		logger.warn("binaryJedisCluster为空,无法使用jedis缓存");
    	//	throw new NullPointerException("binaryJedisCluster为空,请先调用init方法进行初始化之后再获取redis集群实例");
    	}
        return binaryJedisCluster;  
    }  
  
    /**
     * 从配置文件中解析出集群主机的ip和端口
     * @return
     * @throws Exception
     */
    private Set<HostAndPort> parseHostAndPort() throws Exception {  
        try {  
            Properties prop = new Properties();  
            prop.load(new FileInputStream(new File(addressConfig)));  
  
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
            throw new RuntimeException("解析 jedis 配置文件失败", ex);  
        }  
    }  
      
    public void init() {  
        Set<HostAndPort> haps;
		try {
			haps = this.parseHostAndPort();
			logger.info("开始初始化redis……");
			binaryJedisCluster = new BinaryJedisCluster(haps, timeout, maxRedirections);  
		} catch (Exception e) {
			logger.error("binaryJedisCluster init exceptioni:",e);
		}  
		logger.info("初始化redis完成……");  
    }  
    
    public void setAddressConfig(String addressConfig) {  
        this.addressConfig = addressConfig;  
    }  
  
    public void setTimeout(int timeout) {  
    	if(timeout!=0){
    		this.timeout = timeout;  
    	}
    }  
  
    public void setMaxRedirections(int maxRedirections) {  
    	if(maxRedirections!=0){
    		this.maxRedirections = maxRedirections;  
    	}
    }  
  
    public void setAddressKeyPrefix(String addressKeyPrefix) {  
        this.addressKeyPrefix = addressKeyPrefix;  
    }  
  
}  