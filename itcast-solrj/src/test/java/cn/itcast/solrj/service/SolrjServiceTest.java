package cn.itcast.solrj.service;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import cn.itcast.solrj.pojo.Foo;
import cn.itcast.solrj.pojo.Item;

public class SolrjServiceTest {

    private SolrjService solrjService;
    
    private HttpSolrServer httpSolrServer;

    @Before
    public void setUp() throws Exception {
        // 在url中指定core名称：taotao
        String url = "http://solr.taotao.com/taotao";
        HttpSolrServer httpSolrServer = new HttpSolrServer(url); //定义solr的server
        httpSolrServer.setParser(new XMLResponseParser()); // 设置响应解析器
        httpSolrServer.setMaxRetries(1); // 设置重试次数，推荐设置为1
        httpSolrServer.setConnectionTimeout(500); // 建立连接的最长时间

        this.httpSolrServer = httpSolrServer;
        solrjService = new SolrjService(httpSolrServer);
    }
    @Test
    public void testInsert() throws Exception{
        Item item = new Item();
        item.setCid(1L);
        item.setId(999L);
        item.setImage("image");
        item.setPrice(100L);
        item.setSellPoint("很好啊，赶紧来买吧.");
        item.setStatus(1);
        item.setTitle("飞利浦 老人手机 (X2560) 深情蓝 移动联通2G手机 双卡双待");
        this.httpSolrServer.addBean(item);
        this.httpSolrServer.commit();
    }
    
    @Test
    public void testUpdate() throws Exception{
        Item item = new Item();
        item.setCid(560L);
        item.setId(1187781L);
        item.setImage("http://image.taotao.com/jd/1c949a7322304ed4991fb232ae094f11.jpg");
        item.setPrice(1399000L);
        item.setSellPoint("四核处理器，5英寸1920*1080高清大屏，16GB ROM+2GB RAM内存，1300+200万摄像头！");
        item.setStatus(1);
        item.setTitle("小米 3 联通16G 星空灰 联通3G手机");
        this.httpSolrServer.addBean(item);
        this.httpSolrServer.commit();
    }
    @Test
    public void testAdd() throws Exception {
        Foo foo = new Foo();
        foo.setId(System.currentTimeMillis() + "");
        foo.setTitle("轻量级Java EE企业应用实战（第3版）：Struts2＋Spring3＋Hibernate整合开发（附CD光盘）");

        this.solrjService.add(foo);
    }

    @Test
    public void testDelete() throws Exception{
        this.httpSolrServer.deleteById("999");
        this.httpSolrServer.commit();
    }

    @Test
    public void testSearch() throws Exception {
        List<Foo> foos = this.solrjService.search("linux", 1, 10);
        for (Foo foo : foos) {
            System.out.println(foo);
        }
    }
    
    @Test
    public void testDeleteByQuery() throws Exception{
        httpSolrServer.deleteByQuery("*:*");
        httpSolrServer.commit();
    }
    
    @Test
    public void testAddData() throws ClientProtocolException, IOException, SolrServerException{
    	String url = "http://manage.taotao.com/rest/item?page={page}&rows=30000";
    	int page=1;
    	int pageSize = 0;
    	ObjectMapper mapper = new ObjectMapper();
		url = StringUtils.replace(url, "{page}", ""+page);
		String jsonData = doget(url);
		JsonNode jsonNode = mapper.readTree(jsonData);
		String rowStr = jsonNode.get("rows").toString();
		List<Item> items = mapper.readValue(rowStr, mapper.getTypeFactory().constructCollectionType(List.class, Item.class));
		pageSize = items.size();
		this.httpSolrServer.addBeans(items);
		this.httpSolrServer.commit();
    }
    
    private String doget(String url ) throws ClientProtocolException, IOException {
        CloseableHttpClient httpclient = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet(url);
        CloseableHttpResponse response = null;
        try {
            // 执行请求
            response = httpclient.execute(httpGet);
            // 判断返回状态是否为200
            if (response.getStatusLine().getStatusCode() == 200) {
                String content = EntityUtils.toString(response.getEntity(), "UTF-8");
                System.out.println("内容长度："+content.length());
                return content;
            }
        } finally {
            if (response != null) {
                response.close();
            }
            httpclient.close();
        }
        
        return null;
	}
}
