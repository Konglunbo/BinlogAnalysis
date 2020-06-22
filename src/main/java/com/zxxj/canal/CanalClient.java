package com.zxxj.canal;


import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.Header;
import net.sf.json.JSONObject;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.zxxj.utils.KafkaUtils;
import com.zxxj.utils.PropertiesUtil;

import java.text.SimpleDateFormat;
import java.util.*;


public class CanalClient extends AbstractCanalConsumer {
    private final static Logger LOG = LoggerFactory.getLogger(CanalClient.class);
    private boolean showDetailLog = Boolean.parseBoolean(PropertiesUtil.getString("showDetailLog"));
    private String columeFilter = PropertiesUtil.getString("columeFilter");

    public static void main(String args[]) {
        LOG.info(" binLog task start run ");
        CanalClient c = new CanalClient();
        c.start();
    }
    private void printColumn(List<Column> columns, String eventType, Header header) {
        // HashMap用来存放 拆分后的列名
        Map tableAndColumeMap = new HashMap<String,String>();
        // 获取kafka topic的名字
        String kafkaTopic = PropertiesUtil.getString("topicName");
        // 对传入的columeFilter 进行切分
        String[] columeArray =columeFilter.split("\\$");
        for(String colume: columeArray){
            String[] tableAndColume =colume.split(":");
            //表名和列名 放入hashMap
            tableAndColumeMap.put(tableAndColume[0],tableAndColume[1]);
        }
        //获取数据库名称
        String dataBaseName = header.getSchemaName();
        // 获取表名
        String tableName = header.getTableName();
        // 获取时间戳
        String binLogTimeStamp = stampToDate(header.getExecuteTime());
        if (StringUtils.isNotBlank(dataBaseName)) {

            Map colume = new HashMap<String, String>();
            colume.put("dataEventType", eventType);
            colume.put("dataBaseName", dataBaseName);
            colume.put("tableName", header.getTableName());
            colume.put("binLogTimeStamp", binLogTimeStamp);
            for (Column column : columns) {

                if(!(tableAndColumeMap.containsKey(header.getTableName())&& ArrayUtils.contains(tableAndColumeMap.get(header.getTableName()).toString().split(","),column.getName()))){
                    colume.put(column.getName(), column.getValue());
//                    LOG.info(column.getName() + " : " + column.getValue() + "," + column.getMysqlType() + " ," + column.toString());
                    if(showDetailLog){
                        LOG.info(column.getName() + " : " + column.getValue());
                    }
                }
            }
            // 生成唯一标识符
            String uuid = UUID.randomUUID().toString();
            colume.put("UUID", uuid);
            JSONObject json = JSONObject.fromObject(colume);
            LOG.info("--- DB changes event DataBaseName  " + dataBaseName + " tableName " + tableName + "eventType  " + eventType + " uuid "+uuid);
            KafkaUtils.sendMsg(kafkaTopic, uuid, json.toString());
        } else {
            LOG.info("database name is enpty ");
        }
    }

    /*
     * 将时间戳转换为时间
     */
    public static String stampToDate(long s) {
        String res;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = new Date(s);
        res = simpleDateFormat.format(date);
        return res;
    }

    @Override
    public void insert(Header header, List<Column> afterColumns) {
        printColumn(afterColumns, "insert", header);
    }

    @Override
    public void update(Header header, List<Column> beforeColumns, List<Column> afterColumns) {
        printColumn(afterColumns, "update", header);
    }

    @Override
    public void delete(Header header, List<Column> beforeColumns) {
        printColumn(beforeColumns, "delete", header);
    }
}
