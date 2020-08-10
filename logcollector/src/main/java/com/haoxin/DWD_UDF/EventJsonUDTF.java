package com.haoxin.DWD_UDF;


import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONException;


import java.util.ArrayList;

/**
 * @Auther Huolibin
 * @Date 2020/8/5
 */
public class EventJsonUDTF extends GenericUDTF {

    //该方法中，我们将制定输出参数的名称和参数类型
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        ArrayList<String> fieldNames = new ArrayList<>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<>();

        fieldNames.add("event_name");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("event_json");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,fieldOIs);
    }

    //输入1条记录，输出若干条结果
    @Override
    public void process(Object[] objects) throws HiveException {
        //获取传入的et
        String input = objects[0].toString();

        // 如果传进来的数据为空，直接返回过滤掉该数据
        if (StringUtils.isBlank(input)){
            return;
        }else {
            try {
                //获取一共有几个事件
                JSONArray ja = new JSONArray(input);
                if (ja == null)
                    return;

                //循环遍历事件
                for (int i = 0; i < ja.length(); i++) {
                    String[] result = new String[2];
                    try {
                        //获得事件名称
                        result[0] = ja.getJSONObject(i).getString("en");
                        //获得整个事件
                        result[1] = ja.getString(i);
                    } catch (JSONException e) {
                        e.printStackTrace();
                        continue;
                    }
                    forward(result);
                }

            } catch (JSONException e) {
                e.printStackTrace();
            }
        }

    }

    //当没有记录处理的时候该方法会被调用，用来清理代码或者产生额外的输出
    @Override
    public void close() throws HiveException {

    }
}
