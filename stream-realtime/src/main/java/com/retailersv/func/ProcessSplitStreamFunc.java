package com.retailersv.func;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.streaming.api.functions.ProcessFunction;

/**
 * 数据分流处理函数：将输入的JSON对象按业务类型拆分到不同的侧输出流（Side Output）
 *  主要处理逻辑：
 *  1. 提取"err"字段数据，输出到错误流
 *  2. 提取"start"字段数据，输出到启动流；若"start"不存在，则继续处理其他字段
 *  3. 提取"displays"数组数据，补全公共信息后输出到展示流
 *  4. 提取"actions"数组数据，补全公共信息后输出到动作流
 *  5. 剩余核心数据输出到主流
 */

public class ProcessSplitStreamFunc extends ProcessFunction<JSONObject,String> {
    // 侧输出流标签：用于输出错误信息（如接口调用错误、数据格式错误等）
    private OutputTag< String> errTag;
    // 侧输出流标签：用于输出启动信息（如应用启动、页面初始化等事件）
    private OutputTag< String> startTag;
    // 侧输出流标签：用于输出展示信息（如商品曝光、广告展示等事件）
    private OutputTag< String> displayTag;
    // 侧输出流标签：用于输出动作信息（如点击、滑动、提交等用户行为）
    private OutputTag< String> actionTag;


    /**
     * 构造方法：初始化侧输出流标签
     * 通过外部传入标签，实现流的动态绑定，提高代码复用性
     * @param errTag 错误流标签
     * @param startTag 启动流标签
     * @param displayTag 展示流标签
     * @param actionTag 动作流标签
     */
    public ProcessSplitStreamFunc(OutputTag< String> errTag,OutputTag< String> startTag,OutputTag< String> displayTag,OutputTag< String> actionTag) {
        this.errTag = errTag;
        this.startTag = startTag;
        this.displayTag = displayTag;
        this.actionTag = actionTag;
    }
    @Override
    public void processElement(JSONObject jsonObject, Context context, Collector<String> collector) throws Exception {
        // 1. 处理错误信息：提取"err"字段，输出到错误侧输出流
        // 场景：如数据上报时的错误日志、接口调用失败信息等
        JSONObject errJson = jsonObject.getJSONObject("err");
        if (errJson != null){
            // 将错误信息JSON转为字符串，输出到errTag对应的侧输出流
            context.output(errTag,errJson.toJSONString());
            // 从原始JSON中移除"err"字段，避免后续处理重复消费
            jsonObject.remove("err");
        }
        JSONObject startJsonObj = jsonObject.getJSONObject("start");
        if (startJsonObj != null){
            context.output(startTag,startJsonObj.toJSONString());
        }else {
            JSONObject commonJsonObj = jsonObject.getJSONObject("common");
            JSONObject pageJsonObj = jsonObject.getJSONObject("page");
            Long ts = jsonObject.getLong("ts");
            JSONArray displayArr = jsonObject.getJSONArray("displays");
            if (displayArr != null && displayArr.size() > 0){
                for (int i = 0; i < displayArr.size(); i++) {
                    JSONObject displayJsonObj = displayArr.getJSONObject(i);
                    displayJsonObj.put("ts",ts);
                    displayJsonObj.put("common",commonJsonObj);
                    displayJsonObj.put("page",pageJsonObj);
                    displayJsonObj.put("display",displayJsonObj);
                    context.output(displayTag,displayJsonObj.toJSONString());
                }
                jsonObject.remove("displays");
            }
            // 5. 处理动作信息数组：如用户点击、滑动、提交等交互行为
            JSONArray actionArr = jsonObject.getJSONArray("actions");
            if (actionArr != null && actionArr.size() > 0){
                for (int i = 0; i < actionArr.size(); i++){
                    JSONObject actionJsonObj = actionArr.getJSONObject(i);
                    JSONObject newActionObj = new JSONObject();
                    newActionObj.put("ts",ts);
                    newActionObj.put("common",commonJsonObj);
                    newActionObj.put("page",pageJsonObj);
                    newActionObj.put("action",actionJsonObj);
                    context.output(actionTag,newActionObj.toJSONString());
                }
                jsonObject.remove("actions");
            }
            // 6. 将处理后剩余的核心数据输出到主流（如基础事件信息、非数组类型的业务数据等）
            collector.collect(jsonObject.toJSONString());
        }


    }


}
