package com.retailersv.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.retailersv.domain.TradeTrademarkCategoryUserRefundBean;
import com.retailersv.domain.TrafficSkuUvBean;
import com.retailersv.func.BeanToJsonStrMapFunction;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.DateTimeUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.KafkaUtils;
import com.stream.utils.DorisUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


import java.time.Duration;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

/**
 * @Package com.retailersv.dws
 * @Author xiaoye
 * @Date 2025/8/29 11:51
 * @description:
 */
public class DwsTrafficSkuUv2Doris {
    private static final String kafka_bootstrap_server = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String DWD_TRADE_ORDER_DETAIL = ConfigUtils.getString("kafka.dwd.trade.order.detail");

    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        DataStreamSource<String> kafkaSourceDs = env.fromSource(
                KafkaUtils.buildKafkaSecureSource(
                        kafka_bootstrap_server,
                        DWD_TRADE_ORDER_DETAIL,
                        new Date().toString(),
                        OffsetsInitializer.earliest()
                ), WatermarkStrategy.noWatermarks(), "read_dwd_order_refund"
        );
//        kafkaSourceDs.print() ;
        SingleOutputStreamOperator<TrafficSkuUvBean> mapDs = kafkaSourceDs.map(new MapFunction<String, TrafficSkuUvBean>() {
            @Override
            public TrafficSkuUvBean map(String value) throws Exception {
                JSONObject obj = JSON.parseObject(value);
                return TrafficSkuUvBean.builder()
                        .skuName(obj.getString("sku_name"))
                        .userId(obj.getString("user_id"))
                        .ts(obj.getLong("ts"))
                        .build();
            }
        });

        SingleOutputStreamOperator<TrafficSkuUvBean> withWatermarkDS  = mapDs.assignTimestampsAndWatermarks(
                WatermarkStrategy.<TrafficSkuUvBean>forBoundedOutOfOrderness(Duration.ofMinutes(5))
                        .withTimestampAssigner((element, recordTs) -> element.getTs())
                        .withIdleness(Duration.ofSeconds(120L))
        );


//        withWatermarkDS.print();
        SingleOutputStreamOperator<TrafficSkuUvBean> processDs = withWatermarkDS
                .keyBy(TrafficSkuUvBean::getSkuName)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .process(new ProcessWindowFunction<TrafficSkuUvBean, TrafficSkuUvBean, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<TrafficSkuUvBean> elements,
                                        Collector<TrafficSkuUvBean> out) {

                        Set<String> userSet = new HashSet<>();
                        for (TrafficSkuUvBean e : elements) {
                            userSet.add(e.getUserId());
                        }

                        TrafficSkuUvBean result = new TrafficSkuUvBean();
                        result.setSkuName(key);
                        result.setUvCnt((long) userSet.size());
                        result.setStt(DateTimeUtils.tsToDateTime(context.window().getStart()));
                        result.setEdt(DateTimeUtils.tsToDateTime(context.window().getEnd()));
                        result.setCurDate(DateTimeUtils.tsToDate(context.window().getStart()));
                        out.collect(result);
                    }
                });
        processDs.print();
        processDs
                .map(new BeanToJsonStrMapFunction<>())
                .sinkTo(DorisUtils.getDorisSink("dws_traffic_sku_uv_window"));


        env.execute();
    }
}
