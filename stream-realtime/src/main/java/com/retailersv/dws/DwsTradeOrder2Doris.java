package com.retailersv.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.retailersv.domain.TradeOrderBean;
import com.retailersv.func.BeanToJsonStrMapFunction;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.DateTimeUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.KafkaUtils;
import com.stream.utils.DorisUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Date;

/**
 * @Package com.retailersv.dws
 * @Author xiaoye
 * @Date 2025/8/28 16:12
 * @description:
 */
public class DwsTradeOrder2Doris {
    private static final String kafka_bootstrap_server = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String DWD_TRADE_ORDER_DETAIL = ConfigUtils.getString("kafka.dwd.trade.order.detail");

    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        DataStream<String> kafkaSourceDs = env.fromSource(
                KafkaUtils.buildKafkaSecureSource(
                        kafka_bootstrap_server,
                        DWD_TRADE_ORDER_DETAIL,
                        new Date().toString(),
                        OffsetsInitializer.earliest()
                ),
                WatermarkStrategy.noWatermarks(),
                "read_dws_trade_order"
        ).filter(str -> str != null);
//        kafkaSourceDs.print() ;
        //TODO 1.过滤空消息  并对流中数据进行类型转换    jsonStr->jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaSourceDs.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        if (jsonStr != null) {
                            out.collect(JSON.parseObject(jsonStr));
                        }
                    }
                }
        );


        SingleOutputStreamOperator<TradeOrderBean> mapDs = jsonObjDS
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                                .withTimestampAssigner((obj, ts) -> obj.getLong("ts"))
                                .withIdleness(Duration.ofSeconds(120L))
                )
                .keyBy(obj -> obj.getString("user_id"))
                .process(new KeyedProcessFunction<String, JSONObject, TradeOrderBean>() {

                    private ValueState<String> lastOrderDateState;

                    @Override
                    public void open(Configuration parameters) {
                        lastOrderDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastOrderDate", String.class));
                    }

                    @Override
                    public void processElement(JSONObject value,
                                               Context ctx,
                                               Collector<TradeOrderBean> out) throws Exception {
                        long ts = value.getLong("ts");

                        String today = DateTimeUtils.tsToDate(ts);
                        String lastOrderDate = lastOrderDateState.value();

                        long orderUu = 0L;
                        long orderNew = 0L;
                        if (!today.equals(lastOrderDate)) {
                            orderUu = 1L;
                            lastOrderDateState.update(today);

                            if (lastOrderDate == null) {
                                orderNew = 1L;
                            }

                        }
                        if (orderUu == 1) {
                            out.collect(new TradeOrderBean("", "", "", orderUu, orderNew, ts));
                        }
                    }
                });

//        mapDs.print();

        SingleOutputStreamOperator<TradeOrderBean> reduceDs = mapDs
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5L)))
                .reduce(
                        new ReduceFunction<TradeOrderBean>() {
                            @Override
                            public TradeOrderBean reduce(TradeOrderBean value1,
                                                         TradeOrderBean value2) {
                                if (value1 == null) return value2;
                                if (value2 == null) return value1;

                                long unique1 = value1.getOrderUniqueUserCount() == null ? 0L : value1.getOrderUniqueUserCount();
                                long unique2 = value2.getOrderUniqueUserCount() == null ? 0L : value2.getOrderUniqueUserCount();

                                long newUser1 = value1.getOrderNewUserCount() == null ? 0L : value1.getOrderNewUserCount();
                                long newUser2 = value2.getOrderNewUserCount() == null ? 0L : value2.getOrderNewUserCount();

                                value1.setOrderUniqueUserCount(unique1 + unique2);
                                value1.setOrderNewUserCount(newUser1 + newUser2);
                                return value1;
                            }
                        },
                        new ProcessAllWindowFunction<TradeOrderBean, TradeOrderBean, TimeWindow>() {
                            @Override
                            public void process(Context ctx,
                                                Iterable<TradeOrderBean> elements,
                                                Collector<TradeOrderBean> out) throws Exception {
                                TradeOrderBean bean = elements.iterator().next();
                                bean.setStt(DateTimeUtils.tsToDateTime(ctx.window().getStart()));
                                bean.setEdt(DateTimeUtils.tsToDateTime(ctx.window().getEnd()));
                                bean.setCurDate(DateTimeUtils.tsToDate(ctx.window().getStart()));
                                out.collect(bean);
                            }
                        }
                );

        reduceDs.print();

        reduceDs
                .map(new BeanToJsonStrMapFunction<TradeOrderBean>())
                .sinkTo(DorisUtils.getDorisSink("dws_trade_order_window"));

        env.execute();
    }
}
