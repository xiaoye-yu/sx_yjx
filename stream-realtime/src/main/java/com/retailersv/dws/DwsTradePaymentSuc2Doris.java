package com.retailersv.dws;

import com.alibaba.fastjson.JSONObject;
import com.cloudera.io.netty.util.Constant;
import com.retailersv.domain.TradePaymentBean;
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
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Date;

/**
 * @Package com.retailersv.dws
 * @Author xiaoye
 * @Date 2025/8/28 14:18
 * @description:
 */
public class DwsTradePaymentSuc2Doris {
    private static final String kafka_bootstrap_server = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String DWD_TRADE_ORDER_PAYMENT_SUCCESS = ConfigUtils.getString("kafka.dwd.trade.order.pay.suc.detail");

    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        DataStreamSource<String> kafkaSourceDs = env.fromSource(
                KafkaUtils.buildKafkaSource(
                        kafka_bootstrap_server,
                        DWD_TRADE_ORDER_PAYMENT_SUCCESS,
                        new Date().toString(),
                        OffsetsInitializer.earliest()
                ), WatermarkStrategy.noWatermarks(), "read_dwd_user_register"
        );
//        kafkaSourceDs.print() ;

        SingleOutputStreamOperator<TradePaymentBean> mapDs = kafkaSourceDs.map(JSONObject::parseObject)
                .keyBy(obj -> obj.getString("user_id"))
                .process(new KeyedProcessFunction<String, JSONObject, TradePaymentBean>() {

                    private ValueState<String> lastPayDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastPayDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastPayDate", String.class));
                    }

                    @Override
                    public void processElement(JSONObject obj,
                                               Context ctx,
                                               Collector<TradePaymentBean> out) throws Exception {
                        String lastPayDate = lastPayDateState.value();
                        long ts = obj.getLong("ts");
                        String today = DateTimeUtils.tsToDate(ts);

                        long payUuCount = 0L;
                        long payNewCount = 0L;
                        if (!today.equals(lastPayDate)) {  // 今天第一次支付成功
                            lastPayDateState.update(today);
                            payUuCount = 1L;

                            if (lastPayDate == null) {
                                // 表示这个用户曾经没有支付过, 是一个新用户支付
                                payNewCount = 1L;
                            }
                        }

                        if (payUuCount == 1) {
                            out.collect(new TradePaymentBean("", "", "", payUuCount, payNewCount, ts));
                        }

                    }
                });
//        mapDs.print();

        SingleOutputStreamOperator<TradePaymentBean> reduceDs = mapDs.assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<TradePaymentBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                                .withTimestampAssigner((bean, ts) -> bean.getTs())
                                .withIdleness(Duration.ofSeconds(120L))
                )
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5L)))
                .reduce(
                        new ReduceFunction<TradePaymentBean>() {
                            @Override
                            public TradePaymentBean reduce(TradePaymentBean value1,
                                                           TradePaymentBean value2) throws Exception {
                                value1.setPaymentSucNewUserCount(value1.getPaymentSucNewUserCount() + value2.getPaymentSucNewUserCount());
                                value1.setPaymentSucUniqueUserCount(value1.getPaymentSucUniqueUserCount() + value2.getPaymentSucUniqueUserCount());
                                return value1;
                            }
                        },
                        new ProcessAllWindowFunction<TradePaymentBean, TradePaymentBean, TimeWindow>() {
                            @Override
                            public void process(Context ctx,
                                                Iterable<TradePaymentBean> elements,
                                                Collector<TradePaymentBean> out) throws Exception {
                                TradePaymentBean bean = elements.iterator().next();
                                bean.setStt(DateTimeUtils.tsToDateTime(ctx.window().getStart()));
                                bean.setEdt(DateTimeUtils.tsToDateTime(ctx.window().getEnd()));
                                bean.setCurDate(DateTimeUtils.tsToDate(ctx.window().getStart()));
                                out.collect(bean);
                            }
                        }
                );

        reduceDs.print();

        reduceDs
                .map(new BeanToJsonStrMapFunction<TradePaymentBean>())
                .sinkTo(DorisUtils.getDorisSink("dws_trade_payment_suc_window"));

        env.execute();

    }
}
