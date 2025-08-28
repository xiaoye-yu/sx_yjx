package com.retailersv.dws;

import com.alibaba.fastjson.JSONObject;
import com.retailersv.domain.UserRegisterBean;
import com.retailersv.func.BeanToJsonStrMapFunction;
import com.stream.common.utils.DateTimeUtils;
import com.stream.utils.DorisUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.KafkaUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Date;

/**
 * @Package com.retailersv.dws
 * @Author xiaoye
 * @Date 2025/8/28 9:30
 * @description:
 */
public class DwsUserUserRegister2Doris {
    private static final String kafka_bootstrap_server = ConfigUtils.getString("kafka.bootstrap.servers");

    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        DataStreamSource<String> kafkaSourceDs = env.fromSource(
                KafkaUtils.buildKafkaSource(
                        kafka_bootstrap_server,
                        "dwd_user_register",
                        new Date().toString(),
                        OffsetsInitializer.earliest()
                ), WatermarkStrategy.noWatermarks(), "read_dwd_user_register"
        );

//        kafkaSourceDs.print();
        SingleOutputStreamOperator<JSONObject> windowsDs = kafkaSourceDs.map(JSONObject::parseObject)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<JSONObject>() {
                                            @Override
                                            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                                return element.getLong("create_time");
                                            }
                                        }
                                ) // fastjson 会自动把 datetime 转成 long
                                .withIdleness(Duration.ofSeconds(120L))
                );

//        windowsDs.print();


        SingleOutputStreamOperator<UserRegisterBean> aggregateDs = windowsDs
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(
                        new AggregateFunction<JSONObject, Long, Long>() {
                            @Override
                            public Long createAccumulator() {
                                return 0L;
                            }

                            @Override
                            public Long add(JSONObject value, Long acc) {
                                return acc + 1;
                            }

                            @Override
                            public Long getResult(Long acc) {
                                return acc;
                            }

                            @Override
                            public Long merge(Long acc1, Long acc2) {
                                return acc1 + acc2;
                            }
                        },
                        new ProcessAllWindowFunction<Long, UserRegisterBean, TimeWindow>() {
                            @Override
                            public void process(Context ctx,
                                                Iterable<Long> elements,
                                                Collector<UserRegisterBean> out)  {
                                Long result = elements.iterator().next();

                                out.collect(new UserRegisterBean(DateTimeUtils.tsToDateTime(ctx.window().getStart()),
                                        DateTimeUtils.tsToDateTime(ctx.window().getEnd()),
                                        DateTimeUtils.tsToDate(ctx.window().getStart()),
                                        result
                                ));

                            }
                        }
                );
        aggregateDs.print();
        aggregateDs
                .map(new BeanToJsonStrMapFunction<UserRegisterBean>())
                .sinkTo(DorisUtils.getDorisSink("dws_user_user_register_window"));
        env.execute();

    }
}
