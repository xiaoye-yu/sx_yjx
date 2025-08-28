package com.retailersv.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.retailersv.domain.TradeTrademarkCategoryUserRefundBean;
import com.retailersv.func.AsyncDimFunction;
import com.retailersv.func.BeanToJsonStrMapFunction;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.DateTimeUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.KafkaUtils;
import com.stream.utils.DorisUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.util.MD5Hash;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * @Package com.retailersv.dws
 * @Author xiaoye
 * @Date 2025/8/28 19:00
 * @description:
 */
public class DwsTradeTrademarkCategoryUserRefund2Doris {
    private static final String kafka_bootstrap_server = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String DWD_TRADE_ORDER_REFUND = ConfigUtils.getString("kafka.dwd.trade.order.refund");

    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        DataStreamSource<String> kafkaSourceDs = env.fromSource(
                KafkaUtils.buildKafkaSource(
                        kafka_bootstrap_server,
                        DWD_TRADE_ORDER_REFUND,
                        new Date().toString(),
                        OffsetsInitializer.earliest()
                ), WatermarkStrategy.noWatermarks(), "read_dwd_order_refund"
        );
//        kafkaSourceDs.print() ;

        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> beanStream = kafkaSourceDs
                .map(new MapFunction<String, TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    public TradeTrademarkCategoryUserRefundBean map(String value) {
                        JSONObject obj = JSON.parseObject(value);
                        return TradeTrademarkCategoryUserRefundBean.builder()
                                .orderIdSet(new HashSet<>(Collections.singleton(obj.getString("order_id"))))
                                .skuId(obj.getString("sku_id"))
                                .userId(obj.getString("user_id"))
                                .ts(obj.getLong("ts") )
                                .build();
                    }
                });
//        beanStream.print();

        // 补充 keyBy 字段维度
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> reducedStream = AsyncDataStream
                .unorderedWait(
                        beanStream,
                        new AsyncDimFunction<TradeTrademarkCategoryUserRefundBean>() {
                            @Override
                            public String getRowKey(TradeTrademarkCategoryUserRefundBean bean) {
                                String skuId = bean.getSkuId();
                                String md5SkuId = MD5Hash.getMD5AsHex(skuId.getBytes(StandardCharsets.UTF_8));
                                return md5SkuId;
                            }

                            @Override
                            public String getTableName() {
                                return "dim_sku_info";
                            }

                            @Override
                            public void addDims(TradeTrademarkCategoryUserRefundBean bean, JSONObject dim) {
                                if (dim != null){
                                    bean.setTrademarkId(dim.getString("tm_id"));
                                    bean.setCategory3Id(dim.getString("category3_id"));
                                }
                            }
                        },
                        120,
                        TimeUnit.SECONDS
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<TradeTrademarkCategoryUserRefundBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                                .withTimestampAssigner((bean, ts) -> bean.getTs())
                                .withIdleness(Duration.ofSeconds(120L))

                )
                .keyBy(bean -> bean.getUserId() + "_" + bean.getCategory3Id() + "_" + bean.getTrademarkId())
                .window(TumblingEventTimeWindows.of(Time.seconds(5L)))
                .reduce(
                        new ReduceFunction<TradeTrademarkCategoryUserRefundBean>() {
                            @Override
                            public TradeTrademarkCategoryUserRefundBean reduce(TradeTrademarkCategoryUserRefundBean value1,
                                                                               TradeTrademarkCategoryUserRefundBean value2) throws Exception {
                                value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                                return value1;
                            }
                        },
                        new ProcessWindowFunction<TradeTrademarkCategoryUserRefundBean, TradeTrademarkCategoryUserRefundBean, String, TimeWindow>() {
                            @Override
                            public void process(String s,
                                                Context ctx,
                                                Iterable<TradeTrademarkCategoryUserRefundBean> elements,
                                                Collector<TradeTrademarkCategoryUserRefundBean> out) throws Exception {
                                TradeTrademarkCategoryUserRefundBean bean = elements.iterator().next();

                                bean.setStt(DateTimeUtils.tsToDateTime(ctx.window().getStart()));
                                bean.setEdt(DateTimeUtils.tsToDateTime(ctx.window().getEnd()));

                                bean.setCurDate(DateTimeUtils.tsToDate(ctx.window().getStart()));

                                bean.setRefundCount((long) bean.getOrderIdSet().size());

                                out.collect(bean);
                            }
                        }
                );

//        reducedStream.print();

        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> tmStream = AsyncDataStream.unorderedWait(
                reducedStream,
                new AsyncDimFunction<TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    public String getRowKey(TradeTrademarkCategoryUserRefundBean bean) {
                        String trademarkId = bean.getTrademarkId();
                        String md5TrademarkId = MD5Hash.getMD5AsHex(trademarkId.getBytes(StandardCharsets.UTF_8));
                        return md5TrademarkId;

                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_trademark";
                    }

                    @Override
                    public void addDims(TradeTrademarkCategoryUserRefundBean bean,
                                        JSONObject dim) {
                        if (dim != null){
                            bean.setTrademarkName(dim.getString("tm_name"));
                        }
                    }
                },
                120,
                TimeUnit.SECONDS
        );

//        tmStream.print();

        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> c3Stream = AsyncDataStream.unorderedWait(
                tmStream,
                new AsyncDimFunction<TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    public String getRowKey(TradeTrademarkCategoryUserRefundBean bean) {
                        String category3Id = bean.getCategory3Id();
                        String md5Category3Id = MD5Hash.getMD5AsHex(category3Id.getBytes(StandardCharsets.UTF_8));
                        return md5Category3Id;
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_category3";
                    }

                    @Override
                    public void addDims(TradeTrademarkCategoryUserRefundBean bean, JSONObject dim) {
                        if (dim != null){
                            bean.setCategory3Name(dim.getString("name"));
                            bean.setCategory2Id(dim.getString("category2_id"));
                        }
                    }
                },
                120,
                TimeUnit.SECONDS
        );
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> c2Stream = AsyncDataStream.unorderedWait(
                c3Stream,
                new AsyncDimFunction<TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    public String getRowKey(TradeTrademarkCategoryUserRefundBean bean) {
                        String category2Id = bean.getCategory2Id();
                        String md5Category2Id = MD5Hash.getMD5AsHex(category2Id.getBytes(StandardCharsets.UTF_8));
                        return md5Category2Id;
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_category2";
                    }

                    @Override
                    public void addDims(TradeTrademarkCategoryUserRefundBean bean, JSONObject dim) {
                        if (dim != null){
                            bean.setCategory2Name(dim.getString("name"));
                            bean.setCategory1Id(dim.getString("category1_id"));
                        }
                    }
                },
                120,
                TimeUnit.SECONDS
        );


        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> resultStream = AsyncDataStream.unorderedWait(
                c2Stream,
                new AsyncDimFunction<TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    public String getRowKey(TradeTrademarkCategoryUserRefundBean bean) {
                        String category1Id = bean.getCategory1Id();
                        String md5Category1Id = MD5Hash.getMD5AsHex(category1Id.getBytes(StandardCharsets.UTF_8));
                        return md5Category1Id;
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_category1";
                    }

                    @Override
                    public void addDims(TradeTrademarkCategoryUserRefundBean bean, JSONObject dim) {
                        if (dim != null){
                            bean.setCategory1Name(dim.getString("name"));
                        }
                    }
                },
                120,
                TimeUnit.SECONDS
        );
        resultStream.print();


        resultStream
                .map(new BeanToJsonStrMapFunction<TradeTrademarkCategoryUserRefundBean>())
                .sinkTo(DorisUtils.getDorisSink("dws_trade_trademark_category_user_refund_window"));

        env.execute();
    }
}
