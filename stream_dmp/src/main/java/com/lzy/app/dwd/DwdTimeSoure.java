package com.lzy.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lzy.stream.realtime.v1.utils.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

/**
 * @Package com.lzy.app.dwd.DwdTimeSoure
 * @Author zheyuan.liu
 * @Date 2025/5/15 9:14
 * @description:
 */

public class DwdTimeSoure {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource("dwd_order_info_join", "DwdTimeSoure");

        DataStreamSource<String> fromSource = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource");

//        fromSource.print();

        SingleOutputStreamOperator<JSONObject> streamOperator = fromSource.map(JSON::parseObject)
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                return element.getLong("ts_ms");
                            }
                        }));

        SingleOutputStreamOperator<JSONObject> map = streamOperator.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject value) throws Exception {
                JSONObject object = new JSONObject();
                String create_time = value.getString("create_time");
                String time = create_time.split(" ")[1];
                String hourStr = time.substring(0, 2);
                int hour = Integer.parseInt(hourStr);
                double timeRate = 0.1;
                // 定义时间段分类
                String timePeriod;
                if (hour >= 0 && hour < 6) {
                    timePeriod = "凌晨";
                    object.put("time_18_24", round(0.2 * timeRate));
                    object.put("time_25_29", round(0.1 * timeRate));
                    object.put("time_30_34", round(0.1 * timeRate));
                    object.put("time_35_39", round(0.1 * timeRate));
                    object.put("time_40_49", round(0.1 * timeRate));
                    object.put("time_50", round(0.1 * timeRate));
                } else if (hour >= 6 && hour < 9) {
                    timePeriod = "早晨";
                    object.put("time_18_24", round(0.1 * timeRate));
                    object.put("time_25_29", round(0.1 * timeRate));
                    object.put("time_30_34", round(0.1 * timeRate));
                    object.put("time_35_39", round(0.1 * timeRate));
                    object.put("time_40_49", round(0.2 * timeRate));
                    object.put("time_50", round(0.3 * timeRate));
                } else if (hour >= 9 && hour < 12) {
                    timePeriod = "上午";
                    object.put("time_18_24", round(0.2 * timeRate));
                    object.put("time_25_29", round(0.2 * timeRate));
                    object.put("time_30_34", round(0.2 * timeRate));
                    object.put("time_35_39", round(0.2 * timeRate));
                    object.put("time_40_49", round(0.3 * timeRate));
                    object.put("time_50", round(0.4 * timeRate));
                } else if (hour >= 12 && hour < 14) {
                    timePeriod = "中午";
                    object.put("time_18_24", round(0.4 * timeRate));
                    object.put("time_25_29", round(0.4 * timeRate));
                    object.put("time_30_34", round(0.4 * timeRate));
                    object.put("time_35_39", round(0.4 * timeRate));
                    object.put("time_40_49", round(0.4 * timeRate));
                    object.put("time_50", round(0.3 * timeRate));
                } else if (hour >= 14 && hour < 18) {
                    timePeriod = "下午";
                    object.put("time_18_24", round(0.4 * timeRate));
                    object.put("time_25_29", round(0.5 * timeRate));
                    object.put("time_30_34", round(0.5 * timeRate));
                    object.put("time_35_39", round(0.5 * timeRate));
                    object.put("time_40_49", round(0.5 * timeRate));
                    object.put("time_50", round(0.4 * timeRate));
                } else if (hour >= 18 && hour < 22) {
                    timePeriod = "晚上";
                    object.put("time_18_24", round(0.8 * timeRate));
                    object.put("time_25_29", round(0.7 * timeRate));
                    object.put("time_30_34", round(0.6 * timeRate));
                    object.put("time_35_39", round(0.5 * timeRate));
                    object.put("time_40_49", round(0.4 * timeRate));
                    object.put("time_50", round(0.3 * timeRate));
                } else {
                    timePeriod = "夜间";
                    object.put("time_18_24", round(0.9 * timeRate));
                    object.put("time_25_29", round(0.7 * timeRate));
                    object.put("time_30_34", round(0.5 * timeRate));
                    object.put("time_35_39", round(0.3 * timeRate));
                    object.put("time_40_49", round(0.2 * timeRate));
                    object.put("time_50", round(0.1 * timeRate));
                }
                object.put("name",value.getString("sku_name"));
                object.put("create_time",value.getString("create_time"));
                object.put("time_period", timePeriod);

                return object;
            }
        });

//        map.print();

        SingleOutputStreamOperator<JSONObject> operator = streamOperator.map(new RichMapFunction<JSONObject, JSONObject>() {
            final double lowPriceThreshold = 100.0;
            final double highPriceThreshold = 500.0;
            double priceRate = 0.15;

            @Override
            public JSONObject map(JSONObject value) throws Exception {
                JSONObject object = new JSONObject();
                double price = value.getDouble("order_price");
                String priceRange;

                if (price < lowPriceThreshold) {
                    priceRange = "低价商品";
                    object.put("price_18_24", round(0.8 * priceRate));
                    object.put("price_25_29", round(0.6 * priceRate));
                    object.put("price_30_34", round(0.4 * priceRate));
                    object.put("price_35_39", round(0.3 * priceRate));
                    object.put("price_40_49", round(0.2 * priceRate));
                    object.put("price_50", round(0.1 * priceRate));
                } else if (price <= highPriceThreshold) {
                    priceRange = "中价商品";
                    object.put("price_18_24", round(0.2 * priceRate));
                    object.put("price_25_29", round(0.4 * priceRate));
                    object.put("price_30_34", round(0.6 * priceRate));
                    object.put("price_35_39", round(0.7 * priceRate));
                    object.put("price_40_49", round(0.8 * priceRate));
                    object.put("price_50", round(0.7 * priceRate));
                } else {
                    priceRange = "高价商品";
                    object.put("price_18_24", round(0.1 * priceRate));
                    object.put("price_25_29", round(0.2 * priceRate));
                    object.put("price_30_34", round(0.3 * priceRate));
                    object.put("price_35_39", round(0.4 * priceRate));
                    object.put("price_40_49", round(0.5 * priceRate));
                    object.put("price_50", round(0.6 * priceRate));
                }

                object.put("price_range", priceRange);
                object.put("name", value.getString("sku_name"));

                return object;
            }
        });

        operator
                .keyBy(value -> value.getString("name"))
                .print();


        env.execute("DwdTimeSoure");
    }
    private static double round(double value) {
        return BigDecimal.valueOf(value)
                .setScale(3, RoundingMode.HALF_UP)
                .doubleValue();
    }
}
