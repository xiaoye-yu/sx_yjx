package com.retailersv.domain;

/**
 * @Package com.retailersv.domain
 * @Author xiaoye
 * @Date 2025/8/29 11:47
 * @description:
 */
import com.alibaba.fastjson.annotation.JSONField;
import com.amazonaws.thirdparty.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class TrafficSkuUvBean {
    // 窗口起始时间
    String stt;
    // 窗口结束时间
    String edt;
    // 当天日期
    @JsonProperty("cur_date")
    String curDate;
    @JsonProperty("sku_name")
    // 商品名称
    String skuName;
    // 访客数
    @JsonProperty("uv_cnt")
    Long uvCnt;
    // 时间戳
    @JSONField(serialize = false)
    Long ts;
    @JSONField(serialize = false)
    String userId;
}
