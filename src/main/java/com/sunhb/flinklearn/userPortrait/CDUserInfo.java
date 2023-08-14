package com.sunhb.flinklearn.userPortrait;

/**
 * @author: SunHB
 * @createTime: 2023/08/11 上午9:49
 * @description:
 */
public class CDUserInfo {
    private String user;
    private String order_datetime;
    private String order_products;
    private Double order_amount;

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        user = user;
    }

    public String getOrder_datetime() {
        return order_datetime;
    }

    public void setOrder_datetime(String order_datetime) {
        this.order_datetime = order_datetime;
    }

    public String getOrder_products() {
        return order_products;
    }

    public void setOrder_products(String order_products) {
        this.order_products = order_products;
    }

    public Double getOrder_amount() {
        return order_amount;
    }

    public void setOrder_amount(Double order_amount) {
        this.order_amount = order_amount;
    }

    @Override
    public String toString() {
        return "CDUserInfo{" +
                "User='" + user + '\'' +
                ", order_datetime='" + order_datetime + '\'' +
                ", order_products='" + order_products + '\'' +
                ", order_amount='" + order_amount + '\'' +
                '}';
    }
}
