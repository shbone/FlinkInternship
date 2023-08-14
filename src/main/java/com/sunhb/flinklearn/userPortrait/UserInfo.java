package com.sunhb.flinklearn.userPortrait;

/**
 * @author: SunHB
 * @createTime: 2023/08/10 上午8:30
 * @description:
 */
public class UserInfo {
    private String pid;
    private String label;
    private String gender;
    private String age;

    private String province;
    private String city;

    private String make;
    private String model;

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public String getAge() {
        return age;
    }

    public void setAge(String age) {
        this.age = age;
    }

    public String getMake() {
        return make;
    }

    public void setMake(String make) {
        this.make = make;
    }

    public String getModel() {
        return model;
    }

    public void setModel(String model) {
        this.model = model;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    @Override
    public String toString() {
        return "UserInfo{" +
                "pid='" + pid + '\'' +
                ", label='" + label + '\'' +
                ", gender='" + gender + '\'' +
                ", age='" + age + '\'' +
                ", province='" + province + '\'' +
                ", city='" + city + '\'' +
                ", make='" + make + '\'' +
                ", model='" + model + '\'' +
                '}';
    }
}
