package com.sunhb.flinklearn.controller;

import com.sunhb.flinklearn.userPortrait.CDUserPurchaseAnalysis;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author: SunHB
 * @createTime: 2023/08/14 下午8:50
 * @description:
 */
@RestController
public class CDUserController {
    @Autowired
    private CDUserPurchaseAnalysis cdUserPurchaseAnalysis;
    @RequestMapping(path = "/cduserfilter",method = RequestMethod.GET)
    public String filter() throws Exception {
        //cdUserPurchaseAnalysis.processCDUser();
        return "SUCCESSFUL!";
    }

}
