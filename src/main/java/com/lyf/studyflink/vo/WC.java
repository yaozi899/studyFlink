package com.lyf.studyflink.vo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @description: WC
 * @author: lyf
 * @create: 2020-06-26 16:12
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class WC {
    /**
     * word
     */
    public String word;

    /**
     * 出现的次数
     */
    public String pfId;
}

