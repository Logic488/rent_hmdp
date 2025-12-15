package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.core.conditions.interfaces.Func;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryTypeList() {

        String key = "shop:type:list:";
        //从redis查询店铺类型缓存
        String shopType = stringRedisTemplate.opsForValue().get(key);
        //判断是否命中
        if(StrUtil.isNotBlank(shopType)){
            //命中，返回店铺类型信息
            List<ShopType> shopTypeList = JSONUtil.toList(shopType, ShopType.class);
            return Result.ok(shopTypeList);
        }
        //未命中，从数据库查找
        List<ShopType> shopTypes = query().orderByAsc("sort").list();
        //判断商铺是否存在
        if(shopTypes.isEmpty()){
            //不存在，返回错误信息
            return Result.fail("未找到商铺信息");
        }
        //存在，写入redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shopTypes));
        //返回
        return Result.ok(shopTypes);
    }
}
