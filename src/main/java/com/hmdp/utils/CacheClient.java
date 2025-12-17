package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

@Slf4j
@Component
public class CacheClient {

    private final StringRedisTemplate stringRedisTemplate;
    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public void set(String key, Object value, Long time, TimeUnit unit){
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit){
        //设置逻辑过期
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        //写入redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData), time, unit);
    }


    public <R, ID> R queryWithPassThrough(
            String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit){
        String key = keyPrefix + id;

        //从redis查询商铺缓存
        String Json = stringRedisTemplate.opsForValue().get(key);
        //判断缓存是否命中
        if (StrUtil.isNotBlank(Json)){
            //命中，返回商铺信息
            //判断是否是空值
            if(Json == null){
                //是空
                return null;
            }
            //不是空
            return JSONUtil.toBean(Json, type);
        }

        //根据r查询数据库
        R r = dbFallback.apply(id);
        //判断商铺是否存在
        //不存在，返回404
        if(r == null){
            //将空值写入redis
            stringRedisTemplate.opsForValue().set(key, "null", CACHE_NULL_TTL, TimeUnit.MINUTES);
            //返回错误信息
            return null;

        }
        //存在，将商铺数据写入redis
//        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(r), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        this.set(key, r, time, unit);

        return r;
    }


    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    public <R, ID> R queryWithLogicalExpire(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit){
        String key = keyPrefix + id;

        //从redis查询商铺缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        //判断缓存是否命中
        if (StrUtil.isBlank(json)){
            //命中，返回商铺信息
            return null;
        }
        //命中，需要先把json反序列化为对象
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        R r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        LocalDateTime expireTime = redisData.getExpireTime();
        //判断是否过期
        if(expireTime.isAfter(LocalDateTime.now())){
            //未过期，直接返回店铺信息
            return r;
        }

        //已过期，需要缓存重建
        //获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        //判断是否取锁成功
        if(tryLock(lockKey)){
            //成功。开启独立线程，实现缓存重建
            CACHE_REBUILD_EXECUTOR.submit(() ->{
                try {
                    //重建缓存
                    //查询数据库
                    R r1 = dbFallback.apply(id);
                    //写入redis
                    this.setWithLogicalExpire(key, r1, time, unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    //释放锁
                    unLock((lockKey));
                }
            });
        }

        //返回过期的商铺信息
        return r;
    }

    //上锁
    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }
    //释放锁
    private void unLock(String key){
        stringRedisTemplate.delete(key);
    }


}
