package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;

    @Override
    public Result queryById(Long id) {
        //缓存穿透
        //Shop shop = queryWithPassThrough(id);
//        Shop shop = cacheClient
//                .queryWithPassThrough(CACHE_SHOP_KEY, id, Shop.class, id2 -> getById(id2), CACHE_SHOP_TTL, TimeUnit.MINUTES);

        //互斥锁解决缓存击穿
//        Shop shop = queryWithMutex(id);

        //逻辑过期解决缓存击穿
//        Shop shop = queryWithLogicalExpire(id);
        Shop shop = cacheClient.queryWithLogicalExpire(CACHE_SHOP_KEY, id, Shop.class,id2 -> getById(id2), CACHE_SHOP_TTL, TimeUnit.MINUTES );

        //返回
        return Result.ok(shop);
    }

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    public Shop queryWithLogicalExpire(Long id){
        String key = CACHE_SHOP_KEY + id;

        //从redis查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //判断缓存是否命中
        if (StrUtil.isBlank(shopJson)){
            //命中，返回商铺信息
            return null;
        }
        //命中，需要先把json反序列化为对象
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        JSONObject data = (JSONObject) redisData.getData();
        Shop shop = JSONUtil.toBean(data, Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        //判断是否过期
        if(expireTime.isAfter(LocalDateTime.now())){
            //未过期，直接返回店铺信息
            return shop;
        }

        //已过期，需要缓存重建
        //获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        //判断是否取锁成功
        if(tryLock(lockKey)){
            //成功。开启独立线程，实现缓存重建
            CACHE_REBUILD_EXECUTOR.submit(() ->{
                //重建缓存
                this.saveShop2Redis(id, 20L);
                //释放锁
                unLock((lockKey));
            });
        }

        //返回过期的商铺信息
        return shop;
    }


    public Shop queryWithMutex(Long id){
        String key = CACHE_SHOP_KEY + id;

        //从redis查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //判断缓存是否命中
        if (StrUtil.isNotBlank(shopJson)){
            //命中，返回商铺信息
            //判断是否是空值
            if(shopJson == null){
                //是空
                return null;
            }
            //不是空
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        //未命中，实现缓存重建
        //获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        Shop shop = null;
        try {
            boolean isLock = tryLock(lockKey);
            //判断是否获取成功
            if(!isLock){
                //失败，则休眠并重试
                Thread.sleep(50); //休眠50毫秒
                queryWithMutex(id); //重试，即递归
            }
            //成功，根据id查询数据库
            shop = getById(id);

            //判断商铺是否存在
            //不存在，返回404
            if(shop == null){
                //将空值写入redis
                stringRedisTemplate.opsForValue().set(key, "null", CACHE_NULL_TTL, TimeUnit.MINUTES);
                //返回错误信息
                return null;

            }
            //存在，将商铺数据写入redis
            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            //释放互斥锁
            unLock(lockKey);
        }

        //返回
        return shop;
    }



    public Shop queryWithPassThrough(Long id){
        String key = CACHE_SHOP_KEY + id;

        //从redis查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //判断缓存是否命中
        if (StrUtil.isNotBlank(shopJson)){
            //命中，返回商铺信息
            //判断是否是空值
            if(shopJson == null){
                //是空
                return null;
            }
            //不是空
            return JSONUtil.toBean(shopJson, Shop.class);
        }

        //根据id查询数据库
        Shop shop = getById(id);
        //判断商铺是否存在
        //不存在，返回404
        if(shop == null){
            //将空值写入redis
            stringRedisTemplate.opsForValue().set(key, "null", CACHE_NULL_TTL, TimeUnit.MINUTES);
            //返回错误信息
            return null;

        }
        //存在，将商铺数据写入redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);

        return shop;
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

    public void saveShop2Redis(Long id, Long expireSeconds){
        //查询店铺数据
        Shop shop = getById(id);
        //封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        //写入redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
    }


    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if(id==null){
            return Result.fail("店铺id不能为空");
        }
        //更新数据库
        updateById(shop);
        //删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + id);

        return Result.ok();
    }
}
