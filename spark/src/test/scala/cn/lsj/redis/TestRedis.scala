package cn.lsj.redis

/**
  * Created by lsj on 2017/9/20.
  */
object TestRedis extends App {
  val jedis = RedisClient.pool.getResource
  val uid = "97edfc08311c70143401745a03a50706"
  jedis.select(2)
  val map1 = jedis.hgetAll(uid)
  if (map1.get("rowCount") == null) {
    println("取到了空的了")
  }

  println(map1)
  println("uid=" + uid, "clickCount=" + map1.get("clickCount"), "rowCount=" + map1.get("rowCount"))
  RedisClient.pool.returnResource(jedis)
}
