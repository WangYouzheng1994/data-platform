开发参考资料

1. 多线程读取LGBR刷新延迟问题
https://blog.csdn.net/qiuqiufangfang1314/article/details/129095438
2. 多线程读取 碰到多线程读到了同一个事务数据的问题
https://www.modb.pro/db/191408
> 解决思路，如果是线程1读到了未提交的事务，要刷进缓存，她继续往下读取，

3. oracle 12C 在从库 新建一个抽数的实例，Logminer的深度解析思路
https://www.infoq.cn/article/pGakNSLI9xUfEj9HtOlT

> 总结以上大事务问题，思路是刷新缓存（后面如果大场景一定会oom）要存好。然后不管哪些线程，及时读取到的是没commit的数据一样去推送，
> 然后当读取到了undo以后，再生成删除的数据，类似于先推送一定会rollback的数据，进行完整的回放~
https://blog.csdn.net/weixin_34306446/article/details/94175733  
> https://blog.csdn.net/u011868279/article/details/127133931
