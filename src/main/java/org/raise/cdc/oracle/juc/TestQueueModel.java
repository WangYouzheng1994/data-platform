package org.raise.cdc.oracle.juc;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Description: https://zhuanlan.zhihu.com/p/442047649
 * @Author: WangYouzheng
 * @Date: 2023/6/12 15:56
 * @Version: V1.0
 */
public class TestQueueModel {

    public static void main(String[] args) {
        Object lock = new Object();
        AtomicInteger quite = new AtomicInteger();
        new Thread1(lock, quite).start();
        new Thread2(lock, quite).start();
    }

    /**
     * 线程1
     */
    static class Thread1 extends Thread {
        Object lock = new Object();
        AtomicInteger quiteFlag;

        public Thread1(Object lock, AtomicInteger quite) {
            this.quiteFlag = quite;
            this.lock = lock;
        }


        @Override
        public void run() {
            do {
                System.out.println("我是thread1");
                synchronized (lock) {
                    System.out.println("进入锁区间");
                    lock.notify();
                    try {
                        lock.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            } while (quiteFlag.incrementAndGet() < 500);
        }
    }

    /**
     * 线程2
     */
    static class Thread2 extends Thread {
        Object lock = new Object();
        AtomicInteger quiteFlag;

        public Thread2(Object lock, AtomicInteger quite) {
            this.quiteFlag = quite;
            this.lock = lock;
        }

        @Override
        public void run() {
            do {
                System.out.println("我是thread2");
                synchronized (lock) {
                    System.out.println("进入锁区间");
                    lock.notify();
                    try {
                        lock.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            } while (quiteFlag.incrementAndGet() < 500);
        }
    }
}
