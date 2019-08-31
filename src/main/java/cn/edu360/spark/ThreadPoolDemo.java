package cn.edu360.spark;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ThreadPoolDemo {

    public static void main(String[] args){
        //创建一个单线程的线程池
        //ExecutorService pool = Executors.newSingleThreadExecutor();

        //固定大小的线程池 5个
        //ExecutorService pool = Executors.newFixedThreadPool(5);
        //可缓冲的线程池(可以有多个线程)
        ExecutorService pool = Executors.newCachedThreadPool();


        //提交了10个线程
        for(int i=1;i<=20;i++){
        pool.execute(new Runnable() {
            @Override
            public void run() {
                //打印当前线程的名字
                System.out.println(Thread.currentThread().getName());
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println(Thread.currentThread().getName() + " is over");

            }
            });
        }
        System.out.println("all task is submitted");
//        pool.shutdownNow();

    }

}
