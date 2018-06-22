package net.sp.Main;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import net.sp.init.copy.initCopy;
import net.sp.init.wx.initWx_sub_dev;
import net.sp.init.wx.initWx_subscriber;

public class Main {
	public static void main(String []args) throws Exception{
		//initCopy.number(0);
		System.out.println("start!");
		long start = System.currentTimeMillis(); //����ʼ��¼ʱ��
		
		ExecutorService exec=Executors.newFixedThreadPool(2);
		//while(true){
		for(int i=10;i<100;i++){
			exec.execute(new initCopy(i,10000,10000));
			
		}
		exec.execute(new initWx_sub_dev(10000));
		exec.execute(new initWx_subscriber(10000));
		//System.out.println(start1-start);
		 
        exec.shutdown();
		
         
        while(true){
             if(exec.isTerminated()){
                 //System.out.println("Finally do something ");
                 long end = System.currentTimeMillis();
                 System.out.println("��ʱ: " + (end - start) + "ms");
                 
                 break;
             }

         }
	
		
	}
}
