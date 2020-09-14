import java.io.*;


public class QueryOptimizer
{

     // calculation
   long ts_t1=20,ts_t2=40,ts_t3=100;
   long pages_t1=1000,pages_t2=500,pages_t3=2000;
   
   long pagesize=4096; // 4096 bytes
   long blocksize=100; // 100 pages= 409600 bytes
   static String joinMethod;
   static long tempPages;
   static long temptuplesperpage;
   static double totalquerycost;
   static double totalquerycost_RQ;
   static long tempPages_RQ;
   static long temp1Pages_RQ;
   static long temptuplesperpage_RQ;
   static long temp1tuplesperpage_RQ;
   static long mincost=0;
   static long mincost_RQ=0;
   static long totalCost = 0;
   static long totalCost_RQ=0;
   static long leastcost_RQ=0,leastcost_RQ2=0,leastcost_RQ3=0,leastcostfinal=0;
   

    static long tuplesperpage(long pagesize,long tuplesize)
    {
      return pagesize/tuplesize;
    }
   
   static long tuplesperblock(long blocksize,long tuplesperpage)
   {
      return blocksize*tuplesperpage;
   }
   
   static long numofblocks(long pagesintable,long blocksize)
   {
      return pagesintable/blocksize;
   }
   
   static long totaltuples(long pagesintable,long tuplesperpage)
   {
      return pagesintable*tuplesperpage;
   }
   static long costOfTNLJoin(long ltPages, long rtPages, long ltTuplsperPage)
   {
     long  TNLJoinCost = ltPages + ((ltTuplsperPage*ltPages)*rtPages);
       double leastCostOfJoin = TNLJoinCost;
       joinMethod = "TNLJoin";
       
      return TNLJoinCost;
	}
   static long costOfJoinOptimization(long ltPages, long rtPages, long ltTuplsperPage)
   {

   //Tuple Nested Loop Join:TNL: M (to scan R) + (pR * M) times * N (to scan S)
	   
      double TNLJoinCost = 0;
	   TNLJoinCost = ltPages + ((ltTuplsperPage*ltPages)*rtPages);
       double leastCostOfJoin = TNLJoinCost;
       joinMethod = "TNLJoin";
	

      //Page Nested Loop Join:PNL: M (to scan R) + M times *N 
   	double PNLJoinCost = 0;
   	PNLJoinCost = ltPages + (ltPages * rtPages);
   	if(leastCostOfJoin > PNLJoinCost)
      {
   		leastCostOfJoin = PNLJoinCost;
   		joinMethod = "PNLJoin";
   	}
   
      //Block Nested Loop Join: with Buffer memory B =50: BNJM
      //BNJM cost: M (to scan R) + (M/B) times *N 
   	double BNJMJoinCost = 0;
   	BNJMJoinCost = ltPages + ((ltPages/50) * rtPages);
           if(leastCostOfJoin > BNJMJoinCost)
           {
      		   leastCostOfJoin = BNJMJoinCost;
      		   joinMethod = "BNJMJoin";
   	      }

      //Sort Merge Join with buffer memory B=50: SMJM
      	double SMJMJoinCost = 0;
      	if(50 > Math.sqrt(ltPages))
         {
      		SMJMJoinCost = 3 *(ltPages + rtPages);
      	}
         else
         {
      		SMJMJoinCost = (2 * (ltPages + rtPages)*(1 + Math.ceil((Math.log10(ltPages/50))/(Math.log10(50-1))))) + ltPages + rtPages;
      	}
         
      	if(leastCostOfJoin > SMJMJoinCost)
         {
      		leastCostOfJoin = SMJMJoinCost;
      		joinMethod = "SMJMJoin";
      	}

      //Hash Join: HJM(Buffer = 50)
      		double HJMJoinCost = 0;
      	if(50 > Math.sqrt(ltPages))
         {
           HJMJoinCost= 3 *(ltPages + rtPages);
         }
         else
         {
             HJMJoinCost = (2 * (ltPages + rtPages)*(1 + Math.ceil((Math.log10(ltPages/50-1))/(Math.log10(50-1))))) + ltPages + rtPages;
         }
        
      	if(leastCostOfJoin > HJMJoinCost)
         {
      		leastCostOfJoin = HJMJoinCost;
      		joinMethod = "HJMJoin";
      	}

      //Hash Join: HJL(Buffer = 30)
      		double HJLJoinCost = 0;
      		if(30 > Math.sqrt(ltPages))
             {
               HJLJoinCost= 3 *(ltPages + rtPages);
             }
            else
            {
               HJLJoinCost = (2 * (ltPages + rtPages)*(1 + Math.ceil((Math.log10(ltPages/30-1))/(Math.log10(30-1))))) + ltPages + rtPages;
            }
      	              
            if(leastCostOfJoin > HJLJoinCost)
            {
      		   leastCostOfJoin = HJLJoinCost;
      		   joinMethod = "HJLJoin";
      	   }
      
         //Block Nested Loop Join: with Buffer memory B =30: BNJL
         //BNJM cost: M (to scan R) + (M/B) times *N 
         	
            double BNJLJoinCost = 0;
         	BNJLJoinCost = ltPages + ((ltPages/30) * rtPages);
         	if(leastCostOfJoin > BNJLJoinCost)
            {
         		leastCostOfJoin = BNJLJoinCost;
         		joinMethod = "BNJLJoin";
         	}
         
         //Sort Merge Join with buffer memory B=30: SMJL
         	double SMJLJoinCost = 0;
         	if(30 > Math.sqrt(ltPages))
            {
         		SMJLJoinCost = 3 *(ltPages + rtPages);
         	}
            else
            {
         		SMJLJoinCost = (2 * (ltPages + rtPages)*(1 + Math.ceil((Math.log10(ltPages/30))/(Math.log10(30-1))))) + ltPages + rtPages;
         	}

         	if(leastCostOfJoin > SMJLJoinCost)
            {
         		leastCostOfJoin = SMJLJoinCost;
         		joinMethod = "SMJLJoin";
         	}
            
           return (long)leastCostOfJoin;
}

 
 
public static void main(String[] args) throws Exception 
{

   long ltPages,rtPages,ltTuplsperPage;
   
   
   QueryOptimizer q=new QueryOptimizer();
   
   long tuplesperpage_t1=tuplesperpage(q.pagesize,q.ts_t1);
   long tuplesperblock_t1=tuplesperblock(q.blocksize,tuplesperpage_t1);
   long totaltuples_t1=totaltuples(q.pages_t1,tuplesperpage_t1);
   long numofblocks_t1=numofblocks(q.pages_t1,q.blocksize);
 
   long tuplesperpage_t2=tuplesperpage(q.pagesize,q.ts_t2);
   long tuplesperblock_t2=tuplesperblock(q.blocksize,tuplesperpage_t2);
   long totaltuples_t2=totaltuples(q.pages_t2,tuplesperpage_t2);
   long numofblocks_t2=numofblocks(q.pages_t2,q.blocksize);
   
   long tuplesperpage_t3=tuplesperpage(q.pagesize,q.ts_t3);
   long tuplesperblock_t3=tuplesperblock(q.blocksize,tuplesperpage_t3);
   long totaltuples_t3=totaltuples(q.pages_t3,tuplesperpage_t3);
   long numofblocks_t3=numofblocks(q.pages_t3,q.blocksize);

   File file = new File("Q1.txt"); 
   
   BufferedReader br = new BufferedReader(new FileReader(file)); 
  
   String st; 
   
   System.out.println("----------Q1 Query-------------------------------\n");
 
    while ((st = br.readLine()) != null)
    {
	
	      if(st.contains("Join"))
         {
		      String[] jstr =	st.split(" ");
            
            if((jstr[1].equals("t1") && jstr[2].equals("t2")) || (jstr[1]=="t2" && jstr[2]=="t1"))
            {
                         
               ltPages=q.pages_t1;
               rtPages=q.pages_t2;
               ltTuplsperPage= tuplesperpage_t1;
               mincost=costOfJoinOptimization(ltPages,rtPages,ltTuplsperPage);
               
                
               ltPages=q.pages_t2;
               rtPages=q.pages_t1;
               ltTuplsperPage= tuplesperpage_t2;
               
               if(costOfJoinOptimization(ltPages,rtPages,ltTuplsperPage) <  mincost)
               {
                  mincost=costOfJoinOptimization(ltPages,rtPages,ltTuplsperPage);
               }
               totalCost = totalCost + mincost;
               // new
                tempPages = (long)(0.1 * 0.2* ltPages * rtPages);
               long temptuplesize = 60;
                temptuplesperpage = tuplesperpage(q.pagesize,temptuplesize);
              // display
              System.out.println(st +" "+joinMethod+" cost="+mincost);

            }
            else if((jstr[1].equals("t1") && jstr[2].equals("t3")) || (jstr[1]=="t3" && jstr[2]=="t1"))
            {
               ltPages=q.pages_t1;
               rtPages=q.pages_t3;
               ltTuplsperPage= tuplesperpage_t1;
               mincost=costOfJoinOptimization(ltPages,rtPages,ltTuplsperPage);
               
               ltPages=q.pages_t3;
               rtPages=q.pages_t1;
               ltTuplsperPage= tuplesperpage_t3;
               
               if(costOfJoinOptimization(ltPages,rtPages,ltTuplsperPage) <  mincost)
               {
                  mincost=costOfJoinOptimization(ltPages,rtPages,ltTuplsperPage);
               }
                totalCost = totalCost + mincost;
                tempPages = (long)(ltPages * rtPages);
               long temptuplesize = 120;
                temptuplesperpage = tuplesperpage(q.pagesize,temptuplesize);

            }
            else if((jstr[1]=="t2" && jstr[2]=="t3") || (jstr[1]=="t3" && jstr[2]=="t2"))
            {
               ltPages=q.pages_t2;
               rtPages=q.pages_t3;
               ltTuplsperPage= tuplesperpage_t2;
               mincost=costOfJoinOptimization(ltPages,rtPages,ltTuplsperPage);
               
               ltPages=q.pages_t3;
               rtPages=q.pages_t2;
               ltTuplsperPage= tuplesperpage_t3;
               
               if(costOfJoinOptimization(ltPages,rtPages,ltTuplsperPage) <  mincost)
               {
                  mincost=costOfJoinOptimization(ltPages,rtPages,ltTuplsperPage);
               }
                totalCost = totalCost + mincost;
                tempPages = (long)(ltPages * rtPages);
                  long temptuplesize = 140;
                temptuplesperpage = tuplesperpage(q.pagesize,temptuplesize);
            }
            
            else if((jstr[1].equals("temp1") && jstr[2].equals("t3")) || (jstr[1].equals("t3") && jstr[2].equals("temp1")))
            {
               ltPages = tempPages;
               rtPages = q.pages_t3;
               ltTuplsperPage= temptuplesperpage;
               mincost = costOfTNLJoin(ltPages,rtPages,ltTuplsperPage);
                
               ltPages = q.pages_t3;
               rtPages = tempPages;
               ltTuplsperPage= tuplesperpage_t3;
               
               if(costOfTNLJoin(ltPages,rtPages,ltTuplsperPage) < mincost){
                    mincost = costOfTNLJoin(ltPages,rtPages,ltTuplsperPage);
               }
               
                totalCost = totalCost + mincost;
               
               System.out.println(st +" "+joinMethod+" cost="+mincost);

            }
           
	      }
         else if(st.contains("Project"))
         {
            // neglecting the cost by projection as it is same for all
            System.out.println(st +" "+"Neglecting this cost as it is same for all queries");

         }
         else if(st.contains("GroupBy"))
         {
         long groupbycost = 0;
            if(joinMethod.equals("SMJMJoin")||joinMethod.equals("SMJLJoin"))
            {
              groupbycost = (tempPages+q.pages_t3);
              totalCost += 2*(tempPages+q.pages_t3);
              
            }
            else
            {
                groupbycost = (tempPages+q.pages_t3);
               totalCost += 3*(tempPages+q.pages_t3);
            }
           System.out.println(st +" "+"cost = "+groupbycost);

         }
         
      }   
          //Query Processing Cost = Disk I/O Cost = # of Disk I/O * Disk Access Time(i.e 8 ms + 4 ms)
	             
            totalquerycost = totalCost*0.012;  // In milliseconds
            long hh =(long) totalquerycost/3600;       // In hours
            long mm = (long)(totalquerycost-(hh*3600))/60;
            long ss=(long)(totalquerycost-(hh*3600)-(mm*60));
            
            System.out.println("Total Disk I/O cost of Q1: "+totalCost);
             System.out.println("Query Processing Time: HH:MM:SS = "+hh+" hours"+":"+mm+" mins"+":"+ss+" sec");

     
     System.out.println("----------RQ1 Query-------------------------------\n");
   File file1 = new File("RQ1.txt"); 
   BufferedReader b = new BufferedReader(new FileReader(file1)); 
  
   String st1; 
   int flag=0; 
 
    while ((st1 = b.readLine()) != null)
    {
	      if(st1.contains("Join"))
         {
		      String[] jstr1 =	st1.split(" ");
            
            if((jstr1[1].equals("t1") && jstr1[2].equals("t3")) || (jstr1[1].equals("t3") && jstr1[2].equals("t1")))
            {
                              

               ltPages=q.pages_t1;
               rtPages=q.pages_t3;
               ltTuplsperPage= tuplesperpage_t1;
               
               mincost_RQ=costOfJoinOptimization(ltPages,rtPages,ltTuplsperPage);
               
               ltPages=q.pages_t3;
               rtPages=q.pages_t1;
               ltTuplsperPage= tuplesperpage_t3;
               
               if(costOfJoinOptimization(ltPages,rtPages,ltTuplsperPage) <  mincost_RQ)
               {
                  mincost_RQ=costOfJoinOptimization(ltPages,rtPages,ltTuplsperPage);
               }
                totalCost_RQ = totalCost_RQ + mincost_RQ;
                tempPages_RQ = (long)(0.20 * ltPages * rtPages);
                long temptuplesize = 120;
                temptuplesperpage_RQ = tuplesperpage(q.pagesize,temptuplesize);
                
               System.out.println(st1 +" "+joinMethod+" cost="+mincost_RQ);

            }
            
            else if((jstr1[1].equals("t2") && jstr1[2].equals("temp2")) || (jstr1[1].equals("temp2") && jstr1[2].equals("t2")))

            {
            
               ltPages=q.pages_t1;
               rtPages=q.pages_t2;
               ltTuplsperPage= tuplesperpage_t1;
               mincost_RQ = costOfJoinOptimization(ltPages,rtPages,ltTuplsperPage);
               leastcost_RQ=mincost_RQ;
               
                
               ltPages=q.pages_t2;
               rtPages=q.pages_t1;
               ltTuplsperPage= tuplesperpage_t2;
               
               if(costOfJoinOptimization(ltPages,rtPages,ltTuplsperPage) <  mincost_RQ)
               {
                  mincost_RQ = costOfJoinOptimization(ltPages,rtPages,ltTuplsperPage);
                  leastcost_RQ=mincost_RQ;
               }

                temp1Pages_RQ = (long)(0.15 * ltPages * rtPages);
                long temptuplesize = 60;
                temp1tuplesperpage_RQ = tuplesperpage(q.pagesize,temptuplesize);
                
                // temp1 temp to be joined
                
                  ltPages=temp1Pages_RQ;
                  rtPages=tempPages_RQ;
                  ltTuplsperPage= temp1tuplesperpage_RQ;
                  
                     long  mincost_RQ1 = costOfJoinOptimization(ltPages,rtPages,ltTuplsperPage);
                  
                  ltPages=tempPages_RQ;
                  rtPages=temp1Pages_RQ;
                  ltTuplsperPage= temptuplesperpage_RQ;
                  
                  if(costOfJoinOptimization(ltPages,rtPages,ltTuplsperPage) <  mincost_RQ1)
                  {
                      mincost_RQ1 = costOfJoinOptimization(ltPages,rtPages,ltTuplsperPage);
                  }
                  
                  
                  leastcost_RQ = leastcost_RQ + mincost_RQ1;
                  leastcostfinal = leastcost_RQ;
                  
                  // 2. tmp and t2 join with t1
                  
               ltPages = tempPages_RQ;
               rtPages = q.pages_t2;
               ltTuplsperPage= temptuplesperpage_RQ;
               mincost_RQ = costOfJoinOptimization(ltPages,rtPages,ltTuplsperPage);
               leastcost_RQ2=mincost_RQ;
               
                
               ltPages=q.pages_t2;
               rtPages=tempPages_RQ;
               ltTuplsperPage= tuplesperpage_t2;
               
               if(costOfJoinOptimization(ltPages,rtPages,ltTuplsperPage) <  mincost_RQ)
               {
                  mincost_RQ = costOfJoinOptimization(ltPages,rtPages,ltTuplsperPage);
                  leastcost_RQ2=mincost_RQ;
               }

               
               
                temp1Pages_RQ = (long)(0.10*ltPages * rtPages);
                long temp1tuplesize = 180;
                temp1tuplesperpage_RQ = tuplesperpage(q.pagesize,temp1tuplesize);
                
               
                
                  ltPages = temp1Pages_RQ;
                  rtPages= q.pages_t1;
                  ltTuplsperPage= temp1tuplesperpage_RQ;
                  
                     long  mincost_RQ2 = costOfJoinOptimization(ltPages,rtPages,ltTuplsperPage);
                  
                  ltPages=q.pages_t1;
                  rtPages=temp1Pages_RQ;
                  ltTuplsperPage= tuplesperpage_t1;
                  
                  if(costOfJoinOptimization(ltPages,rtPages,ltTuplsperPage) <  mincost_RQ1)
                  {
                      mincost_RQ2 = costOfJoinOptimization(ltPages,rtPages,ltTuplsperPage);
                  }
                  
                  leastcost_RQ2 += mincost_RQ2;
                  if(leastcost_RQ2 < leastcostfinal){
                     leastcostfinal = leastcost_RQ2;
                  }
               
               // perm 3
               
                ltPages = tempPages_RQ;
               rtPages = q.pages_t1;
               ltTuplsperPage= temptuplesperpage_RQ;
               mincost_RQ = costOfJoinOptimization(ltPages,rtPages,ltTuplsperPage);
               leastcost_RQ3=mincost_RQ;
                               
               ltPages=q.pages_t1;
               rtPages=tempPages_RQ;
               ltTuplsperPage= tuplesperpage_t1;
               
               if(costOfJoinOptimization(ltPages,rtPages,ltTuplsperPage) <  mincost_RQ)
               {
                  mincost_RQ = costOfJoinOptimization(ltPages,rtPages,ltTuplsperPage);
                  leastcost_RQ3=mincost_RQ;
               }
                temp1Pages_RQ = (long)(0.15 * ltPages * rtPages);
                long temp2tuplesize = 160;
                temp1tuplesperpage_RQ = tuplesperpage(q.pagesize,temp2tuplesize);
                
                // temp1 temp to be joined
                
                  ltPages=temp1Pages_RQ;
                  rtPages= q.pages_t2;
                  ltTuplsperPage= temp1tuplesperpage_RQ;
                  
                       long mincost_RQ3 = costOfJoinOptimization(ltPages,rtPages,ltTuplsperPage);
                  
                  ltPages=q.pages_t2;
                  rtPages=temp1Pages_RQ;
                  ltTuplsperPage= tuplesperpage_t2;
                  
                  if(costOfJoinOptimization(ltPages,rtPages,ltTuplsperPage) <  mincost_RQ1)
                  {
                      mincost_RQ3 = costOfJoinOptimization(ltPages,rtPages,ltTuplsperPage);
                  }           
               
                  leastcost_RQ3 += mincost_RQ3;
                  if(leastcost_RQ3 < leastcostfinal){
                     leastcostfinal = leastcost_RQ3;
                  }

                  System.out.println("\n"+st1 +" "+joinMethod+" Cost= "+leastcostfinal);
               }
            else if((jstr1[1].equals("t1") && jstr1[2].equals("temp1")) || (jstr1[1].equals("temp1") && jstr1[2].equals("t1")))
               {
                                    
                  System.out.print(st1+" "+"SMJMJoin");
               
               }            
               
             totalCost_RQ += leastcostfinal;
         }                            
         else if(st1.contains("Project"))
         {
                     System.out.println(st1 +" "+"Neglecting this cost as it is same for all queries");

         }
         else if(st1.contains("GroupBy"))
         {
          long groupbycost = 0;
         if(joinMethod.equals("SMJMJoin")||joinMethod.equals("SMJLJoin"))
            {
               if(flag==0)
               {
               groupbycost = 2*(q.pages_t1+q.pages_t3);
               totalCost_RQ += 2*(q.pages_t1+q.pages_t3);
               flag++;
              
               }
               else
               {
               groupbycost = 2*(temp1Pages_RQ+q.pages_t3);
               totalCost_RQ += 2*(temp1Pages_RQ+q.pages_t3);
               }
              
            }
            else
            {
               if(flag==0)
               {
                  totalCost_RQ += 3*(q.pages_t1+q.pages_t3);
                  flag++;
               
               }
               else
               {
                  totalCost_RQ += 3*(temp1Pages_RQ+q.pages_t3);
               }
            }
             System.out.println(st1 +" "+"cost = "+groupbycost);       
         }
     } 
     
     // RQ1 total query processing time
      totalquerycost_RQ = totalCost_RQ*0.012;  // In milliseconds
            long hh_RQ =(long) totalquerycost_RQ/3600;       // In hours
            long mm_RQ = (long)(totalquerycost_RQ-(hh_RQ*3600))/60;
            long ss_RQ =(long)(totalquerycost_RQ-(hh_RQ*3600)-(mm_RQ*60));
            
            System.out.println("Total Disk I/O cost of RQ1: "+totalCost_RQ);
             System.out.println("Query Processing Time: HH:MM:SS = "+hh_RQ+" hours"+":"+mm_RQ+" mins"+":"+ss_RQ+" sec");

System.out.println("\n-----------Best Query------------------");

if(totalCost > totalCost_RQ)
{
    System.out.println("The Best Query Plan is RQ1 as Cost of RQ1 is less than Q1");
}
else
{
   System.out.println("The Best Query Plan is Q1 as Cost of Q1 is less than RQ1");
}
   
}

}