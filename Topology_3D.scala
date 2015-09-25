import scala.io.StdIn.{readLine, readInt}
import scala.math._
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import java.io.PrintWriter

object Topology_3D
{
	def main(args: Array[String])
	{
		var n = 14564664;
		printf("Enter n: ");
		n = readLine.toInt;

		printf("Topology: 3d\n");

		val cuberoot = (pow(n, 1.0/3.0) + .001).toInt  //Cuberoot to determin nodes in one line
		println("Cuberot of " + n + " is: " + cuberoot)
		//var gridSize = 311331;
		val gridSize = pow(cuberoot,3).toInt;	//Total elements accomodated 
		println("Maximum Grid Size for " + n + " nodes : " + gridSize);

		/*Intializing all elements as alive by maintaing an array and setting all respective indices as 1
		After the node is killed, its value here is set to 0
		It wont be considered anyone's neighbor
		*/

		val nodes_aliveIndex = new Array[Int](gridSize);	

		for(i<-0 to gridSize-1)	
		{
			nodes_aliveIndex(i) = 1;
		}

		/*println("\nAll Nodes: \n")
		for(i<-0 to gridSize-1)
		{
			var dead_alive = "saadasd";
			if(nodes_aliveIndex(i) == 1)
			{
				dead_alive = "Alive"
			}
			else
			{
				dead_alive = "Dead"	
			}
			println( "\nNode " + i + " is: " + dead_alive);
		}*/

		//var nodes_plane = 1313345654;
		
		var check = 'c';	//to run do-while loop
		do
		{

			val nodes_plane = pow(cuberoot,2).toInt; 	//nodes_plane is the number of nodes on one face
			println("Nodes on one planar face: " + nodes_plane);
			println();

			var j = 123;

			/*
			The following for loop will check for all possible neighbors (6 for 3-D grid)

			(i-1): node immidiately before on the same edge
			(i+1): node immidiately after on the same edge
			(i-cuberoot): node immidiately above on the same face
			(i+cuberoot): node immidiately below on the same face
			(i-nodes_plane): parallel node facing immidiately front
			(i+nodes_plane): parallel node facing immidiately behind		
			*/
						
			for(i<-0 to gridSize-1)
			{
				if(nodes_aliveIndex(i) == 1)
				{
					//j = (i/nodes_plane).toInt;
					var count = 0; //Count Number of Neighbos for each node
					j = i/nodes_plane;
					
					var neighbor:String = i.toString
					
					if(i-1 >= 0)	
					{
						if( (i-1)/nodes_plane == j && (i-1) / cuberoot == i/cuberoot && (nodes_aliveIndex(i-1) == 1) )
						//if( (i-1)/nodes_plane == j && (nodes_aliveIndex(i-1) == 1) )
						{
							neighbor = neighbor + ", " + (i-1).toString;
							count += 1;
						}	
					}

					if((i+1) < ((j+1)*nodes_plane))
					{
						if( (i+1)/nodes_plane == j && (i+1) / cuberoot == i/cuberoot && (nodes_aliveIndex(i+1) == 1) )
						{
							neighbor = neighbor + ", " + (i+1).toString;
							count += 1;
						}
					}

					if((i-cuberoot) >= 0)
					{
						if( ((i-cuberoot)/nodes_plane).toInt == j && (nodes_aliveIndex(i-cuberoot) == 1) )
						{
							neighbor = neighbor + ", " + (i-cuberoot).toString;
							count += 1;
						}
					}

					if((i+cuberoot) < ((j+1)*nodes_plane))
					{
						if( ((i+cuberoot)/nodes_plane).toInt == j && (nodes_aliveIndex(i+cuberoot) == 1) )
						{
							neighbor = neighbor + ", " + (i+cuberoot).toString;
							count += 1;
						}					
					}

					if(i-nodes_plane >= 0)
					{
						if( ((i-nodes_plane)/nodes_plane).toInt == j-1 && (nodes_aliveIndex(i-nodes_plane) == 1) )
						{
							neighbor = neighbor + ", " + (i-nodes_plane).toString;
							count += 1;
						}	
					}

					if(i+nodes_plane < gridSize)
					{
						if( ((i+nodes_plane)/nodes_plane).toInt == j+1 && (nodes_aliveIndex(i+nodes_plane) == 1) )
						{
							neighbor = neighbor + ", " + (i+nodes_plane).toString;
							count += 1;
						}	
					}
					
					printf("\nNode: %-4d  Neighbor Count = %d 	Neighbors: %s ",i,count,neighbor);

				}
	
			}


			printf("\nDo you want to continue: (y/n): ");
			check = readChar;

			if(check == 'y')
			{
				var k = 23432;
				printf("\nEnter the node to be killed: ");
				k = readLine.toInt;

				nodes_aliveIndex(k)	 = 0;
				println("New Neighbors")
			}

			

		}
		while(check == 'y');
		//while(true);


	}
}




 		
