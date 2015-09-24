import akka.actor._
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import scala.concurrent._
import scala.concurrent.duration._
//import scala.concurrent.ExecutionContext.Implicits.global

/*
*manager case classes
*/
case class CreateCommunicationNetwork(numberOfNodes:Int, topology:String, algorithm: String)
case class Start(algorithm:String)
case class ImTransmitting(name:String)

/*
*worker case classes
*/
case class SetNeighbors(neighborsFromMaster:ArrayBuffer[ActorRef])
case class RemoveNeighbor(neighbor:ActorRef)
case object Rumor //for gossip communication
case object Gossip //for transmitting rumor
case object Finish //Node tells that its done. 



object Main extends App{
	override def main(args: Array[String])
	{
		try{
			var numberOfNodes:Int = args(0).toInt
			var topology:String = args(1)
			var algorithm:String = args(2)

			//wake up the manager and start it to create the network 
			val system = ActorSystem("NetworkManager")
			//val manager = system.actorOf(Props(new Manager(numberOfNodes,topology,algorithm)), name = "manager")
			val manager = system.actorOf(Props[Manager], name = "manager")
			manager ! CreateCommunicationNetwork(numberOfNodes, topology, algorithm)
		}
		catch{
			case e:Exception => println("Enter the Right Data, Make sure to add arguments: "+e.toString())
		}		
	}
	
}

class Manager extends Actor{
	var nodeList = new ArrayBuffer[ActorRef]()
	var time:Long = 0
	var numNodesLeft:Int = 0	
	var numNodesTransmitting:Int = 0;
	var numNodesDone:Int = 0;
	println("I've been Summoned Fellas")
	def receive = {
		case CreateCommunicationNetwork(numberOfNodes, topology, algorithm) => {
			numNodesLeft = numberOfNodes
			for(i <- 0 until numberOfNodes){
				nodeList += context.actorOf(Props[GossipPushSumSimulator], name = "node"+i)
			}
			topology match {
				case "full" => {
					for(i <- 0 until numberOfNodes) {
						nodeList(i)!SetNeighbors(nodeList - nodeList(i))
					}
				}

				case "line" => {
					var tempBuffer = new ArrayBuffer[ActorRef]()
					//set the first and last nodes neighbor (only one)
					nodeList(0)!SetNeighbors(tempBuffer+=nodeList(1))
					tempBuffer=new ArrayBuffer[ActorRef]()//tempBuffer.clear()
					nodeList(numberOfNodes-1)!SetNeighbors(tempBuffer+=nodeList(numberOfNodes - 2))

					//set all other neighbors. 
					for(i <- 1 to numberOfNodes - 2){
						tempBuffer=new ArrayBuffer[ActorRef]()
						tempBuffer += (nodeList(i-1), nodeList(i+1))
						nodeList(i)!SetNeighbors(tempBuffer)
					}
				}
				case "3D" => {
					//to be implemented
				}  
				case "Imp3D" =>  {
					//to be implemented
				} 
				case _ => {
					println("Unrecognized Communication network")
					context.system.shutdown()
				}
			}
			self ! Start(algorithm)
		}
		case Start(algorithm) => {
			//println("Implement " + algorithm)
			time = System.currentTimeMillis()
			algorithm match {
				case "gossip" => {
					var randomNode = Random.nextInt(nodeList.length)
					//start a rumor on a random node
					nodeList(randomNode) ! Rumor
				} 
				case "push-sum" => {
					//to be implemented
				}
				case _ => {
					println("Unrecognized Algorithm")	
					context.system.shutdown()
				} 
			}	
		}
		case ImTransmitting(name) => {
			//println(name + " is transmitting")
			numNodesTransmitting += 1
			numNodesDone += 1
		}
		case Finish => {
			numNodesLeft -= 1
			numNodesDone -= 1
			if(numNodesLeft == 0){
				println("Number of Nodes Transmitted: " + numNodesTransmitting)
				println("Number of Nodes Left: " + numNodesDone)
				println("Time to Spread Gossip: " + (System.currentTimeMillis() - time) + " miliseconds")
				context.system.shutdown()
			}
			else if(numNodesDone == 0){
				println("Number of Nodes Transmitted: " + numNodesTransmitting)
				println("Number of Nodes didn't receive the message: " + numNodesLeft)
				context.system.shutdown()
			}
		} 
	}
}


class GossipPushSumSimulator extends Actor{
	import context._
	var neighbors = new ArrayBuffer[ActorRef]()
	var rumorCount:Int = 0
	var manager:ActorRef = null
	var stepTime: Int = 10
	def receive = {
		case SetNeighbors(neighborsFromMaster) => {
			neighbors = neighborsFromMaster
			manager = sender
			//("self: "+self + " neighbors: " + neighbors)
			//context.system.shutdown()
		}
		case Rumor => {
			rumorCount += 1
			if(rumorCount == 1){
				/*
				*to transmit it the first time. 
				*the transmit calls will be asynchronous
				*from this point. 
				*/
				self ! Gossip
				manager ! ImTransmitting(self.path.name.toString())
			}
			
			//self ! TransmitRumor()
		}
		case Gossip => {
			//stop transmitting if rumor count is 10.
			if(rumorCount >= 10 || neighbors.length <= 0){
				//delete itself from all its neghbors.
				for( i <- 0 until neighbors.length) {
				 	neighbors(i) ! RemoveNeighbor(self)
				 } 
				 /*
				 *tell manager that i am done. and stop
				 */
				 manager ! Finish
				 context.stop(self)
			}
			else{
				//transmit the rumor to a random node
				var randomNode = Random.nextInt(neighbors.length)
				neighbors(randomNode) ! Rumor
				context.system.scheduler.scheduleOnce(stepTime milliseconds, self, Gossip)
			}
		}
		case RemoveNeighbor(neighbor) => {
			neighbors = neighbors - neighbor
		}
	}
}

