import akka.actor._
import scala.util.Random
import scala.math._

object project2bonus {
  case class GossipProtocol(allActor : List[ActorRef])
  case class pushsumProtocol(allActor : List[ActorRef])


  def main(args: Array[String]) =
  {
    //Accepts three arguments and create akka system
    if(args.length != 3)
    {
      println("Please enter input in format as : number_of_nodes type_of_topology algorithm")
    }
    else{
      //first argument is number of total nodes in topology
      var numOfnodes : Int = args(0).toInt

      //second argument is type of topology
      var topo : String = args(1)

      //third argument is protocol to be used
      var protocol : String = args(2)

      //select random node to start gossiping since rumor can begin at any node
      var rumorBegin = Random.nextInt(numOfnodes)

      //create an actor system
      val system = ActorSystem("gossip")
      var allActor:List[ActorRef] = Nil
      if((topo.equalsIgnoreCase("line")) || (topo.equalsIgnoreCase("full"))){
        //for calculating purpose we define base case
        if(numOfnodes<10){
          numOfnodes = 10
        }
      }
      else if((topo.equalsIgnoreCase("3D")) || (topo.equalsIgnoreCase("Imp3D")))
      {
        //to make perfect 3 dimensional grid, basic condition is to have number of nodes to be perfect cube
        //taking value which user has entered and converting it to nearest perfect cube value
        var temp = math.cbrt(numOfnodes);
        //rounding off to nearest upper interger value
        var per3D = temp.ceil.toInt;
        numOfnodes = math.pow(per3D, 3).toInt;
      }
      else{
        println("enter proper topology")
        System.exit(0)
      }

      //generating master system with initial rumor point and all number of nodes
      var startMaster = system.actorOf(Props(new Master(rumorBegin,numOfnodes)))

      //allActor is array of nodes of given topology
      for(i<-1 to numOfnodes){
        allActor ::= system.actorOf(Props(new Nodes(topo)))
      }

      //after getting all nodes from topology, we are assigning neighbors based to topology
      for (i <- 0 to numOfnodes-1) {
        allActor(i) ! BuildTopology(allActor,startMaster,(i+1).toDouble)
      }

      //check which topology has entered and call method based on that
      if (protocol.equalsIgnoreCase("gossip"))
        startMaster ! GossipAlgo(allActor)
      else if(protocol.equalsIgnoreCase("pushsum"))
        startMaster ! pushsumAlgo(allActor)
    }
  }

  case class terminate_proto2()
  case class GossipAlgo(allActor : List[ActorRef])
  case class pushsumAlgo(allActor : List[ActorRef])
  case class worktable(var  newCount : Int, var i : Int)

  class Master(rumorBegin : Int,numOfnodes : Int) extends Actor
  {
    var arrayIndex:Int = 0
    val count = new Array[Int](numOfnodes)
    //used to keep track whether all nodes received the message at least once
    var tracker = 0
    var InitTime : Long = _
    def receive =
    {
      //initial point to start rumor at "rumorBegin" with start timestamp for gossip protocol
      case GossipAlgo(arActors : List[ActorRef]) =>
        var startNode = rumorBegin
        InitTime = System.currentTimeMillis;
        arActors(startNode) ! RunGossip(startNode)

      //initial point to start rumor at "rumorBegin" with start timestamp for pushsum protocol
      case pushsumAlgo(arActors : List[ActorRef]) =>
        var startNode = rumorBegin
        InitTime = System.currentTimeMillis;
        arActors(startNode) ! RunPushSum(0.0,0.0,startNode)

      /*first it will update count of node who has received message and then
       * it will check if its first time that node has received message it will increase count of tracker.*/

      case worktable(newCount : Int, arrayIndex :Int)=>
        while(tracker<=arrayIndex){
          count(arrayIndex) = newCount
          if(count(arrayIndex) == 1)
            tracker = tracker + 1
        }

        /*  if tracker becomes equal to number of nodes, it means all nodes have received the message at least once.
        * and process will terminate*/
        if(tracker == numOfnodes)
        {
          print("Time taken to converge gossip protocol : ")
          println(((System.currentTimeMillis - InitTime)/1000.0) + "seconds")
          System.exit(0)
        }

      //terminate pushsum protocol after converging s/w
      case terminate_proto2() =>
        println("Time taken to converge pushsum protocol : ")
        println(((System.currentTimeMillis - InitTime)/1000.0) + "seconds")
        System.exit(0)

    }

  }

  case class RunGossip(var i : Int)
  case class RunPushSum(var s : Double, var w : Double, var i: Int)
  case class BuildTopology( var arActors : List[ActorRef], count: ActorRef, s_init : Double)

  class Nodes(topo : String) extends Actor
  {
    var nodes: List[ActorRef] = Nil
    var count  = 0
    var Reference : ActorRef = _

    //list of all neighbor of current node
    var neighbour:List[Int] = Nil

    //to have track of neighbor who are dead
    var failure_neighbour:List[Int] = Nil
    var s_old : Double = 0.0
    var w_old : Double = 0.0
    var s : Double = 0.0
    var w : Double = 1.0

    //convergence criteria
    var convergentPoint : Double = pow(10,-10)
    var diff : Double = 0.0
    /*Here all the values are stored in different variables for each of the actors. */

    def receive =
    {
      //Initializing Actor list and neighbour list for each actor
      case BuildTopology(allActor : List[ActorRef], count : ActorRef, s_init : Double) =>
        nodes = allActor
        Reference = count
        s = s_init
        w = 1.0
        //array start with index 0, hence 1 is substracted
        var curr_node = (s_init - 1.0).toInt

        //calculating neighbour list according to the topology
        var mod:Int = 0
        //Build line topology
        if (topo.equalsIgnoreCase("line"))
        {
          if(curr_node > 0)
            neighbour ::= curr_node - 1
          if(curr_node < allActor.length -1)
            neighbour ::=  curr_node + 1
        }

        //build full topology
        if (topo.equalsIgnoreCase("full"))
        {
          //all points are connected to every other point
          for(mod <- 0 to allActor.length-1){
            if(curr_node!=mod){
              neighbour ::= mod
            }
          }
        }

        //Creating 3D and imperfect 3D grid
        if ((topo.equalsIgnoreCase("3D")) || (topo.equalsIgnoreCase("Imp3D")))
        {

          mod = math.cbrt(allActor.length).toInt  //to make it perfect 3D, mod now contains cube root value of input
          if(allActor.length < 8){
            if(curr_node % (2*mod) == 0){
              neighbour ::= curr_node + 1
              neighbour ::= curr_node + 2
              if(curr_node - 4 == 0)
                neighbour ::= curr_node - 4
              else
                neighbour ::= curr_node + 4
            }
            else{
              if(curr_node % (2*mod) == 1){
                neighbour ::= curr_node - 1
                neighbour ::= curr_node + 2

              }

              if(curr_node % (2*mod) == 2){
                neighbour ::= curr_node + 1
                neighbour ::= curr_node - 2

              }

              if(curr_node % (2*mod) == 3){
                neighbour ::= curr_node - 1
                neighbour ::= curr_node - 2

              }
              if(curr_node - (2*mod) < 0)
                neighbour ::= curr_node + 4
              else
                neighbour ::= curr_node - 4
            }
          }


          else{
            if(curr_node % mod == 0){
              neighbour ::= curr_node + 1

              if(curr_node%(mod*mod)<=(mod-1)){
                neighbour ::= curr_node + mod
              }
              else if(curr_node%(mod*mod)>=((mod*mod)-mod))
                neighbour ::= curr_node - mod
              else{
                neighbour ::= curr_node - mod
                neighbour ::= curr_node + mod
              }


              if(curr_node < (mod*mod))
                neighbour ::= curr_node + (mod*mod)
              else if(((mod*mod*(mod-1)) <= curr_node) && (curr_node  < (mod*mod*mod)))
                neighbour ::= curr_node - (mod*mod)
              else{
                neighbour ::= curr_node + (mod*mod)
                neighbour ::= curr_node - (mod*mod)
              }

            }else if((curr_node+1) % mod == 0){
              if(curr_node%(mod*mod)<=mod)
                neighbour ::= curr_node + mod
              else if (curr_node%(mod*mod)>((mod*mod)-mod))
                neighbour ::= curr_node - mod
              else{
                neighbour ::= curr_node + mod
                neighbour ::= curr_node - mod
              }

              neighbour ::= curr_node - 1
              if(curr_node < (mod*mod))
                neighbour ::= curr_node + (mod*mod)
              else if(((mod*mod*(mod-1)) <= curr_node) && (curr_node  < (mod*mod*mod)))
                neighbour ::= curr_node - (mod*mod)
              else{
                neighbour ::= curr_node + (mod*mod)
                neighbour ::= curr_node - (mod*mod)
              }

            }
            else{

              neighbour ::= curr_node + 1
              neighbour ::= curr_node - 1
              if(curr_node%(mod*mod)<=(mod-1)){
                neighbour ::= curr_node + mod
              }
              else if(curr_node%(mod*mod)>=((mod*mod)-mod))
                neighbour ::= curr_node - mod
              else{
                neighbour ::= curr_node - mod
                neighbour ::= curr_node + mod
              }


              if(curr_node < (mod*mod))
                neighbour ::= curr_node + (mod*mod)
              else if(((mod*mod*(mod-1)) <= curr_node) && (curr_node  < (mod*mod*mod)))
                neighbour ::= curr_node - (mod*mod)
              else{
                neighbour ::= curr_node + (mod*mod)
                neighbour ::= curr_node - (mod*mod)
              }
            }
          }

        }

        var random : Int = 0
        if (topo.equalsIgnoreCase("Imp3D"))
        {
          var random = Random.nextInt(allActor.length)
          //checks if randomly selected node is same or already part of its neighbor
          while(random == curr_node || neighbour.contains(random))
          {
            random = Random.nextInt(allActor.length)
          }
          neighbour ::= random
          //println(i +" "+neighbour);
        }


      //Gossip protocol
      case RunGossip(i) =>
        var j : Int = 0

        //checks if any of the actor received rumor 10 times, if any one did, it will kill the particular actor
        if(count < 10)
        {
          count+=1
          Reference ! worktable(count, i)
          for (j <-1 until 3)
          {
            var randomNumber = Random.nextInt(neighbour.length)
            if(!failure_neighbour.contains(randomNumber)){
              var next = neighbour(randomNumber)
              try{
                nodes(neighbour(randomNumber))! RunGossip(next)
              }catch{
                case e: Exception =>
                  failure_neighbour ::= next
                  randomNumber = Random.nextInt(neighbour.length)
                  next = neighbour(randomNumber)
                  nodes(neighbour(randomNumber))! RunGossip(next)
              }
            }else{
              var next = neighbour(randomNumber)
              nodes(next)!RunPushSum(s,w,next)
            }

          }
        }
        else  if (count==10)
        {
          self ! PoisonPill
        }

      //PushSum protocol
      case RunPushSum(s_new,w_new,act_num) =>
        s = s + s_new
        w = w + w_new
        //to make sure value doesn't change for three times to make sure of convergence
        if(count<3)
        {

          if(w_old!=0)
          {
            diff = Math.abs((s_old/w_old) - (s/w))
            if(diff <= convergentPoint)
            {
              count+=1
            }
            else
              count =0
          }
        }
        else
        {
          var n = s/w
          //Printing the final average value of all the current nodes
          println("s/w value after converging for actor " + act_num + "is " + n)
          Reference ! terminate_proto2()
          self ! PoisonPill
        }

        s_old = s
        w_old = w

        //for next cycle
        s -= s/2
        w -= w/2
        var randomNumber = Random.nextInt(neighbour.length)
        if(!failure_neighbour.contains(randomNumber)){
          var next = neighbour(randomNumber)
          try{
            nodes(next)!RunPushSum(s,w,next)
          }catch{
            case e: Exception =>
              failure_neighbour ::= next
              randomNumber = Random.nextInt(neighbour.length)
              next = neighbour(randomNumber)
              nodes(next)!RunPushSum(s,w,next)
          }

        }else{
          randomNumber = Random.nextInt(neighbour.length)
          var next = neighbour(randomNumber)
          nodes(next)!RunPushSum(s,w,next)
        }
    }
  }

}