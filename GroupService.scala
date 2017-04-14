package KVmessenger

import akka.actor.{Actor, ActorSystem, ActorRef, Props}
import akka.event.Logging
import scala.collection.mutable.ArrayBuffer
import scala.swing._

sealed trait GroupServiceAPI
case class JoinGroup(groupId : Int) extends GroupServiceAPI
case class Invite(date: String, requester: Int, dateIndex: Int) extends GroupServiceAPI
case class Accepted(accepter: Int) extends GroupServiceAPI

class GroupServer(val myNodeID: Int, 
  val numNodes: Int, 
  storeServersMeeting: Seq[ActorRef],
  storeServers: Seq[ActorRef], 
  BurstSize: Int) extends Actor {

	val generator = new scala.util.Random
  	val groupstore = new KVClient(storeServers, myNodeID)
    val groupstoreMeeting = new KVClient(storeServersMeeting, myNodeID)
  	var endpoints: Option[Seq[ActorRef]] = None
  	val log = Logging(context.system, this)
  	var GroupId = -1

  	val ui = new UI(myNodeID, numNodes, 2)
   	ui.visible = true

    var pending = ArrayBuffer[Int]()
    var accepted = ArrayBuffer[Int]()
    

  	def receive() = {
      case Prime() =>
    
      case Command() =>

      case View(e) =>
        endpoints = Some(e)
        ui.setServers(endpoints)
      case JoinGroup(groupid) =>
        join(groupid)
      case Invite(date, requester, dateIndex) =>
        invite(date, requester,dateIndex)
      case Accepted(accepter) =>
        ui.displayMessage("*client " + accepter + " accepted.")
      case msg =>
        processMessage(msg.asInstanceOf[String])
      	
      
  	}



  private def invite (date: String, requester: Int, dateIndex: Int) = {

    ui.displayMessage("*******Meeting request from client "  + requester + " for " + date + "*******")
    
    val res =ui.acceptInvite(date, requester)
    if (res == 1) {
    
      endpoints.get(requester) ! accepted(myNodeID)

      var cell_current = readMeeting(dateIndex)
      var elapse = 0
      while (!cell_current.isDefined) {

        if (elapse == 100) {

          cell_current = readMeeting(dateIndex)
          elapse = 0
        }

        elapse += 1
      }
    }



  }

	private def join (groupid: Int) : Any = {

    var elapse = 0

    //just leave group
    if (groupid == -1) {

      if (GroupId != -1) {

        var cell_current = read(GroupId)
        while (!cell_current.isDefined) {

          if (elapse == 100) {

            cell_current = read(groupid)
            elapse = 0
          }

          elapse += 1
        }

        var cell_currentL = cell_current.get
        var l = cell_currentL.filter(_ != myNodeID)
        write(GroupId, l)

        groupstore.begin()
        GroupId = groupid 
      }
      return
    }

    //just join group

    var cell_future = read(groupid)

    if (GroupId == -1) {

      while (!cell_future.isDefined) {

        if (elapse == 100) {

          cell_future = read(groupid)
          elapse = 0
        }

        elapse += 1
      }

       var cell_futureL = cell_future.get
       if(!cell_futureL.contains(myNodeID)){
        
        var l = myNodeID :: cell_futureL
        write(groupid, l)

        groupstore.begin()
        GroupId = groupid 
      }
    }

    //leave then join
    else {

      elapse = 0

		  var cell_current = read(GroupId)
      while (!cell_current.isDefined) {

        if (elapse == 100) {

          cell_current = read(GroupId)
          elapse = 0
        }

        elapse += 1
      }

    //

      elapse = 0 

      cell_future = read(groupid)

      while (!cell_current.isDefined || !cell_future.isDefined) {

        if (elapse == 100) {

          cell_future = read(groupid)
          cell_current = read(GroupId)
          elapse = 0
        }

        elapse += 1 
      }

      //
      var cell_currentL = cell_current.get 
      var cell_futureL = cell_future.get

      if(!cell_futureL.contains(myNodeID)){
        
        var l = myNodeID :: cell_futureL
        write(groupid, l)

        l = cell_currentL.filter(_ != myNodeID)
        write(GroupId, l)

        groupstore.begin()
        GroupId = groupid 
      }
    }
	}



  private def processMessage(message : String) : Any = {


    if (message.substring(0, 11) == "*Broadcast*") { ///todo 


      var cell = read(GroupId)
      var elapse = 0
      while (!cell.isDefined) {

        if (elapse == 100) {

          cell = read(GroupId)
          elapse = 0
        }

        elapse += 1
      } 

      var ID_list = cell.get 

      for (ID <- ID_list) {

        if (ID != myNodeID) {

          endpoints.get(ID) ! ("*" + message) 
        }
      }

      write(GroupId, ID_list)
      groupstore.begin()
    }
    else { ///thisp part is fine

      var mesg = message
      if (message.substring(0, 2) == "**") {
        mesg = message.substring(1)
      }
      ui.displayMessage(mesg)
    }
  }

	
	def read(key: BigInt): Option[List[Int]] = {
    val result = groupstore.directRead(key)
    if (result.isEmpty) None else
      Some(result.get.asInstanceOf[List[Int]])
  }

  def write(key: BigInt, value: List[Int]): Option[List[Int]] = {
    val result = groupstore.directWrite(key, value)
    if (result.isEmpty) None else
      Some(result.get.asInstanceOf[List[Int]])
  }

  def readMeeting(key: BigInt): Option[List[Int]] = {
    val result = groupstore.directRead(key)
    if (result.isEmpty) None else
      Some(result.get.asInstanceOf[List[Int]])
  }

  def writeMeeting(key: BigInt, value: List[Int]): Option[List[Int]] = {
    val result = groupstore.directWrite(key, value)
    if (result.isEmpty) None else
      Some(result.get.asInstanceOf[List[Int]])
  }
}



object GroupServer {
  def props(myNodeID: Int, 
    numNodes: Int, 
    storeServersMeeting: Seq[ActorRef], 
    storeServers: Seq[ActorRef], 
    burstSize: Int): Props = {
    Props(classOf[GroupServer], myNodeID, numNodes, storeServersMeeting, storeServers, burstSize)
  }
}

