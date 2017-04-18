package KVmessenger

import akka.actor.{Actor, ActorSystem, ActorRef, Props}
import akka.event.Logging
import scala.collection.mutable.ArrayBuffer
import scala.swing._

sealed trait GroupServiceAPI
case class JoinGroup(groupId : Int) extends GroupServiceAPI
case class Invite(date: String, requester: Int, dateIndex: Int, participantsList: List[Int]) extends GroupServiceAPI
case class Accepted(participantsList: List[Int]) extends GroupServiceAPI
case class Taken() extends GroupServiceAPI
case class Rejected(participantsList: List[Int]) extends GroupServiceAPI

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
    

  	def receive() = {
      case Prime() =>
    
      case Command() =>

      case View(e) =>
        endpoints = Some(e)
        ui.setServers(endpoints)
      case JoinGroup(groupid) =>
        join(groupid)
      case Invite(date, requester, dateIndex, participantsList) =>
        invite(date, requester,dateIndex, participantsList)
      case Accepted(participantsList: List[Int]) =>
      	accept(participantsList)
      case Taken() =>
      	ui.displayMessage("******Time slot was taken******")
      case Rejected(participantsList) => 
      	reject(participantsList)
      case msg =>
        processMessage(msg.asInstanceOf[String])
  	}


  	private def reject (participantsList: List[Int]) {
  		ui.displayMessage("******Meeting was rejected******")

  		var index = participantsList.indexOf(myNodeID)

  		if (index > 0) {

  			endpoints.get(participantsList(index - 1)) ! Rejected(participantsList)
  		}
  	}

  	private def accept(participantsList: List[Int]) {
  		ui.displayMessage("******Meeting was confirmed******")

  		var index = participantsList.indexOf(myNodeID)

  		if (index > 0) {

  			endpoints.get(participantsList(index - 1)) ! Accepted(participantsList)
  		}
  	}


  private def invite (date: String, requester: Int, dateIndex: Int, participantsList: List[Int]) : Any = {

    ui.displayMessage("*******Meeting request from client "  + requester + " for " + date + "*******")

    //check to see if the time slot was taken
    var cell_current = readMeeting(dateIndex)
    var elapse = 0
    while (!cell_current.isDefined) {

        if (elapse == 100) {

          cell_current = readMeeting(dateIndex)
          elapse = 0
        }
        elapse += 1
    }

    
    var cell_currentL = cell_current.get

    var i = -1
    var isTaken = 0
    for (i <- cell_currentL) {
    	isTaken = 1
    }

    //if time slot was taken 

    if (isTaken == 1) {
    	ui.displayMessage("******Time slot was taken******")
    	endpoints.get(requester) ! Taken()
    	groupstoreMeeting.begin()
    	return
    }
    
    //if time slot was not taken
    
    val res =ui.acceptInvite(date, requester)
    if (res == 1) {

      	var plen = participantsList.length

      	//if current client is the last one

      	if (participantsList(plen - 1) == myNodeID) {

      		writeMeeting(dateIndex, participantsList)
      		groupstoreMeeting.begin()
      		ui.displayMessage("******Meeting was confirmed******")
      		endpoints.get(requester) ! Accepted(participantsList)
      	}

      	else {
      	
      		groupstoreMeeting.begin()
      		var currIndex = participantsList.indexOf(myNodeID)
      		endpoints.get(participantsList(currIndex + 1)) ! Invite(date, myNodeID, dateIndex, participantsList)
      	}
    }

    else {
    	groupstoreMeeting.begin()
    	ui.displayMessage("******Meeting was rejected******")
    	endpoints.get(requester) ! Rejected(participantsList)
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
    val result = groupstoreMeeting.directRead(key)
    if (result.isEmpty) None else
      Some(result.get.asInstanceOf[List[Int]])
  }

  def writeMeeting(key: BigInt, value: List[Int]): Option[List[Int]] = {
    val result = groupstoreMeeting.directWrite(key, value)
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

