package KVmessenger
import scala.swing._
import scala.swing.event._
import akka.actor.{Actor, ActorSystem, ActorRef, Props}
import scala.concurrent.duration._
import scala.concurrent.Await

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout

class UI (clientId : Int, numNodes: Int, numGroups: Int) extends MainFrame {
  def restrictHeight(s: Component) {
    s.maximumSize = new Dimension(Short.MaxValue, s.preferredSize.height)
  }

  val typeField = new TextArea { rows = 3; lineWrap = true; wordWrap = true }
  val typeField_SP = new ScrollPane(typeField)

  val messageField = new TextArea { rows = 8; lineWrap = true; wordWrap = true }
  val messageField_SP = new ScrollPane(messageField)

  var clients_list = List[String]()
  var i = 0
  for (i <- 0 to numNodes - 1) {

    if (i != clientId) {

      clients_list ::= i.toString
    }
  }
  val clients = new ComboBox(clients_list) 
  restrictHeight(clients)

  //

  var groups_list = List[String]()
  i = 0
  for (i <- 0 to numGroups - 1) {

      groups_list ::= i.toString
  }
  groups_list ::= "private"
  val groups = new ComboBox(groups_list) 
  restrictHeight(groups)

  var currentGroup = -1

  var dates_list = List[String]()
  dates_list ::= "April 17th 5:00pm, 2017"
  dates_list ::= "April 18th 8:00am, 2017"
  dates_list ::= "April 18th 9:00am, 2017"
  dates_list ::= "April 18th 10:00am, 2017"
  dates_list ::= "April 18th 11:00am, 2017"
  var dates = new ComboBox(dates_list)
  restrictHeight(dates)

  var added = List[Int]()

  added ::= clientId

  def initializePanel () {

    title = "Client " + clientId 
    preferredSize = new Dimension(500, 500)

    contents = new BoxPanel(Orientation.Vertical) {

      contents += new BoxPanel(Orientation.Horizontal) {
        contents += new Label("Clients")
        contents += Swing.HStrut(20)
        contents += clients
        contents += Button("add") { 

          if (added.contains(clients.item.toInt)) {
            displayMessage("*******Client " + clients.item + " is already in invite list*******")

          }
          else {
            added ::= clients.item.toInt
            displayMessage("*******Client " + clients.item + " is added to invite list*******")
          }
          
        }
        contents += Swing.HStrut(10)
        contents += new Label("Groups")
        contents += Swing.HStrut(5)
        contents += groups
        contents += Swing.HStrut(5)
        contents += Button("join") { 

          if (groups.item == "private") {
            joinGroup(-1)
          }
          else {
            joinGroup(groups.item.toInt)
          }
          displayMessage("*******Group " + groups.item + "*******")
        }
      }

      //meeting 
    
      contents += new BoxPanel(Orientation.Horizontal) {
        
        contents += new Label("Date")
        contents += Swing.HStrut(5)
        contents += dates
        contents += Swing.HStrut(5)
        contents += Button("request") {

          if (added.length > 1) {
          	invite(added, dates.item, dates.selection.index)

          	displayMessage("*******Meeting request sent to client" + added(1) + " *******")

          	added = List[Int]()
          	added ::= clientId
          }

        }
      }

      //message
      contents += Swing.VStrut(5)
      contents += new Label("Messages")
      contents += Swing.VStrut(3)
      contents += messageField_SP
      contents += Swing.VStrut(5)
      
      contents += new Label("Type your message here")
      contents += typeField_SP
      contents += Swing.VStrut(5)
      
      contents += new BoxPanel(Orientation.Horizontal) {
        
        contents += Button("Send") {  

          if (typeField.text.length() != 0) {  

            if (currentGroup == -1) {
              
              displayMessage("Me: " + typeField.text)
              sendMessage(clients.item.toInt, "Client " + clientId + ": "  + typeField.text) 
            }

            else {

              displayMessage("*Broadcast* Me: " + typeField.text)
              sendMessage(clientId, "*Broadcast* Client " + clientId + ": "  + typeField.text)
            }
          }
        }

        contents += Button("Close") {  sys.exit(0) }
      }

      for (e <- contents)
        e.xLayoutAlignment = 0.0
      border = Swing.EmptyBorder(10, 10, 10, 10)
    }

    listenTo(messageField)

  }

  initializePanel()

  //

  var servers : Option[Seq[ActorRef]] = None

  def setServers (clts: Option[Seq[ActorRef]]) {

    servers = clts
  }

  //

  def displayMessage (message : String) {
    
    messageField.append(message+ '\n');
  }

  def sendMessage (targetId : Int, msg : String) {
    
    servers.get(targetId) ! msg

  }

  def joinGroup (groupId: Int) {

    servers.get(clientId) ! JoinGroup(groupId)

    currentGroup = groupId
  }

  def invite (targetIds: List[Int], date: String, dateIndex: Int) {
    servers.get(targetIds(1)) ! Invite(date, clientId, dateIndex, targetIds)
  }

  def acceptInvite (date: String, requester: Int) : Int = {
    var res = Dialog.showConfirmation(contents.head, 
              "Do you want to accept meeting from client " + requester + " for " + date, 
              optionType=Dialog.Options.YesNo,
              title="client " + clientId)

    if (res == Dialog.Result.Ok)  {

      return 1
    }
    return -1
  }

}


object GUI {
 
}