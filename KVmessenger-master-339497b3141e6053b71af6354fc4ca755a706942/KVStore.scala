package KVmessenger

import akka.actor.{Actor, Props}

import scala.concurrent.duration._

import scala.concurrent.ExecutionContext.Implicits.global


sealed trait KVStoreAPI
case class Put(key: BigInt, value: Any) extends KVStoreAPI
case class PutTemp(key: BigInt, value: Any) extends KVStoreAPI
case class Get(key: BigInt) extends KVStoreAPI
case class Acquire(key: BigInt, requester: Int) extends KVStoreAPI
case class Release(key: BigInt) extends KVStoreAPI
case class Abort(key: BigInt) extends KVStoreAPI

/**
 * KVStore is a local key-value store based on actors.  Each store actor controls a portion of
 * the key space and maintains a hash of values for the keys in its portion.  The keys are 128 bits
 * (BigInt), and the values are of type Any.
 */

class KVStore extends Actor {

  private val store = new scala.collection.mutable.HashMap[BigInt, Any]
  private val store_temp = new scala.collection.mutable.HashMap[BigInt, Any]
  private var locks = new scala.collection.mutable.HashMap[BigInt, Int]

  override def receive = {
    case Put(key, list) =>
      sender ! store.put(key, list)
    case PutTemp(key, list) =>
      sender ! store_temp.put(key, list)
    case Get(key) =>
      sender ! store.get(key)
    case Acquire(key, requester) =>
      sender ! acquire(key, requester)
    case Release(key) =>
      release(key)
    case Abort(key) => 
      abort(key)
  }


  def acquire (key: BigInt, requester: Int) : Int = {

    var members_list = List[Int]()
  	store_temp.put(key, members_list)

  	var result = requester
	  var owner = locks.getOrElse(key, -2) 

	if (owner == -2) {
      
      locks.put(key, requester)
    }
    else {
    	if (owner == -1 || owner == requester) {

    		locks.put(key, requester)
    	}
    	else {
    		result = owner
    	}
    }
    result
  }

  def release (key: BigInt) = {

  	val owner = locks.get(key) 
	if (!owner.isEmpty) {
      
  		locks.put(key, -1)

  	}
  }

  def abort (key: BigInt) = {

  	store_temp -= key
  }

}



object KVStore {
  def props(): Props = {
     Props(classOf[KVStore])
  }
}
