package KVmessenger

import scala.concurrent.duration._
import scala.concurrent.Await

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout

import scala.collection.mutable.ListBuffer


class AnyMap extends scala.collection.mutable.HashMap[BigInt, Any]

/**
 * KVClient implements a client's interface to a KVStore, with an optional writeback cache.
 * Instantiate one KVClient for each actor that is a client of the KVStore.  The values placed
 * in the store are of type Any: it is up to the client app to cast to/from the app's value types.
 * @param stores ActorRefs for the KVStore actors to use as storage servers.
 */

class KVClient (stores: Seq[ActorRef], clientId: Int) {
  private val cache = new AnyMap
  private val dirtyset = new AnyMap
  implicit val timeout = Timeout(2 seconds)

  private val acquired_lock = ListBuffer[BigInt] ()

  import scala.concurrent.ExecutionContext.Implicits.global


  /** Cached read */
  def read(key: BigInt): Option[Any] = {
    var value = cache.get(key)
    if (value.isEmpty) {
      value = directRead(key)
      if (value.isDefined)
        cache.put(key, value.get)
    }

    value
  }


  /** Cached write: place new value in the local cache, record the update in dirtyset. */
  def write(key: BigInt, value: Any) = {
    cache.put(key, value)
    dirtyset.put(key, value)
  }


  /** Direct read, bypass the cache: always a synchronous read from the store, leaving the cache unchanged. */
  def directRead(key: BigInt): Option[Any] = {

  	var result = acquire(key)

  	if (result != clientId) {
  		//acquire lock failed, return none
  		None
  	} 
  	else {
    	val future = ask(route(key), Get(key)).mapTo[Option[Any]]
    	var future_result = Await.result(future, timeout.duration)
    	if (!future_result.isDefined) {
    		//The account does not existed, create one
        var members_list = List[Int]()
    		directWrite(key, members_list)
    	}
    	else {
    		future_result
    	}
	}
  }

  /** Direct write, bypass the cache: always a synchronous write to the store, leaving the cache unchanged. */
  def directWrite(key: BigInt, value: Any) = {

    val future = ask(route(key), Put(key,value)).mapTo[Option[Any]]
    Await.result(future, timeout.duration)
  }

  //
  def directWriteTemp(key: BigInt, value: Any) = {

    val future = ask(route(key), PutTemp(key,value)).mapTo[Option[Any]]
    Await.result(future, timeout.duration)
  }

  //

  def release() = {
  	for (lock <- acquired_lock) {
  		route(lock) ! Release(lock)
  		acquired_lock -= lock
  	}
  }

  //

  def reset () = {

  	release()
  	cache.clear()
  	dirtyset.clear()
  }

  //
  def acquire (key: BigInt) : Any = {

  	var result = ask(route(key), Acquire(key, clientId))
  	var owner = Await.result(result, timeout.duration)

  	//lock is currently hold by another client, release all locks and retry later
  	if (owner != clientId) {

  		reset()

  	}
  	//lock acquired successfully
  	else {
  		acquired_lock += key
  	}

  	owner
  }

  /*It ask KVStores to write data into their storage. If anything goes wrong, abort and rollback
  Otherwise, everything will be committed. After either abort or commit, cache, dirtyset and locks
  will be reset*/

  def begin () = {

  	var should_abort = false

	for ((key, v) <- dirtyset) {
      val result = directWriteTemp(key, v)

      if (!result.isDefined) {

      	should_abort = true

      }
	}

	if (should_abort) {

		println(s"Client $clientId aborted")
		abort()
	}

	
	else {
		println(s"Client $clientId committed")
		commit()
	}

	reset()
  }

  //
  def abort () : Any = {

  	for ((key, v) <- dirtyset) {
     	
     	route(key) ! Abort(key)

	}
  }

  // 
  def commit () : Any = {

  	for ((key, v) <- dirtyset) {
      directWrite(key, v)
	}

  }

  /**
    * @param key A key
    * @return An ActorRef for a store server that stores the key's value.
    */
  private def route(key: BigInt): ActorRef = {
    stores((key % stores.length).toInt)
  }
}

