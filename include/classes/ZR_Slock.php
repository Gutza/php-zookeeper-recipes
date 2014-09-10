<?php

/**
* ZooKeeper shared lock.
* @author Bogdan Stancescu <bogdan@moongate.ro>
* @copyright Copyright (c) October 2012, Bogdan Stancescu
* @license http://www.gnu.org/licenses/agpl-3.0.html GNU Affero GPL
*/
class ZR_Slock extends ZR_Base
{
	/**
	* Constant used internally to indicate which type of lock we're using.
	* This one is used for read locks.
	*/
	const MODE_READ="read";

	/**
	* Constant used internally to indicate which type of lock we're using.
	* This one is used for write locks.
	*/
	const MODE_WRITE="write";
	/**
	* The name of the read lock keys.
	*
	* @var string
	*/
	var $read_lock_name="read-";

	/**
	* The name of the write lock keys.
	*
	* @var string
	*/
	var $write_lock_name="write-";

	/**
	* Create a write lock.
	*
	* Write locks are exclusive: once a write lock is in place, no new read or
	* write locks are allowed until this one is unlocked.
	*
	* Creates a sequenced key and waits for {@link _waitForLock()} to confirm
	* ours is the very first in the sequence.
	*
	* On success, it returns the sequence lock key (sequence number included),
	* which can then be used with {@link unlock()} to unlock.
	*
	* @param string $key the key to use. Typically a key under the default path
	*	defined in {@link __construct}, unless $key starts with a
	*	slash, in which case $key is the full path
	* @param int $timeout how many seconds to wait. If zero, just return.
	* @return mixed (boolean) false on failure, or (string) sequence lock key
	*/
	public function wlock($key, $timeout=0)
	{
		return $this->_lock($key, $timeout, self::MODE_WRITE);
	}

	/**
	* Creates a read lock.
	*
	* Read locks are shared: any number of read locks can be set at the same
	* time, but once a write lock is set, read locks must wait for the write
	* lock to be unlocked.
	*
	* Creates a sequenced key and waits for {@link _waitForLock()} to confirm
	* there is no earlier write lock in place.
	*
	* On success, it returns the sequence lock key (sequence number included),
	* which can then be used with {@link unlock()} to unlock.
	*
	* @param string $key the key to use. Typically a key under the default path
	*	defined in {@link __construct}, unless $key starts with a
	*	slash, in which case $key is the full path
	* @param int $timeout how many seconds to wait. If zero, just return.
	* @return mixed (boolean) false on failure, or (string) sequence lock key
	*/
	public function rlock($key, $timeout=0)
	{
		return $this->_lock($key, $timeout, self::MODE_READ);
	}

	/**
	* An internal method for creating read or write locks.
	*
	* @param string $key the key name
	* @param int $timeout how many seconds to wait before timing out
	* @param string $mode the lock mode (one of the mode constants)
	* @return mixed (string) the full lock key on success, (bool) false on failure
	*/
	private function _lock($key, $timeout, $mode)
	{
		$full_key=$this->computeFullKey($this->getLockName($key, $mode));
		$lock_key=$this->createLockKey($full_key);
		if (!$this->_waitForLock($key, $lock_key, $full_key, $timeout, $mode)) {
			// Clean up
			self::$zk_h->delete($lock_key);
			return false;
		}
		return $lock_key;
	}

	/**
	* Get a generic name and return the name of the ZK key appropriate for locking.
	*
	* ZK sequences are per parent node, so this ensures we're always working inside
	* the requested node. It simply appends a slash and {@link $lock_name} to the
	* specified key name.
	*
	* @param string $key the desired name for the lock
	* @return string the name of the ZK node for this lock
	*/
	protected function getLockName($key, $mode)
	{
		switch($mode) {
			case self::MODE_READ:
				$name=$this->read_lock_name;
				break;
			case self::MODE_WRITE:
				$name=$this->write_lock_name;
				break;
			default:
				throw new RuntimeException("Unknown lock mode: ".$mode);
		}
		return $key."/".$name;
	}

	/**
	* Waits for a sequenced key to be the first in the sequence,
	* thus ensuring that specific key's process has the lock.
	*
	* @param string $my_key a sequence lock key, as created in
	*		{@link lock}
	* @param string $base_key the lock key's associated full lock key
	*		(with full, absolute path)
	* @param float $timeout how many seconds to wait before giving up.
	* @param string $mode lock mode (one of the mode constants)
	* @return bool true if ours is the first key in the sequence, false
	*		otherwise.
	*/
	private function _waitForLock($key, $my_key, $base_key, $timeout, $mode)
	{
		$deadline=microtime(true)+$timeout;
		$my_index=$this->getIndex($my_key);

		while(true) {
			if ($mode==self::MODE_READ){
				$name_filter=$this->computeFullKey($this->getLockName($key, self::MODE_WRITE));
            }else{
				$name_filter=false;
            }

			if (!$this->isAnyLock($base_key, $my_index, $name_filter)){
				return true;
            }
			if ($deadline<=microtime(true)){
				return false;
            }
			usleep($this->sleep_cycle*1000000);
		}
	}

	/**
	* Check if there's ANY lock on a specific key, blocking write locks.
	*
	* Be advised this returns true if ANY lock is in place
	* for this key, regardless of its type or index in the sequence.
	* That is, if you obtain any kind of lock and then call this, it
	* will tell you it IS locked.
	*
	* @param string $key the key to check for
	* @return bool true if there is any lock, false otherwise
	*/
	public function isWriteLocked($key)
	{
		return $this->isAnyLock($this->computeFullKey($this->getLockName($key, self::MODE_READ)));
	}

	/**
	* Check if there's any WRITE lock on a specific key, blocking read locks.
	*
	* Be advised this returns true if any write lock is in place
	* for this key, regardless of its index in the sequence.
	* That is, if you obtain a write lock and then call this, it
	* will tell you it IS locked.
	*
	* @param string $key the key to check for
	* @return bool true if there is any lock, false otherwise
	*/
	public function isReadLocked($key)
	{
		return $this->isAnyLock($this->computeFullKey($this->getLockName($key, self::MODE_WRITE)), NULL, true);
	}

	/**
	* Wait for all WRITE locks named $key to die.
	*
	* Be advised, this uses {@link isLocked()}, so it does NOT
	* wait for all PREVIOUS locks to die -- this waits for ALL
	* write locks to die. That is, if you obtain a lock on this key
	* prior to calling this method, this will also wait for
	* YOUR own lock to be removed before returning true.
	*
	* $timeout in seconds; 0 means wait indefinitely
	*
	* @param string $key the key to wait for
	* @param float $timeout the amount of time to wait, in seconds
	* @return bool true if all previous locks for this key
	*		are dead, false if we timed out while waiting.
	*/
	public function waitForAllWriteLocks($key, $timeout=0)
	{
		if ($timeout){
			$deadline=microtime(true)+$timeout;
        }else{
			$deadline=0;
        }

		while(true) {
			if (!$this->isReadLocked($key)){
				return true;
            }
			if ($deadline && $deadline<=microtime(true)){
				return false;
            }
			usleep($this->sleep_cycle*1000000);
		}
	}

	/**
	* Wait for ALL locks named $key to die.
	*
	* Be advised, this uses {@link isLocked()}, so it does NOT
	* wait for all PREVIOUS locks to die -- this waits for ALL
	* locks to die. That is, if you obtain a lock on this key
	* prior to calling this method, this will also wait for
	* YOUR own lock to be removed before returning true.
	*
	* $timeout in seconds; 0 means wait indefinitely
	*
	* @param string $key the key to wait for
	* @param float $timeout the amount of time to wait, in seconds
	* @return bool true if all previous locks for this key
	*		are dead, false if we timed out while waiting.
	*/
	public function waitForAllLocks($key, $timeout=0)
	{
		if ($timeout){
			$deadline=microtime(true)+$timeout;
        }else{
			$deadline=0;
        }

		while(true) {
			if (!$this->isWriteLocked($key)){
				return true;
            }
			if ($deadline && $deadline<=microtime(true)){
				return false;
            }
			usleep($this->sleep_cycle*1000000);
		}
	}
}
