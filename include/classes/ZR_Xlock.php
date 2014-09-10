<?php

/**
* ZooKeeper exclusive lock.
* @author Bogdan Stancescu <bogdan@moongate.ro>
* @copyright Copyright (c) August 2012, Bogdan Stancescu
* @license http://www.gnu.org/licenses/agpl-3.0.html GNU Affero GPL
*/
class ZR_Xlock extends ZR_Base
{
	/**
	* The name of the actual lock key.
	*
	* @var string
	*/
	var $lock_name="lock-";

	/**
	* Lock a ZK key.
	*
	* Creates a sequenced key and waits for {@link _waitForLock()} to confirm
	* ours is the first in that sequence.
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
	public function lock($key, $timeout=0)
	{
		$full_key=$this->computeFullKey($this->getLockName($key));
		$lock_key=$this->createLockKey($full_key);

		if (!$this->_waitForLock($lock_key, $full_key, $timeout)) {
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
	protected function getLockName($key)
	{
		return $key."/".$this->lock_name;
	}

	/**
	* Waits for a sequenced key to be the first in the sequence,
	* thus ensuring that specific key's process has the lock.
	*
	* @param string $my_key a sequence lock key, as created in
	*		{@link lock}
	* @param string $base_key the lock key's associated full lock key
	*		(with full, absolute path)
	* @param float $timeout how long to wait before giving up.
	* @return bool true if ours is the first key in the sequence, false
	*		otherwise.
	*/
	private function _waitForLock($my_key, $base_key, $timeout)
	{
		$deadline=microtime(true)+$timeout;
		$my_index=$this->getIndex($my_key);

		while(true) {
			if (!$this->isAnyLock($base_key, $my_index)){
				return true;
            }
			if ($deadline<=microtime(true)){
				return false;
            }
			usleep($this->sleep_cycle*1000000);
		}
	}

	/**
	* Check if there's ANY lock on a specific key.
	*
	* Be advised this returns true if ANY lock is in place
	* for this key, regardless of its index in the sequence.
	* That is, if you obtain a lock and then call this, it
	* will tell you it IS locked.
	*
	* @param string $key the key to check for
	* @return bool true if there is any lock, false otherwise
	*/
	public function isLocked($key)
	{
		return $this->isAnyLock($this->computeFullKey($this->getLockName($key)));
	}

	/**
	* Wait for ALL locks named $base_key to die.
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
			if (!$this->isLocked($key)){
				return true;
            }
			if ($deadline && $deadline<=microtime(true)){
				return false;
            }
			usleep($this->sleep_cycle*1000000);
		}
	}

}
