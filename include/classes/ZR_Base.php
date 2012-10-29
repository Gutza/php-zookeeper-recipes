<?php

/**
 * ZooKeeper locking -- base class.
*
* @author Bogdan Stancescu <bogdan@moongate.ro>
* @copyright Copyright (c) August 2012, Bogdan Stancescu
* @license http://www.gnu.org/licenses/agpl-3.0.html GNU Affero GPL
*/
abstract class ZR_Base
{
	/**
	* ZooKeeper handler (a Zookeeper instance)
	*
	* @var object
	*/
	static $zk_h;

	/**
	* Current ZooKeeper base path
	*
	* @var string
	*/
	protected $base_path;

	/**
	* The default ACL used for ZK keys. You generally don't want to
	* mess with this, if you have a private ZK cluster.
	*
	* @var array
	*/
	public $default_acl=array(
		array(
			"perms"=>Zookeeper::PERM_ALL,
			"scheme"=>"world",
			"id"=>"anyone",
		),
	);

	/**
	* How many seconds to wait between various checks.
	* The default is 0.1 seconds, which should be reasonable
	* for most applications. Be advised that significantly shorter
	* delays might lead to unnecessary loads on the ZK cluster.
	*
	* @var float
	*/
	public $sleep_cycle=0.1;

	/**
	* How many seconds to wait before timing out on connections.
	*
	* @var float
	*/
	public $connection_timeout=5;

	/**
	* A local cache of known paths. Used by {link ensurePath()}.
	*
	* We exploit the assumption that we're always using the same ZK cluster.
	*
	* @var array
	*/
	static protected $known_paths=array();

	/**
	* Constructor. Tries connecting to the cluster.
	*
	* Throws exceptions if anything goes wrong.
	*
	* @param string $path the default path under which all subsequent
	*	operations will take place *by default*. Be advised that
	*	all operations can specify explicit paths if they start
	*	their respective path parameter with a slash ("/").
	* @param string $host the host(s) to connect to. If false,
	*	constant ZR_HOSTLIST is used.
	* @param boolean $renew if true, renew the ZK connection.
	*	You typically do NOT want this (it will kill all your
	*	previous ephemeral nodes). This only makes sense if you fork.
	*/
	public function __construct($path, $host=NULL, $renew=false)
	{
		if (is_null($host)) {
			if (!defined("ZR_HOSTLIST"))
				throw new RuntimeException(
					"Either specify the host explicitly ".
					"or define constant ZR_HOSTLIST."
				);
			$host=ZR_HOSTLIST;
		}

		if (!isset($path) || is_null($path) || !is_string($path) || !strlen($path))
			throw new RuntimeException(
				"The path needs to be a non-empty string."
			);

		while (substr($path, -1, 1)=='/')
			// Clip the final slash(es)
			$path=substr($path, 0, -1);

		if (!strlen($path))
			// Add a slash if we actually work in the root
			$path='/';

		$this->base_path=$path;

		if ($renew && isset(self::$zk_h))
			// It NEEDS to be manually "unset" (not simply replaced with the new one)
			self::$zk_h=NULL;

		if (empty(self::$zk_h)) {
			self::$zk_h=new Zookeeper();
			self::$zk_h->connect($host);
			$this->_waitForConnection();
		}
	}

	/**
	* Wait for the connection to be established.
	*
	* Used internally by the constructor.
	*
	* No parameters, no output. Uses {@link $connection_timeout} to
	* determine how long to wait, and throws an exception if it times out.
	*/
	private function _waitForConnection()
	{
		$deadline=microtime(true)+$this->connection_timeout;
		while(self::$zk_h->getState()!=Zookeeper::CONNECTED_STATE) {
			if ($deadline <= microtime(true))
				throw new RuntimeException("Zookeeper connection timed out!");
			usleep($this->sleep_cycle*1000000);
		}
	}

	/**
	* Unlocks a ZK key.
	*
	* It needs to be provided with a valid sequence lock key, as returned by
	* {@link lock()}.
	*
	* @param string $lock_key the sequence lock key to unlock
	* @return bool true on success, false on failure
	*/
	public function unlock($lock_key)
	{
		if (!is_string($lock_key))
			throw new DomainException(
				"This method expects a string!"
			);
		return self::$zk_h->delete($lock_key);
	}

	/**
	* Returns the sequence number for a given key,
	* or false if not a sequence node.
	*
	* @param string $key the key to compute the index for
	* @return mixed sequence number if a sequence node,
	*	NULL otherwise
	*/
	protected function getIndex($key)
	{
		if (!preg_match("/[0-9]+$/", $key, $matches))
			return NULL;

		return intval($matches[0]);
	}

	/**
	* Check for locks like $base_key.
	*
	* Checks if there is any sequence node under $base_key's parent.
	* By default, it checks if there is any such node whatsoever.
	*
	* If $index_filter is non-null then it checks whether any node
	* with a smaller index that $index_filter is present; returns
	* false if all the nodes it finds are larger.
	*
	* If $name_filter is true, it checks if the nodes are prefixed with
	* the $base_key's name. If $name_filter is a string, it checks if the
	* nodes are prefixed with $name_filter. In either of these cases,
	* nodes that don't match the filter are ignored.
	*
	* @param string $base_key the full key (with path), but
	*		without any sequence info
	* @param int $index_filter whether to filter by node index.
	* @param mixed $name_filter whether to filter by node name results.
	* @return bool true if a matching node is found, false otherwise
	*/
	protected function isAnyLock($base_key, $index_filter=NULL, $name_filter=false)
	{
		$parent=self::getParentName($base_key);
		if (!self::$zk_h->exists($parent))
			return false;

		$children=self::$zk_h->getChildren($parent);
		foreach($children as $child_key) {
			$child=$parent."/".$child_key;
			if ($name_filter===true || is_string($name_filter)) {
				if ($name_filter===true)
					$filter=$base_key;
				else
					$filter=$name_filter;

				if (substr($child, 0, strlen($filter))!=$filter)
					continue;
			}

			if (is_null($index_filter))
				return true;

			$child_index=$this->getIndex($child_key);

			if (is_null($child_index))
				// Not a sequence node
				continue;

			if ($child_index<$index_filter)
				// smaller index
				return true;
		}
		return false;
	}

	/**
	* Creates a generic lock key, with the specified flags.
	*
	* This is a relatively low-level method, please see
	* how it's used in other descendants.
	*
	* @param string $full_key the full key name to be created
	* @param int $flags the Zookeeper flags for this lock;
	*		if not specified, will use
	*		Zookeeper::EPHEMERAL | Zookeeper::SEQUENCE
	*/
	protected function createLockKey($full_key, $flags=NULL)
	{
		if (is_null($flags))
			$flags=Zookeeper::EPHEMERAL | Zookeeper::SEQUENCE;

		$this->ensurePath($full_key);
		$lock_key=self::$zk_h->create(
			$full_key, // path
			1, // value
			$this->default_acl, // ACL
			$flags // flags
		);
		if (!$lock_key)
			throw new RuntimeException("Failed creating lock node ".$full_key);

		return $lock_key;
	}

	/**
	* Converts a relative key to a full key.
	*
	* This uses {@link $base_path} to prepend the path to the
	* specified key. If the key is already a full key (i.e.
	* it starts with a "/") then the key is returned unchanged.
	*
	* @param string $key the key to process
	* @return string the associated full key (with absolute path)
	*/
	protected function computeFullKey($key)
	{
		if (substr($key, 0, 1)=='/')
			return $key;
		return $this->base_path.'/'.$key;
	}

	/**
	* Ensures the path of the specified key exists, and creates
	* all required parents if necessary. It does NOT create the key itself.
	*
	* @param $key the full key (with path)
	* @return bool true on success. On failure it throws an exception.
	*/
	protected function ensurePath($key)
	{
		$parent=self::getParentName($key);
		if (in_array($key, self::$known_paths))
			return true;
		if (self::$zk_h->exists($parent))
			return true;
		if (!$this->ensurePath($parent))
			return false; // We should never execute this
		if (self::$zk_h->create($parent, 1, $this->default_acl))
			return true;
		throw new RuntimeException("Failed creating path [".$parent."]");
	}

	/**
	* Returns the parent's name for a specified key.
	*
	* @param string $key the full key (with path)
	* @return string the key parent's path
	*/
	protected function getParentName($key)
	{
		return dirname($key);
	}
}
