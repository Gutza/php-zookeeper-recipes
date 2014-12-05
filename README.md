php-zookeeper-recipes
=====================

The [Zookeeper recipes](http://zookeeper.apache.org/doc/current/recipes.html) implemented in PHP,
using the [PECL Zookeeper API](http://pecl.php.net/zookeeper).

For the time being (Q4 2012), I only plan to implement a few of the recipes synchronously. Patches welcome.

Here’s what has been implemented so far:

* Exclusive locks (class `ZR_Xlock`)  
* Shared locks (class `ZR_Slock`)  

## Usage

Creating an exclusive lock, using `ZR_Xlock`.

```php
require_once dirname(__FILE__). '/include/ZR_lib.php';

/**
 * ZR_HOSTLIST expects a comma delimited list of Zookeeper 
 * hosts and ports. If not otherwise defined, the value below is used.
 */
define("ZR_HOSTLIST", "127.0.0.1:2181");

/**
 * Configurable timeout to obtain the lock in seconds
 */
$timeout = 5;

/**
 * Create ZR_Xlock object, with base path 'my_path' and attempt to
 * acquire a lock named 'my_lock'
 */
try {
	$lock_manager = new ZR_Xlock('/my_path');
} catch (Exception $e) {
	echo 'Error connecting to Zookeeper, exiting. ('. $e->getMessage() .')' . PHP_EOL;
	exit();
}

try {
	$lock = $lock_manager->lock('my_lock',$timeout);
} catch (Exception $e) {
	echo 'Error obtaining lock, exiting. ('. $e->getMessage() .')' . PHP_EOL;
	exit();
}

/**
 * Some task that requires the lock 'my_lock' happens here.
 * Instead of doing real work, we'll relax.
 */
echo 'Lock '. $lock .' obtained, doing work...' . PHP_EOL;
sleep(3);

/**
 * We're done; release the lock.
 */
try {
	$unlock = $lock_manager->unlock($lock);
} catch (Exception $e) {
	echo 'Error releasing lock, exiting. ('. $e->getMessage() .')' . PHP_EOL;
  exit();
}

echo 'Lock '. $lock .' released.' . PHP_EOL;
```