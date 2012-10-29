php-zookeeper-recipes
=====================

The <a href='http://zookeeper.apache.org/doc/current/recipes.html'>Zookeeper recipes</a> implemented in PHP,
using the <a href='http://pecl.php.net/zookeeper'>PECL Zookeeper API</a>.

For the time being (Q4 2012), I only plan to implement a few of the recipes synchronously. Patches welcome.

Here’s what has been implemented so far:
* Exclusive locks (class ZR_Xlock)
* Shared locks (class ZR_Slock)

