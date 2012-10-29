<?php

require_once dirname(dirname(__FILE__))."/include/ZR_lib.php";
if (!file_exists(dirname(__FILE__)."/config.php")) {
	echo
		"In order to execute the PHPUnit tests you need a config.php file\n".
		"in directory phpunit. Please use config.sample.php as a template.\n";
	exit;
}
require_once "config.php";

class ZR_Slock_Test extends PHPUnit_Framework_TestCase
{
	protected $zl;
	const BASE_PATH="/zr_slock_test";
	const CLN="common_lock_name";

	protected function setUp()
	{
		$this->zl=new ZR_Slock(self::BASE_PATH);
	}

	public function testReadLock()
	{
		$this->assertFalse($this->zl->isReadLocked(self::CLN), "Read lock must not be present before locking.");
		$this->assertFalse($this->zl->isWriteLocked(self::CLN), "Write lock must not be present before locking.");
		$lock=$this->zl->rlock(self::CLN);
		$this->assertTrue((bool) $lock, "Read locking must succeed.");
		$this->assertRegExp("/\\/".self::CLN."\\/.*[0-9]+\$/", $lock, "The sequence key must contain the key name and must end with numbers.");
		$this->assertFalse($this->zl->isReadLocked(self::CLN), "Reading MUST NOT be locked after read locking.");
		$this->assertTrue($this->zl->isWriteLocked(self::CLN), "Writing MUST be locked after read locking.");
		$stime=microtime(true);
		$this->assertFalse($this->zl->waitForAllLocks(self::CLN,2), "The read lock must persist.");
		$this->assertGreaterThan(2, microtime(true)-$stime, "We should have waited more than two seconds for the read lock to clear.");
		$this->assertFalse($this->zl->wlock(self::CLN), "Write locking most not work with read lock in place.");
		$lock2=$this->zl->rlock(self::CLN);
		$this->assertTrue((bool) $lock2, "Read locking must succeed with another read lock in place.");
		$this->assertTrue($this->zl->unlock($lock), "Unlocking the first read lock must succeed.");
		$this->assertTrue($this->zl->isWriteLocked(self::CLN), "Writing must still be locked after deleting the first read lock.");
		$this->assertTrue($this->zl->unlock($lock2), "Unlocking the second read lock must succeed.");
		$this->assertFalse($this->zl->isWriteLocked(self::CLN), "Writing must be unlocked after unlocking both read locks.");
	}

	public function testWriteLock()
	{
		$this->assertFalse($this->zl->isReadLocked(self::CLN), "Read lock must not be present before locking.");
		$this->assertFalse($this->zl->isWriteLocked(self::CLN), "Write lock must not be present before locking.");
		$lock=$this->zl->wlock(self::CLN);
		$this->assertTrue((bool) $lock, "Write locking must succeed.");
		$this->assertRegExp("/\\/".self::CLN."\\/.*[0-9]+\$/", $lock, "The sequence key must contain the key name and must end with numbers.");
		$this->assertTrue($this->zl->isWriteLocked(self::CLN), "Reading MUST be locked after write locking.");
		$this->assertTrue($this->zl->isReadLocked(self::CLN), "Writing MUST be locked after write locking.");
		$stime=microtime(true);
		$this->assertFalse($this->zl->waitForAllLocks(self::CLN,2), "The write lock must persist.");
		$this->assertGreaterThan(2, microtime(true)-$stime, "We should have waited more than two seconds for the write lock to clear.");
		$this->assertFalse($this->zl->wlock(self::CLN), "Write locking most not work with write lock in place.");
		$lock2=$this->zl->rlock(self::CLN);
		$this->assertFalse((bool) $lock2, "Read locking must fail with write lock in place.");
		$this->assertTrue($this->zl->unlock($lock), "Unlocking the write lock must succeed.");
		$this->assertFalse($this->zl->isReadLocked(self::CLN), "No read lock must be present after unlocking the write lock.");
		$this->assertFalse($this->zl->isWriteLocked(self::CLN), "No write lock must be present after unlocking the write lock.");
	}

	public function testComplexLock1()
	{
		$pid=pcntl_fork();

		if ($pid==-1)
			throw new RuntimeException("Failed forking!");

		if ($pid) {
			self::parent_child_test_Beta();
			pcntl_waitpid($pid,$status);
		} else {
			self::parent_child_test_Alpha();
			// We must kill the child process manually, so it doesn't
			// proceed with furher tests
			exit(234);
		}
	}

	public function testComplexLock2()
	{
		$pid=pcntl_fork();

		if ($pid==-1)
			throw new RuntimeException("Failed forking!");

		if ($pid) {
			self::parent_child_test_Alpha();
			pcntl_waitpid($pid, $status);
		} else {
			self::parent_child_test_Beta();
			// We must kill the child process manually, so it doesn't
			// proceed with furher tests
			exit(234);
		}
	}

	private function getRenewedLock()
	{
		return new ZR_Slock(self::BASE_PATH, NULL, true);
	}

	private function parent_child_test_Beta()
	{
		// Beta always goes after Alpha, so make sure Alpha caught up
		while(flock($fp=fopen(__FILE__, 'r'), LOCK_EX + LOCK_NB)) {
			flock($fp, LOCK_UN);
			usleep(10);
		}
		// Now Alpha is ahead by 10 microseconds AT MOST; give Alpha a one second head start
		sleep(1);

		$zl=self::getRenewedLock();
		$this->assertFalse($zl->rlock(self::CLN), "Beta's read lock must fail (a write lock should have been acquired by Alpha by now).");
		$stime=microtime(true);
		$lock=$zl->rlock(self::CLN, 4);
		$this->assertTrue((bool) $lock, "Beta must succeed in acquiring a read lock (after some delay.)");
		$this->assertGreaterThan(2, microtime(true)-$stime, "Beta should've waited at least two seconds before acquiring the read lock.");
		$this->assertTrue($zl->unlock($lock), "Beta must be successfull in unlocking its own lock.");
	}

	private function parent_child_test_Alpha()
	{
		// We want Alpha to run before Beta, so make sure we're first
		flock($fp=fopen(__FILE__, 'r'), LOCK_EX);

		$zl=self::getRenewedLock();
		$lock=$zl->wlock(self::CLN);
		$this->assertTrue((bool) $lock, "Alpha's write lock must succeed.");
		sleep(4);
		$this->assertTrue($zl->unlock($lock), "Alpha must be successfull in unlocking its own lock.");
	}

}
