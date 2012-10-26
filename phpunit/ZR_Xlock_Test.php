<?php

require_once dirname(dirname(__FILE__))."/include/ZR_lib.php";
require_once "config.php";

class ZR_Xlock_Test extends PHPUnit_Framework_TestCase
{
	protected $zl;
	const BASE_PATH="/zr_lock_test";
	const CLN="common_lock_name";

	protected function setUp()
	{
		$this->zl=new ZR_Xlock(self::BASE_PATH);
	}

	public function testSimpleLock()
	{
		$this->assertFalse($this->zl->isLocked(self::CLN), "Lock must not be present before locking.");
		$lock=$this->zl->lock(self::CLN);
		$this->assertTrue((bool) $lock, "Locking must succeed.");
		$this->assertRegExp("/\\/".self::CLN."\\/.*[0-9]+\$/", $lock, "The sequence key must contain the key name and must end with numbers.");
		$this->assertTrue($this->zl->isLocked(self::CLN), "Lock must be present after locking.");
		$stime=microtime(true);
		$this->assertFalse($this->zl->waitForAllLocks(self::CLN,2), "The lock must persist.");
		$this->assertGreaterThan(2, microtime(true)-$stime, "We should have waited more than two seconds.");
		$this->assertFalse($this->zl->lock(self::CLN, "Locking most not work with lock in place."));
		$this->assertTrue($this->zl->unlock($lock), "Unlocking must succeed.");
		$this->assertFalse($this->zl->isLocked(self::CLN), "Lock must not be present after unlocking.");
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
		return new ZR_Xlock(self::BASE_PATH, NULL, true);
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
		$this->assertFalse($zl->lock(self::CLN), "Beta's first lock must fail (it should have been acquired by Alpha by now).");
		$stime=microtime(true);
		$lock=$zl->lock(self::CLN, 4);
		$this->assertTrue((bool) $lock, "Beta must succeed in acquiring the lock (after some delay.)");
		$this->assertGreaterThan(2, microtime(true)-$stime, "Beta should've waited at least two seconds before acquiring the lock.");
		$this->assertTrue($zl->unlock($lock), "Beta must be successfull in unlocking its own lock.");
	}

	private function parent_child_test_Alpha()
	{
		// We want Alpha to run before Beta, so make sure we're first
		flock($fp=fopen(__FILE__, 'r'), LOCK_EX);

		$zl=self::getRenewedLock();
		$lock=$zl->lock(self::CLN);
		$this->assertTrue((bool) $lock, "Alpha's lock must succeed.");
		sleep(4);
		$this->assertTrue($zl->unlock($lock), "Alpha's unlock must succeed.");
	}

}
