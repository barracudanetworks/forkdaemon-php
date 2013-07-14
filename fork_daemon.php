<?php
/**
 * System process functions
 * @category system
 * @package fork_daemon
 */

class fork_daemon
{
	/**
	 * Child process status constants
	 *
	 * @access public
	 */
	const WORKER = 0;
	const HELPER = 1;

	/**
	 * Bucket constants
	 *
	 * @access public
	 */
	const DEFAULT_BUCKET = -1;

	/**
	 * Logging constants
	 *
	 * @access public
	 */
	const LOG_LEVEL_CRIT = 2;
	const LOG_LEVEL_WARN = 4;
	const LOG_LEVEL_INFO = 6;
	const LOG_LEVEL_DEBUG = 7;

	/**
	 * Variables
	 */

	/**
	 * Maximum time in seconds a PID may execute
	 * @access private
	 * @var integer $child_max_run_time
	 */
	private $child_max_run_time = array(self::DEFAULT_BUCKET => 86400);

	/**
	 * Function the child invokes with a set of worker units
	 * @access private
	 * @var integer $child_function_run
	 */
	private $child_function_run = array(self::DEFAULT_BUCKET => '');

	/**
	 * Function the parent invokes when a child finishes
	 * @access private
	 * @var integer $parent_function_child_exited
	 */
	private $parent_function_child_exited = array(self::DEFAULT_BUCKET => '');

	/**
	 * Function the child invokes when sigint/term is received
	 * @access private
	 * @var integer $child_function_exit
	 */
	private $child_function_exit = array(self::DEFAULT_BUCKET => '');

	/**
	 * Function the parent invokes when a child is killed due to exceeding the max runtime
	 * @access private
	 * @var integer $child_function_timeout
	 */
	private $child_function_timeout = array(self::DEFAULT_BUCKET => "");

	/**
	 * Function the parent invokes when a child is spawned
	 * @access private
	 * @var integer $parent_function_fork
	 */
	private $parent_function_fork = array(self::DEFAULT_BUCKET => '');

	/**
	 * Function the parent invokes when the parent receives a SIGHUP
	 * @access private
	 * @var integer $parent_function_sighup
	 */
	private $parent_function_sighup = '';

	/**
	 * Property of the parent sighup function.  If true, the parent
	 * will send sighup to all children when the parent receives a
	 * sighup.
	 * @access private
	 * @var integer $parent_function_sighup_cascade
	 */
	private $parent_function_sighup_cascade = true;

	/**
	 * Function the child invokes when the child receives a SIGHUP
	 * @access private
	 * @var integer $child_function_sighup
	 */
	private $child_function_sighup = array(self::DEFAULT_BUCKET => '');
	
	/**
	 * Max number of seconds to wait for a child process
	 * exit once it has been requested to exit
	 * @access private
	 * @var integer $children_kill_timeout
	 */
	private $children_max_timeout = 30;

	/**
	 * Function the parent runs when the daemon is getting shutdown
	 * @access private
	 * @var integer $parent_function_exit
	 */
	private $parent_function_exit = '';

	/**
	 * Stores whether the daemon is in single item mode or not
	 * @access private
	 * @var bool $child_single_work_item
	 */
	private $child_single_work_item = array(self::DEFAULT_BUCKET => false);

	/**************** SERVER CONTROLS ****************/
	/**
	 * Upper limit on the number of children started.
	 * @access private
	 * @var integer $max_children
	 */
	private $max_children = array(self::DEFAULT_BUCKET => 25);

	/**
	 * Upper limit on the number of work units sent to each child.
	 * @access private
	 * @var integer $max_work_per_child
	 */
	private $max_work_per_child = array(self::DEFAULT_BUCKET => 100);

	/**
	 * Interval to do house keeping in seconds
	 * @access private
	 * @var integer $housekeeping_check_interval
	 */
	private $housekeeping_check_interval = 20;

	/**************** TRACKING CONTROLS ****************/

	/**
	 * track children of parent including their status and create time
	 * @access private
	 * @var array $forked_children
	 */
	private $forked_children = array();

	/**
	 * track the work units to process
	 * @access private
	 * @var array $work_units
	 */
	private $work_units = array(self::DEFAULT_BUCKET => array());

	/**
	 * track the buckets
	 * @access private
	 * @var array $buckets
	 */
	private $buckets = array(0 => self::DEFAULT_BUCKET);

	/**
	 * within a child, track the bucket the child exists in. note,
	 * this shouldn't be set or referenced in the parent process
	 * @access private
	 * @var int $child_bucket
	 */
	private $child_bucket = null;

	/**************** MOST IMPORTANT CONTROLS  ****************/

	/**
	 * parent pid
	 * @access private
	 * @var array $parent_pid
	 */
	static private $parent_pid;

	/**
	 * last housekeeping check time
	 * @access private
	 * @var array $housekeeping_last_check
	 */
	private $housekeeping_last_check = 0;

	/**************** FUNCTION DEFINITIONS  ****************/

	/**
	 * Set and Get functions
	 */

	/**
	 * Allows the app to set the max_children value
	 * @access public
	 * @param int $value the new max_children value.
	 * @param int $bucket the bucket to use
	 */
	public function max_children_set($value, $bucket = self::DEFAULT_BUCKET)
	{
		if ($value < 1)
		{
			$value = 0;
			$this->log(($bucket === self::DEFAULT_BUCKET ? 'default' : $bucket) . ' bucket max_children set to 0, bucket will be disabled', self::LOG_LEVEL_WARN);
		}

		$this->max_children[$bucket] = $value;
	}

	/**
	 * Allows the app to retrieve the current max_children value.
	 * @access public
	 * @param int $bucket the bucket to use
	 * @return int the max_children value
	 */
	public function max_children_get($bucket = self::DEFAULT_BUCKET)
	{
		return($this->max_children[$bucket]);
	}

	/**
	 * Allows the app to set the max_work_per_child value
	 * @access public
	 * @param int $value new max_work_per_child value.
	 * @param int $bucket the bucket to use
	 */
	public function max_work_per_child_set($value, $bucket = self::DEFAULT_BUCKET)
	{
		if ($this->child_single_work_item[$bucket])
		{
			$value = 1;
		}

		if ($value < 1)
		{
			$value = 0;
			$this->log(($bucket === self::DEFAULT_BUCKET ? 'default' : $bucket) . ' bucket max_work_per_child set to 0, bucket will be disabled', self::LOG_LEVEL_WARN);
		}

		$this->max_work_per_child[$bucket] = $value;
	}

	/**
	 * Allows the app to retrieve the current max_work_per_child value.
	 * @access public
	 * @param int $bucket the bucket to use
	 * @return int the max_work_per_child value
	 */
	public function max_work_per_child_get($bucket = self::DEFAULT_BUCKET)
	{
		return($this->max_work_per_child[$bucket]);
	}

	/**
	 * Allows the app to set the child_max_run_time value
	 * @access public
	 * @param int $value new child_max_run_time value.
	 * @param int $bucket the bucket to use
	 */
	public function child_max_run_time_set($value, $bucket = self::DEFAULT_BUCKET)
	{
		if ($value < 1)
		{
			$value = 0;
			$this->log(($bucket === self::DEFAULT_BUCKET ? 'default' : $bucket) . ' bucket child_max_run_time set to 0', self::LOG_LEVEL_WARN);
		}

		$this->child_max_run_time[$bucket] = $value;
	}

	/**
	 * Allows the app to retrieve the current child_max_run_time value.
	 * @access public
	 * @param int $bucket the bucket to use
	 * @return int the child_max_run_time value
	 */
	public function child_max_run_time_get($bucket = self::DEFAULT_BUCKET)
	{
		return($this->child_max_run_time[$bucket]);
	}

	/**
	 * Allows the app to set the child_single_work_item value
	 * @access public
	 * @param int $value new child_single_work_item value.
	 * @param int $bucket the bucket to use
	 */
	public function child_single_work_item_set($value, $bucket = self::DEFAULT_BUCKET)
	{
		if ($value < 1)
		{
			$value = 0;
			$this->log(($bucket === self::DEFAULT_BUCKET ? 'default' : $bucket) . ' bucket child_single_work_item set to 0', self::LOG_LEVEL_WARN);
		}

		$this->child_single_work_item[$bucket] = $value;
	}

	/**
	 * Allows the app to retreive the current child_single_work_item value.
	 * @access public
	 * @param int $bucket the bucket to use
	 * @return int the child_single_work_item value
	 */
	public function child_single_work_item_get($bucket = self::DEFAULT_BUCKET)
	{
		return($this->child_single_work_item[$bucket]);
	}

	/**
	 * Allows the app to retreive the current child_bucket value.
	 * @access public
	 * @return int the child_bucket value representing the bucket number of the child
	 */
	public function child_bucket_get()
	{
		// this function does not apply to the parent
		if (self::$parent_pid == getmypid()) return false;

		return($this->child_bucket);
	}


	/**
	 * Creates a new bucket to house forking operations
	 * @access public
	 * @param int $bucket the bucket to create
	 */
	public function add_bucket($bucket)
	{
		/* create the bucket by copying values from the default bucket */
		$this->max_children[$bucket] = $this->max_children[self::DEFAULT_BUCKET];
		$this->child_single_work_item[$bucket] = $this->child_single_work_item[self::DEFAULT_BUCKET];
		$this->max_work_per_child[$bucket] = $this->max_work_per_child[self::DEFAULT_BUCKET];
		$this->child_max_run_time[$bucket] = $this->child_max_run_time[self::DEFAULT_BUCKET];
		$this->child_single_work_item[$bucket] = $this->child_single_work_item[self::DEFAULT_BUCKET];
		$this->child_function_run[$bucket] = $this->child_function_run[self::DEFAULT_BUCKET];
		$this->parent_function_fork[$bucket] = $this->parent_function_fork[self::DEFAULT_BUCKET];
		$this->child_function_sighup[$bucket] = $this->child_function_sighup[self::DEFAULT_BUCKET];
		$this->child_function_exit[$bucket] = $this->child_function_exit[self::DEFAULT_BUCKET];
		$this->parent_function_child_exited[$bucket] = $this->parent_function_child_exited[self::DEFAULT_BUCKET];
		$this->work_units[$bucket] = array();
		$this->buckets[$bucket] = $bucket;
	}

	/**
	 * Allows the app to set the call back function for child processes
	 * @access public
	 * @param string name of function to be called.
	 * @param int $bucket the bucket to use
	 * @return bool true if the callback was successfully registered, false if it failed
	 */
	public function register_child_run($function_name, $bucket = self::DEFAULT_BUCKET)
	{
		/* call child function */
		if ( ( is_array($function_name) && method_exists($function_name[0], $function_name[1]) ) || method_exists($this, $function_name) || function_exists($function_name) )
		{
			$this->child_function_run[$bucket] = $function_name;
			return true;
		}

		return false;
	}

	/**
	 * Allows the app to set the call back function for when a child process is spawned
	 * @access public
	 * @param string name of function to be called.
	 * @param int $bucket the bucket to use
	 * @return bool true if the callback was successfully registered, false if it failed
	 */
	public function register_parent_fork($function_name, $bucket = self::DEFAULT_BUCKET)
	{
		/* call child function */
		if ( method_exists($this, $function_name) || function_exists($function_name) )
		{
			$this->parent_function_fork[$bucket] = $function_name;
			return true;
		}

		return false;
	}

	/**
	 * Allows the app to set the call back function for when a parent process receives a SIGHUP
	 * @access public
	 * @param string name of function to be called.
	 * @param bool $cascade_signal if true, the parent will send a sighup to all of it's children
	 * @param int $bucket the bucket to use
	 * @return bool true if the callback was successfully registered, false if it failed
	 */
	public function register_parent_sighup($function_name, $cascade_signal = true)
	{
		/* call child function */
		if ( method_exists($this, $function_name) || function_exists($function_name) )
		{
			$this->parent_function_sighup         = $function_name;
			$this->parent_function_sighup_cascade = $cascade_signal;
			return true;
		}

		return false;
	}

	/**
	 * Allows the app to set the call back function for when a child process receives a SIGHUP
	 * @access public
	 * @param string name of function to be called.
	 * @param int $bucket the bucket to use
	 * @return bool true if the callback was successfully registered, false if it failed
	 */
	public function register_child_sighup($function_name, $bucket = self::DEFAULT_BUCKET)
	{
		/* call child function */
		if ( method_exists($this, $function_name) || function_exists($function_name) )
		{
			$this->child_function_sighup[$bucket] = $function_name;
			return true;
		}

		return false;
	}

	/**
	 * Allows the app to set the call back function for when a child process exits
	 * @access public
	 * @param string name of function to be called.
	 * @param int $bucket the bucket to use
	 * @return bool true if the callback was successfully registered, false if it failed
	 */
	public function register_child_exit($function_name, $bucket = self::DEFAULT_BUCKET)
	{
		/* call child function */
		if ( method_exists($this, $function_name) || function_exists($function_name) )
		{
			$this->child_function_exit[$bucket] = $function_name;
			return true;
		}

		return false;
	}

	/**
	 * Allows the app to set the call back function for when a child process is killed to exceeding its max runtime
	 * @access public
	 * @param string name of function to be called.
	 * @param int $bucket the bucket to use
	 * @return bool true if the callback was successfully registered, false if it failed
	 */
	public function register_child_timeout($function_name, $bucket = self::DEFAULT_BUCKET)
	{
		/* call child function */
		if ( method_exists($this, $function_name) || function_exists($function_name) )
		{
			$this->child_function_timeout[$bucket] = $function_name;
			return true;
		}

		return false;
	}

	/**
	 * Allows the app to set the call back function for when the parent process exits
	 * @access public
	 * @param string name of function to be called.
	 * @return bool true if the callback was successfully registered, false if it failed
	 */
	public function register_parent_exit($function_name)
	{
		// call parent function
		if ( method_exists($this, $function_name) || function_exists($function_name) )
		{
			$this->parent_function_exit = $function_name;
			return true;
		}

		return false;
	}

	/**
	 * Allows the app to set the call back function for when a child exits in the parent
	 * @access public
	 * @param string name of function to be called.
	 * @param int $bucket the bucket to use
	 * @return bool true if the callback was successfully registered, false if it failed
	 */
	public function register_parent_child_exit($function_name, $bucket = self::DEFAULT_BUCKET)
	{
		/* call parent function */
		if ( method_exists($this, $function_name) || function_exists($function_name) )
		{
			$this->parent_function_child_exited[$bucket] = $function_name;
			return true;
		}

		return false;
	}
	
	/************ NORMAL FUNCTION DEFS ************/

	/**
	 * This is the class constructor, initializes the object.
	 * @access public
	 */
	public function __construct()
	{
		/* record pid of parent process */
		self::$parent_pid = getmypid();

		/* install signal handlers */
		declare(ticks = 1);
		pcntl_signal(SIGHUP,  array(&$this, 'signal_handler_sighup'));
		pcntl_signal(SIGCHLD, array(&$this, 'signal_handler_sigchild'));
		pcntl_signal(SIGTERM, array(&$this, 'signal_handler_sigint'));
		pcntl_signal(SIGINT,  array(&$this, 'signal_handler_sigint'));
		pcntl_signal(SIGALRM, SIG_IGN);
		pcntl_signal(SIGUSR2, SIG_IGN);
		pcntl_signal(SIGBUS,  SIG_IGN);
		pcntl_signal(SIGPIPE, SIG_IGN);
		pcntl_signal(SIGABRT, SIG_IGN);
		pcntl_signal(SIGFPE,  SIG_IGN);
		pcntl_signal(SIGILL,  SIG_IGN);
		pcntl_signal(SIGQUIT, SIG_IGN);
		pcntl_signal(SIGTRAP, SIG_IGN);
		pcntl_signal(SIGSYS,  SIG_IGN);
	}

	/**
	 * Destructor does not do anything.
	 * @access public
	*/
	public function __destruct()
	{
	}

	/**
	 * Handle both parent and child registered sighup callbacks.
	 *
	 * @param int $signal_number is the signal that called this function. (should be '1' for SIGHUP)
	 * @access public
	 */
	public function signal_handler_sighup($signal_number)
	{
		if (self::$parent_pid == getmypid())
		{
			// parent received sighup
			$this->log('parent process [' . getmypid() . '] received sighup', self::LOG_LEVEL_DEBUG);

			// call parent's sighup registered callback
			$this->invoke_callback($this->parent_function_sighup, $parameters = array(), true);

			// if cascading, send sighup to all child processes
			if ($this->parent_function_sighup_cascade === true)
			{
				foreach ($this->forked_children as $pid => $pid_info)
				{
					$this->log('parent process [' . getmypid() . '] sending sighup to child ' . $pid, self::LOG_LEVEL_DEBUG);
					posix_kill($pid, SIGHUP);
				}
			}
		}
		else
		{
			// child received sighup. note a child is only in one bucket, do not loop through all buckets
			$this->log('child process [' . getmypid() . '] received sighup with bucket type [' . $this->child_bucket . ']', self::LOG_LEVEL_DEBUG);
			$this->invoke_callback(
				$this->child_function_sighup[$this->child_bucket],
				array($this->child_bucket),
				true
			);
		}
	}
	
	/**
	 * Handle parent registered sigchild callbacks.
	 *
	 * @param int $signal_number is the signal that called this function.
	 * @access public
	 */
	public function signal_handler_sigchild($signal_number)
	{
		// do not allow signals to interupt this
		declare(ticks = 0)
		{
			// reap all child zombie processes
			if (self::$parent_pid == getmypid())
			{
				$status = '';

				do
				{
					// get child pid that exited
					$child_pid = pcntl_waitpid(0, $status, WNOHANG);

					if ($child_pid > 0)
					{
						// child exited
						$identifier = false;
						if (!isset($this->forked_children[$child_pid]))
						{
							die("Cannot find $child_pid in array!\n");
						}

						$child = $this->forked_children[$child_pid];
						$identifier = $child['identifier'];

						// call exit function if and only if its declared */
						if ($child['status'] == self::WORKER)
							$this->invoke_callback($this->child_function_exit{$child['bucket']}, $parameters = array($child_pid, $identifier), true);

						// stop tracking the childs PID
						unset($this->forked_children[$child_pid]);

						// respawn helper processes
						if ($child['status'] == self::HELPER && $child['respawn'] === true)
						{
							Log::message('Helper process ' . $child_pid . ' died, respawning', LOG_LEVEL_INFO);
							$this->helper_process_spawn($child['function'], $child['arguments'], $child['identifier'], true);
						}
					}
					elseif ($child_pid < 0)
					{
						// pcntl_wait got an error
						$this->log('pcntl_waitpid failed', self::LOG_LEVEL_DEBUG);
					}
				}
				while ($child_pid > 0);
			}
		}
	}
	
	/**
	 * Handle both parent and child registered sigint callbacks
	 *
	 * User terminated by CTRL-C (detected only by the parent)
	 *
	 * @param int $signal_number is the signal that called this function
	 * @access public
	 */
	public function signal_handler_sigint($signal_number)
	{
		// kill child processes
		if (self::$parent_pid == getmypid())
		{
			foreach ($this->forked_children as $pid => $pid_info)
			{
				// tell helpers not to respawn
				if ($pid_info['status'] == self::HELPER)
					$pid_info['respawn'] = false;

				$this->log('requestsing child exit for pid: ' . $pid, self::LOG_LEVEL_INFO);
				posix_kill($pid, SIGINT);
			}

			sleep(1);
			
			// checking for missed sigchild
			$this->signal_handler_sigchild(SIGCHLD);
			
			$start_time = time();

			// wait for child processes to go away
			while (count($this->forked_children) > 0)
			{
				if (time() > ($start_time + $this->children_max_timeout))
				{
					foreach ($this->forked_children as $pid => $child)
					{
						$this->log('force killing child pid: ' . $pid, self::LOG_LEVEL_INFO);
						posix_kill($pid, SIGKILL);
						
						// remove the killed process from being tracked
						unset($this->forked_children[$pid]);
					}
				}
				else
				{
					$this->log('waiting ' . ($start_time + $this->children_max_timeout - time()) . ' seconds for ' . count($this->forked_children) . ' children to clean up', self::LOG_LEVEL_INFO);
					sleep(1);
					$this->housekeeping_check();
				}
			}

			// make call back to parent exit function if it exists
			$this->invoke_callback($this->parent_function_exit, $parameters = array(self::$parent_pid), true);
		}
		else
		{
			// invoke child cleanup callback
			$this->invoke_callback($this->child_function_exit[$this->child_bucket], $parameters = array($this->child_bucket), true);
		}

		exit(-1);
	}

	/**
	 * Add work to the group of work to be processed
	 *
	 * @param mixed array of items to be handed back to child in chunks
	 * @param string a unique identifier for this work
	 * @param int $bucket the bucket to use
	 * @param bool $sort_queue true to sort the work unit queue
	 */
	public function addwork($new_work_units, $identifier = '', $bucket = self::DEFAULT_BUCKET, $sort_queue = false)
	{
		// ensure bucket is setup before we try to add data to it
		if (! array_key_exists($bucket, $this->work_units))
			$this->add_bucket($bucket);

		// add to queue to send
		if ($this->child_single_work_item[$bucket])
		{
			// prepend identifier with 'id-' because array_splice() re-arranges numeric keys
			$this->work_units[$bucket]['id-' . $identifier] = $new_work_units;
		}
		elseif ($new_work_units === null || sizeof($new_work_units) === 0)
		{
			// no work
		}
		else
		{
			// merge in the new work units
			$this->work_units[$bucket] = array_merge($this->work_units[$bucket], $new_work_units);
		}

		// sort the queue
		if ($sort_queue)
			ksort($this->work_units[$bucket]);

		return;
	}

	/*
	 * Based on identifier and bucket is a child working on the work
	 *
	 * @param string unique identifier for the work
	 * @param int $bucket the bucket
	 * @return bool true if child has work, false if not
	 */
	public function is_work_running($identifier, $bucket = self::DEFAULT_BUCKET)
	{
		foreach ($this->forked_children as $info)
		{
			if (($info['identifier'] == $identifier) && ($info['bucket'] == $bucket))
			{
				return true;
			}
		}

		return false;
	}

	/*
	 * Return array of currently running children
	 *
	 * @param string unique identifier for the work
	 * @param int $bucket the bucket
	 * @return bool true if child has work, false if not
	 */
	public function work_running($bucket = self::DEFAULT_BUCKET)
	{
		return $this->forked_children;
	}

	/**
	 * Return a list of the buckets which have been created
	 *
	 * @param bool $include_default_bucket optionally include self::DEFAULT_BUCKET in returned value (DEFAULT: true)
	 * @return array list of buckets
	 */
	public function bucket_list($include_default_bucket = true)
	{
		$bucket_list = array();

		foreach($this->buckets as $bucket_id)
		{
			// skip the default bucket if ignored
			if ( ($include_default_bucket === false) && ($bucket_id === self::DEFAULT_BUCKET) )
				continue;

			$bucket_list[] = $bucket_id;
		}

		return $bucket_list;
	}

	/**
	 * Check to see if a bucket exists
	 *
	 * @return bool true if the bucket exists, false if it does not
	 */
	public function bucket_exists($bucket_id)
	{
		return (array_key_exists($bucket_id, $this->buckets));
	}

	/**
	 * Return the number of work sets queued
	 *
	 * A work set is a chunk of items to be worked on.  A whole work set
	 * is handed off to a child processes.  This size of the work sets can
	 * be controlled by $this->max_work_per_child_set()
	 *
	 * @param int $bucket the bucket to use
	 * @param bool $process_all_buckets if set to true, return the count of all buckets
	 * @return int the number of work sets queued
	 */
	public function work_sets_count($bucket = self::DEFAULT_BUCKET, $process_all_buckets = false)
	{
		// if asked to process all buckets, count all of them and return the count
		if ($process_all_buckets === true)
		{
			$count = 0;
			foreach($this->buckets as $bucket_slot)
			{
				$count += count($this->work_units[$bucket_slot]);
			}
			return $count;
		}

		return count($this->work_units[$bucket]);
	}

	/**
	 * Return the contents of work sets queued
	 *
	 * A work set is a chunk of items to be worked on.  A whole work set
	 * is handed off to a child processes.  This size of the work sets can
	 * be controlled by $this->max_work_per_child_set()
	 *
	 * @param int $bucket the bucket to use
	 * @return array contents of  the bucket
	 */
	public function work_sets($bucket = self::DEFAULT_BUCKET)
	{
		return $this->work_units[$bucket];
	}

	/**
	 * Return the number of children running
	 *
	 * @param int $bucket the bucket to use
	 * @return int the number of children running
	 */
	public function children_running($bucket = self::DEFAULT_BUCKET)
	{
		// force reaping of children
		$this->signal_handler_sigchild(SIGCHLD);

		// return global count if bucket is default
		if ($bucket == self::DEFAULT_BUCKET)
			return count($this->forked_children);

		// count within the specified bucket
		$count = 0;
		foreach ($this->forked_children as $child)
		{
			if ($child['bucket'] == $bucket)
				$count++;
		}
		return $count;
	}
	
	/**
	 * Check if the current processes is a child
	 *
	 * @return bool true if the current PID is a child PID, false otherwise
	 */
	static public function is_child()
	{
		return (isset(self::$parent_pid) ? (self::$parent_pid != getmypid()) : false);
	}

	/**
	 * Spawns a helper process
	 *
	 * Spawns a new helper process to perform duties under the parent server
	 * process without accepting connections. Helper processes can optionally
	 * be respawned when they die.
	 *
	 * @access public
	 * @param string $function_name helper function to call
	 * @param array $arguments function arguments
	 * @param string $identifier helper process unique identifier
	 * @param bool $respawn whether to respawn the helper process when it dies
	 */
	public function helper_process_spawn($function_name, $arguments, $idenfifier = '', $respawn = true)
	{
		if ((is_array($function_name) && method_exists($function_name[0], $function_name[1])) || function_exists($function_name))
		{
			// do not process signals while we are forking
			declare(ticks = 0);
			$pid = pcntl_fork();

			if ($pid == -1)
			{
				die("Forking error!\n");
			}
			elseif ($pid == 0)
			{
				declare(ticks = 1);

				Log::message('Calling function ' . $function_name, LOG_LEVEL_DEBUG);
				call_user_func_array($function_name, $arguments);
				exit(0);
			}
			else
			{
				declare(ticks = 1);
				Log::message('Spawned new helper process with pid ' . $pid, LOG_LEVEL_INFO);

				$this->forked_children[$pid] = array(
					'ctime' => time(),
					'identifier' => $idenfifier,
					'status' => self::HELPER,
					'bucket' => self::DEFAULT_BUCKET,
					'respawn' => true,
					'function' => $function_name,
					'arguments' => $arguments,
				);
			}
		}
		else
		{
			Log::message("Unable to spawn undefined helper function '" . $function_name . "'", LOG_LEVEL_ERR);
		}
	}

	/**
	 * Forces a helper process to respawn
	 *
	 * @param string $identifier id of the helper process to respawn
	 */
	public function helper_process_respawn($identifier)
	{
		if ($identifier == '') return false;

		foreach ($this->forked_children as $pid => $child)
		{
			if ($child['status'] == self::HELPER && $child['identifier'] == $identifier)
			{
				Log::message('Forcing helper process \'' . $identifier . '\' with pid ' . $pid . ' to respawn', LOG_LEVEL_INFO);
				posix_kill($pid, SIGKILL);
			}
		}
	}

	/**
	 * Process work on the work queue
	 *
	 * This function will take work sets and hand them off to children.
	 * Part of the process is calling invoking fork_work_unit to fork
	 * off the child. If $blocking is set to true, this function will
	 * process all work units and wait until the children are done until
	 * returning.  If $blocking is set to false, this function will
	 * start as many work units as max_children allows and then return.
	 *
	 * Note, if $blocking is turned off, the caller has to handle when
	 * the children are done with their current load.
	 *
	 * @param bool true for blocking mode, false for immediate return
	 * @param int $bucket the bucket to use
	 */
	public function process_work($blocking = true, $bucket = self::DEFAULT_BUCKET, $process_all_buckets = false)
	{
		$this->housekeeping_check();

		// process work on all buckets if desired
		if ($process_all_buckets === true)
		{
			foreach($this->buckets as $bucket_slot)
			{
				$this->process_work($blocking, $bucket_slot, false);
			}
			return true;
		}

		// if room fork children
		if ($blocking === true)
		{
			// process work until completed
			while ($this->work_sets_count($bucket) > 0)
			{
				// check to make sure we have not hit or exceded the max children (globally or within the bucket)
				while ( $this->children_running($bucket) >= $this->max_children[$bucket] )
				{
					$this->housekeeping_check();
					$this->signal_handler_sigchild(SIGCHLD);
					sleep(1);
				}

				$this->process_work_unit($bucket);
			}

			// wait until work finishes
			while ($this->children_running($bucket) > 0)
			{
				sleep(1);
				$this->housekeeping_check();
				$this->signal_handler_sigchild(SIGCHLD);
			}

			// make call back to parent exit function if it exists
			$this->invoke_callback($this->parent_function_exit, $parameters = array(self::$parent_pid), true);
		}
		else
		{
			// fork children until max
			while ( $this->children_running($bucket) < $this->max_children[$bucket] )
			{
				if ($this->work_sets_count($bucket) == 0)
					return true;

				$this->process_work_unit($bucket);
			}
		}

		return true;
	}

	/**
	 * Pulls items off the work queue for processing
	 *
	 * Process the work queue by taking up to max_work_per_child items
	 * off the queue. A new child is then spawned off to process the
	 * work.
	 *
	 * @param int $bucket the bucket to use
	 */
	private function process_work_unit($bucket = self::DEFAULT_BUCKET)
	{
		$child_work_units = array_splice($this->work_units[$bucket], 0, $this->max_work_per_child[$bucket]);

		if (count($child_work_units) > 0)
		{
			if ($this->child_single_work_item[$bucket])
			{
				// break out identifier and unit
				list($child_identifier, $child_work_unit) = each($child_work_units);

				// strip preceeding 'id-' from the identifier
				if (strpos($child_identifier, 'id-') === 0)
					$child_identifier = substr($child_identifier, 3);

				// process work unit
				$this->fork_work_unit(array($child_work_unit, $child_identifier), $child_identifier, $bucket);
			}
			else
			{
				$this->fork_work_unit(array($child_work_units), '', $bucket);
			}
		}
	}

	/**
	 * Fork one child with one unit of work
	 *
	 * Given a work unit array, fork a child and hand
	 * off the work unit to the child.
	 *
	 * @param mixed $work_unit an array of work to process
	 * @param string a unique identifier for this work
	 * @param int $bucket the bucket to use
	 * @return mixed the child pid on success or boolean false on failure
	 */
	private function fork_work_unit($work_unit, $identifier = '', $bucket = self::DEFAULT_BUCKET)
	{
		// clear all database connections if the database engine is enabled
		if (function_exists('db_clear_connection_cache'))
			db_clear_connection_cache();

		// clear all memcache connections if memcache is in use
		if (function_exists('memcache_clear_connection_cache'))
			memcache_clear_connection_cache();

		// turn off signals temporarily to prevent a SIGCHLD from interupting the parent before $this->forked_children is updated
		declare(ticks = 0);

		// spoon!
		$pid = pcntl_fork();

		if ($pid == -1)
		{
			/**
			 * Fork Error
			 */

			$this->log('failed to fork', self::LOG_LEVEL_CRIT);
			return false;
		}
		elseif ($pid)
		{
			/**
			 * Parent Process
			 */

			// keep track of this pid in the parent
			$this->forked_children[$pid] = array(
				'ctime' => time(),
				'identifier' => $identifier,
				'bucket' => $bucket,
				'status' => self::WORKER,
			);

			// turn back on signals now that $this->forked_children has been updated
			declare(ticks = 1);

			// debug logging
			$this->log('forking child ' . $pid . ' for bucket ' . $bucket, self::LOG_LEVEL_DEBUG);

			// parent spawned child callback
			$this->invoke_callback($this->parent_function_fork[$bucket], $parameters = array($pid, $identifier), true);
		}
		else
		{
			/**
			 * Child Process
			 */

			// free up unneeded parent memory for child process
			$this->work_units = null;
			$this->forked_children = null;

			// set child properties
			$this->child_bucket = $bucket;

			// turn signals on for the child
			declare(ticks = 1);

			// re-seed the random generator to prevent clone from parent
			srand();

			// child run callback
			$this->invoke_callback($this->child_function_run[$bucket], $work_unit, false);

			// delay the child's exit slightly to avoid race conditions
			usleep(500);

			// exit after we complete one unit of work
			exit;
		}

		return $pid;
	}

	/**
	 * Performs house keeping every housekeeping_check_interval seconds
	 * @access private
	 */
	private function housekeeping_check()
	{
		if ((time() - $this->housekeeping_last_check) >= $this->housekeeping_check_interval)
		{
			// check to make sure no children are violating the max run time
			$this->kill_maxtime_violators();

			// look for zombie children just in case
			$this->signal_handler_sigchild(SIGCHLD);

			// update the last check time to now
			$this->housekeeping_last_check = time();
		}
	}

	/**
	 * Kills any children that have been running for too long.
	 * @access private
	 */
	private function kill_maxtime_violators()
	{
		foreach ($this->forked_children as $pid => $pid_info)
		{
			if ((time() - $pid_info['ctime']) > $this->child_max_run_time[$pid_info['bucket']])
			{
				$this->log('Force kill $pid has run too long', self::LOG_LEVEL_INFO);

				// notify app that child process timed out
				$this->invoke_callback($this->child_function_timeout{$pid_info['bucket']}, array($pid, $pid_info['identifier']), true);

				posix_kill($pid, SIGKILL); // its probably stuck on something, kill it immediately.
				sleep(3); // give the child time to die

				// force signal handling
				$this->signal_handler_sigchild(SIGCHLD);
			}
		}
	}

	/**
	 * Invoke a call back function with parameters
	 *
	 * Given the name of a function and parameters to send it, invoke the function.
	 * This function will try using the objects inherited function if it exists.  If not,
	 * it'll look for a declared function of the given name.
	 *
	 * @param string $function_name the name of the function to invoke
	 * @param array $parameters an array of parameters to pass to function
	 * @param bool $optional is set to true, don't error if function_name not available
	 * @return mixed false on error, otherwise return of callback function
	 */
	private function invoke_callback($function_name, $parameters, $optional = false)
	{
		// call child function
		if (is_array($function_name) && method_exists($function_name[0], $function_name[1]))
		{
			if (!is_array($parameters)) $parameters = array($parameters);
			return call_user_func_array($function_name, $parameters);
		}
		elseif (method_exists($this, $function_name) )
		{
			if (!is_array($parameters)) $parameters = array($parameters);
			return call_user_func_array($this->$function_name, $parameters);
		}
		else if (function_exists($function_name))
		{
			if (!is_array($parameters)) $parameters = array($parameters);
			return call_user_func_array($function_name, $parameters);
		}
		else
		{
			if ($optional === false)
				$this->log("Error there are no functions declared in scope to handle callback for function '" . $function_name . "'", self::LOG_LEVEL_CRIT);
		}
	}

	/**
	 * Log a message
	 *
	* @param string $message the text to log
	* @param int $severity the severity of the message
	* @param bool true on success, false on error
	 */
	private function log($message, $severity)
	{
		// TODO: add ability to set callback function to handle logging

		return true;
	}
}
