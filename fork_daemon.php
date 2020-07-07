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
	const STOPPED = 2;

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
	const LOG_LEVEL_ALL = -1;
	const LOG_LEVEL_CRIT = 2;
	const LOG_LEVEL_WARN = 4;
	const LOG_LEVEL_INFO = 6;
	const LOG_LEVEL_DEBUG = 7;

	/**
	 * Socket constants
	 *
	 * @access public
	 */
	const SOCKET_HEADER_SIZE = 4;

	/**
	 * Variables
	 */

	/**
	 * Maximum time in seconds a PID may execute
	 * @access protected
	 * @var integer $child_max_run_time
	 */
	protected $child_max_run_time = array(self::DEFAULT_BUCKET => 86400);

	/**
	 * Whether the bucket is in persisent mode or not.
	 *
	 * Persistent mode will keep $max_children[$bucket] processes running permanently
	 *
	 * @var array
	 */
	protected $child_persistent_mode = array(self::DEFAULT_BUCKET => false);

	/**
	 * The data that is passed to the child functions when in persistent mode
	 *
	 * @var array
	 */
	protected $child_persistent_mode_data = array(self::DEFAULT_BUCKET => null);

	/**
	 * Function the child invokes with a set of worker units
	 * @access protected
	 * @var integer $child_function_run
	 */
	protected $child_function_run = array(self::DEFAULT_BUCKET => '');

	/**
	 * Function the parent invokes when a child finishes
	 * @access protected
	 * @var integer $parent_function_child_exited
	 */
	protected $parent_function_child_exited = array(self::DEFAULT_BUCKET => '');

	/**
	 * Function the child invokes when sigint/term is received
	 * @access protected
	 * @var integer $child_function_exit
	 */
	protected $child_function_exit = array(self::DEFAULT_BUCKET => '');

	/**
	 * Function the parent invokes when a child is killed due to exceeding the max runtime
	 * @access protected
	 * @var integer $child_function_timeout
	 */
	protected $child_function_timeout = array(self::DEFAULT_BUCKET => '');

	/**
	 * Function the parent invokes before forking a child
	 * @access protected
	 * @var string[] $parent_function_prefork
	 */
	protected $parent_function_prefork = array();

	/**
	 * Function the parent invokes when a child is spawned
	 * @access protected
	 * @var integer $parent_function_fork
	 */
	protected $parent_function_fork = array(self::DEFAULT_BUCKET => '');

	/**
	 * Function the parent invokes when the parent receives a SIGHUP
	 * @access protected
	 * @var integer $parent_function_sighup
	 */
	protected $parent_function_sighup = '';

	/**
	 * Property of the parent sighup function.  If true, the parent
	 * will send sighup to all children when the parent receives a
	 * sighup.
	 * @access protected
	 * @var integer $parent_function_sighup_cascade
	 */
	protected $parent_function_sighup_cascade = true;

	/**
	 * Function the child invokes when the child receives a SIGHUP
	 * @access protected
	 * @var integer $child_function_sighup
	 */
	protected $child_function_sighup = array(self::DEFAULT_BUCKET => '');

	/**
	 * Function the parent invokes when a child has results to post
	 * @access protected
	 * @var integer $parent_function_results
	 */
	protected $parent_function_results = array(self::DEFAULT_BUCKET => '');

	/**
	 * Stores whether results are stored for retrieval by the parent
	 * @access protected
	 * @var boolean $store_result
	 */
	protected $store_result = false;

	/**
	 * Max number of seconds to wait for a child process
	 * exit once it has been requested to exit
	 * @access protected
	 * @var integer $children_kill_timeout
	 */
	protected $children_max_timeout = 30;

	/**
	 * Function the parent runs when the daemon is getting shutdown
	 * @access protected
	 * @var integer $parent_function_exit
	 */
	protected $parent_function_exit = '';

	/**
	 * Stores whether the daemon is in single item mode or not
	 * @access protected
	 * @var bool $child_single_work_item
	 */
	protected $child_single_work_item = array(self::DEFAULT_BUCKET => false);

	/**
	 * Function to call when there is a message to log
	 * @access protected
	 * @var array $log_function array of callables index by severity
	 * called with call_user_func($log_function, $message)
	 */
	protected $log_function = null;

	/**
	 * Stores whether or not we have received an exit request
	 * @access protected
	 * @default false
	 * @var bool $exit_request_status
	 */
	protected $exit_request_status = false;

	/**
	 * In the child process, stores the socket to get results back to the parent
	 * @var socket
	 */
	protected $child_socket_to_parent = null;

	/**************** SERVER CONTROLS ****************/
	/**
	 * Upper limit on the number of children started.
	 * @access protected
	 * @var integer $max_children
	 */
	protected $max_children = array(self::DEFAULT_BUCKET => 25);

	/**
	 * Upper limit on the number of work units sent to each child.
	 * @access protected
	 * @var integer $max_work_per_child
	 */
	protected $max_work_per_child = array(self::DEFAULT_BUCKET => 100);

	/**
	 * Interval to do house keeping in seconds
	 * @access protected
	 * @var integer $housekeeping_check_interval
	 */
	protected $housekeeping_check_interval = 20;

	/**************** TRACKING CONTROLS ****************/

	/**
	 * track children of parent including their status and create time
	 * @access protected
	 * @var array $forked_children
	 */
	protected $forked_children = array();

	/**
	 * number of tracked children (not stopped)
	 * @access protected
	 * @var array $forked_children_count
	 */
	protected $forked_children_count = 0;

	/**
	 * track the work units to process
	 * @access protected
	 * @var array $work_units
	 */
	protected $work_units = array(self::DEFAULT_BUCKET => array());

	/**
	 * track the buckets
	 * @access protected
	 * @var array $buckets
	 */
	protected $buckets = array(0 => self::DEFAULT_BUCKET);

	/**
	 * for the parent the track the results received from chilren
	 * @access protected
	 * @var array $work_units
	 */
	protected $results = array(self::DEFAULT_BUCKET => array());

	/**
	 * within a child, track the bucket the child exists in. note,
	 * this shouldn't be set or referenced in the parent process
	 * @access protected
	 * @var int $child_bucket
	 */
	protected $child_bucket = null;

	/**************** MOST IMPORTANT CONTROLS  ****************/

	/**
	 * parent pid
	 * @access protected
	 * @var array $parent_pid
	 */
	static protected $parent_pid;

	/**
	 * last housekeeping check time
	 * @access protected
	 * @var array $housekeeping_last_check
	 */
	protected $housekeeping_last_check = 0;

	/**************** FUNCTION DEFINITIONS  ****************/

	/**
	 * Set and Get functions
	 */

	/**
	 * @return array The forked children array.
	 */
	public function getForkedChildren()
	{
		return $this->forked_children;
	}

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

		$old = $this->max_children[$bucket];
		$this->max_children[$bucket] = $value;

		if ($this->child_persistent_mode[$bucket] && $old > $this->max_children[$bucket])
		{
			$difference = $old - $this->max_children[$bucket];
			$killed = 0;
			// Kill some of the child processes
			foreach ($this->forked_children as $pid => $child)
			{
				if ($child['bucket'] == $bucket && $child['status'] == self::WORKER)
				{
					$this->safe_kill($pid, SIGINT, "max_children lowered for bucket $bucket, killing pid $pid");
					$killed++;

					if ($killed == $difference)
					{
						break;
					}
				}
			}
		}
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
		if ($value == 0)
		{
			$this->log(($bucket === self::DEFAULT_BUCKET ? 'default' : $bucket) . ' bucket child_max_run_time set to 0', self::LOG_LEVEL_WARN);
		}
		elseif ($value < 0)
		{
			$value = -1;
			$this->log(($bucket === self::DEFAULT_BUCKET ? 'default' : $bucket) . ' bucket child_max_run_time set to unlimited', self::LOG_LEVEL_WARN);
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
	 * @param bool $persistent_mode whether or not the bucket should be in persistent mode
	 * @param int $bucket the bucket to configure
	 */
	public function child_persistent_mode_set($persistent_mode, $bucket = self::DEFAULT_BUCKET)
	{
		$this->child_persistent_mode[$bucket] = $persistent_mode;
	}

	/**
	 * @param mixed $data the data to be passed to the child processes while in persistent mode
	 * @param int $bucket the bucket to configure
	 */
	public function child_persistent_mode_data_set($data, $bucket = self::DEFAULT_BUCKET)
	{
		$this->child_persistent_mode_data[$bucket] = $data;
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
	 * Allows the app to retrieve the current child_single_work_item value.
	 * @access public
	 * @param int $bucket the bucket to use
	 * @return int the child_single_work_item value
	 */
	public function child_single_work_item_get($bucket = self::DEFAULT_BUCKET)
	{
		return($this->child_single_work_item[$bucket]);
	}

	/**
	 * Allows the app to set the store_result value
	 * @access public
	 * @param int $value new store_result value.
	 */
	public function store_result_set($value)
	{
		$this->store_result = $value;
	}

	/**
	 * Allows the app to retrieve the current store_result value.
	 * @access public
	 * @return boolean the store_result value
	 */
	public function store_result_get()
	{
		return $this->store_result;
	}

	/**
	 * Allows the app to retrieve the current child_bucket value.
	 * @access public
	 * @return int the child_bucket value representing the bucket number of the child
	 */
	public function child_bucket_get()
	{
		// this function does not apply to the parent
		if (self::$parent_pid == getmypid())
		{
			return false;
		}

		return($this->child_bucket);
	}


	/**
	 * Creates a new bucket to house forking operations
	 * @access public
	 * @param int $bucket the bucket to create
	 */
	public function add_bucket($bucket)
	{
		// create the bucket by copying values from the default bucket
		$this->max_children[$bucket] = $this->max_children[self::DEFAULT_BUCKET];
		$this->child_single_work_item[$bucket] = $this->child_single_work_item[self::DEFAULT_BUCKET];
		$this->max_work_per_child[$bucket] = $this->max_work_per_child[self::DEFAULT_BUCKET];
		$this->child_max_run_time[$bucket] = $this->child_max_run_time[self::DEFAULT_BUCKET];
		$this->child_single_work_item[$bucket] = $this->child_single_work_item[self::DEFAULT_BUCKET];
		$this->child_function_run[$bucket] = $this->child_function_run[self::DEFAULT_BUCKET];
		$this->parent_function_fork[$bucket] = $this->parent_function_fork[self::DEFAULT_BUCKET];
		$this->child_function_sighup[$bucket] = $this->child_function_sighup[self::DEFAULT_BUCKET];
		$this->child_function_exit[$bucket] = $this->child_function_exit[self::DEFAULT_BUCKET];
		$this->child_function_timeout[$bucket] = $this->child_function_timeout[self::DEFAULT_BUCKET];
		$this->parent_function_child_exited[$bucket] = $this->parent_function_child_exited[self::DEFAULT_BUCKET];
		$this->child_persistent_mode[$bucket] = $this->child_persistent_mode[self::DEFAULT_BUCKET];
		$this->child_persistent_mode_data[$bucket] = $this->child_persistent_mode_data[self::DEFAULT_BUCKET];
		$this->work_units[$bucket] = array();
		$this->buckets[$bucket] = $bucket;
		$this->results[$bucket] = array();
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
		// call child function
		if (is_callable($function_name) || ( is_array($function_name) && method_exists($function_name[0], $function_name[1]) ) || method_exists($this, $function_name) || function_exists($function_name) )
		{
			$this->child_function_run[$bucket] = $function_name;
			return true;
		}

		return false;
	}

	/**
	 * Allows the app to set call back functions to cleanup resources before forking
	 * @access public
	 * @param array names of functions to be called.
	 * @return bool true if the callback was successfully registered, false if it failed
	 */
	public function register_parent_prefork(array $function_names)
	{
		$this->parent_function_prefork = $function_names;
		return true;
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
		// call child function
		if ( is_callable($function_name) || ( is_array($function_name) && method_exists($function_name[0], $function_name[1]) ) || method_exists($this, $function_name) || function_exists($function_name) )
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
		// call child function
		if ( is_callable($function_name) || ( is_array($function_name) && method_exists($function_name[0], $function_name[1]) ) || method_exists($this, $function_name) || function_exists($function_name) )
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
		// call child function
		if ( is_callable($function_name) || ( is_array($function_name) && method_exists($function_name[0], $function_name[1]) ) || method_exists($this, $function_name) || function_exists($function_name) )
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
		// call child function
		if ( is_callable($function_name) || ( is_array($function_name) && method_exists($function_name[0], $function_name[1]) ) || method_exists($this, $function_name) || function_exists($function_name) )
		{
			$this->child_function_exit[$bucket] = $function_name;
			return true;
		}

		return false;
	}

	/**
	 * Allows the app to set the call back function for when a child process is killed for exceeding its max runtime
	 * @access public
	 * @param string name of function to be called.
	 * @param int $bucket the bucket to use
	 * @return bool true if the callback was successfully registered, false if it failed
	 */
	public function register_child_timeout($function_name, $bucket = self::DEFAULT_BUCKET)
	{
		// call child function
		if ( is_callable($function_name) || ( is_array($function_name) && method_exists($function_name[0], $function_name[1]) ) || method_exists($this, $function_name) || function_exists($function_name) )
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
		if ( is_callable($function_name) || ( is_array($function_name) && method_exists($function_name[0], $function_name[1]) ) || method_exists($this, $function_name) || function_exists($function_name) )
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
		// call parent function
		if ( is_callable($function_name) || ( is_array($function_name) && method_exists($function_name[0], $function_name[1]) ) || method_exists($this, $function_name) || function_exists($function_name) )
		{
			$this->parent_function_child_exited[$bucket] = $function_name;
			return true;
		}

		return false;
	}

	/**
	 * Allows the app to set the call back function for when the a child has results
	 * @access public
	 * @param string name of function to be called.
	 * @return bool true if the callback was successfully registered, false if it failed
	 */
	public function register_parent_results($function_name, $bucket = self::DEFAULT_BUCKET)
	{
		// call parent function
		if ( is_callable($function_name) || ( is_array($function_name) && method_exists($function_name[0], $function_name[1]) ) || method_exists($this, $function_name) || function_exists($function_name) )
		{
			$this->parent_function_results[$bucket] = $function_name;
			return true;
		}

		return false;
	}

	/**
	 * Allows the app to set the call back function for logging
	 * @access public
	 * @param callable name of function to be called.
	 * @param int $severity the severity level
	 * @return bool true if the callback was successfully registered, false if it failed
	 */
	public function register_logging($function_name, $severity)
	{
		// call parent function
		if ( is_callable($function_name) || ( is_array($function_name) && method_exists($function_name[0], $function_name[1]) ) || method_exists($this, $function_name) || function_exists($function_name) )
		{
			$this->log_function[$severity] = $function_name;
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
		// record pid of parent process
		self::$parent_pid = getmypid();

		// install signal handlers
		pcntl_async_signals(true);
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
					if ($pid_info['status'] == self::STOPPED)
						continue;
					$this->safe_kill($pid, SIGHUP, 'parent process [' . getmypid() . '] sending sighup to child ' . $pid, self::LOG_LEVEL_DEBUG);
				}
			}
		}
		else
		{
			// child received sighup. note a child is only in one bucket, do not loop through all buckets
			if (isset($this->child_bucket) && isset($this->child_function_sighup[$this->child_bucket]))
			{
				$this->log('child process [' . getmypid() . '] received sighup with bucket type [' . $this->child_bucket . ']', self::LOG_LEVEL_DEBUG);
				$this->invoke_callback(
					$this->child_function_sighup[$this->child_bucket],
					array($this->child_bucket),
					true
				);
			}
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
		// reap all child zombie processes
		if (self::$parent_pid == getmypid())
		{
			// do not allow signals to interrupt this
			pcntl_async_signals(false);

			$status = '';

			do
			{
				// get child pid that exited
				$child_pid = pcntl_waitpid(0, $status, WNOHANG);
				if ($child_pid > 0)
				{
					// child exited
					if (!isset($this->forked_children[$child_pid]))
					{
						$this->log("Cannot find $child_pid in array! (This may be a subprocess not in our fork)", self::LOG_LEVEL_INFO);
						continue;
					}

					$child = $this->forked_children[$child_pid];
					$identifier = $child['identifier'];

					// call exit function if and only if its declared */
					if ($child['status'] == self::WORKER)
						$this->invoke_callback($this->parent_function_child_exited[ $this->forked_children[$child_pid]['bucket'] ], array($child_pid, $this->forked_children[$child_pid]['identifier']), true);

					// stop the child pid
					$this->forked_children[$child_pid]['status'] = self::STOPPED;
					$this->forked_children_count--;

					// respawn helper processes
					if ($child['status'] == self::HELPER && $child['respawn'] === true)
					{
						$this->log('Helper process ' . $child_pid . ' died, respawning', self::LOG_LEVEL_INFO);
						$this->helper_process_spawn($child['function'], $child['arguments'], $child['identifier'], true);
					}

					// Poll for results from any children
					$this->post_results($child['bucket']);
				}
				elseif ($child_pid < 0)
				{
					// ignore acceptable error 'No child processes' given we force this signal to run potentially when no children exist
					if (pcntl_get_last_error() == 10) continue;

					// pcntl_wait got an error
					$this->log('pcntl_waitpid failed with error ' . pcntl_get_last_error() . ':' . pcntl_strerror((pcntl_get_last_error())), self::LOG_LEVEL_DEBUG);
				}
			}
			while ($child_pid > 0);

			// turn signals back on
			pcntl_async_signals(true);
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
		// log that we received an exit request
		$this->received_exit_request(true);

		// kill child processes
		if (self::$parent_pid == getmypid())
		{
			foreach ($this->forked_children as $pid => &$pid_info)
			{
				if ($pid_info['status'] == self::STOPPED)
					continue;

				// tell helpers not to respawn
				if ($pid_info['status'] == self::HELPER)
					$pid_info['respawn'] = false;

				$this->safe_kill($pid, SIGINT, 'requesting child exit for pid: ' . $pid, self::LOG_LEVEL_INFO);
			}

			sleep(1);

			// checking for missed sigchild
			$this->signal_handler_sigchild(SIGCHLD);

			$start_time = time();

			// wait for child processes to go away
			while ($this->forked_children_count > 0)
			{
				if (time() > ($start_time + $this->children_max_timeout))
				{
					foreach ($this->forked_children as $pid => $child)
					{
						if ($child['status'] == self::STOPPED)
							continue;

						$this->safe_kill($pid, SIGKILL, 'force killing child pid: ' . $pid, self::LOG_LEVEL_INFO);

						// stop the child
						$this->forked_children[$pid]['status'] = self::STOPPED;
						$this->forked_children_count--;
					}
				}
				else
				{
					$this->log('waiting ' . ($start_time + $this->children_max_timeout - time()) . ' seconds for ' . $this->forked_children_count . ' children to clean up', self::LOG_LEVEL_INFO);
					sleep(1);
					$this->housekeeping_check();
				}
			}

			// make call back to parent exit function if it exists
			$this->invoke_callback($this->parent_function_exit, $parameters = array(self::$parent_pid, $signal_number), true);
		}
		else
		{
			// invoke child cleanup callback
			if (isset($this->child_bucket))
			{
				$this->invoke_callback($this->child_function_exit[$this->child_bucket], $parameters = array($this->child_bucket), true);
			}
		}

		exit(-1);
	}

	/**
	 * Check or set if we have recieved an exit request
	 *
	 * @param boolen $requested (optional) have we received the request
	 * @return current exit request status
	 */
	public function received_exit_request($requested = null)
	{
		// if we are retreiving the value of the exit request
		if ($requested === null)
		{
			return $this->exit_request_status;
		}

		// ensure we have good data, or set to false if not
		if (! is_bool($requested))
		{
			$requested = false;
		}

		// set and return the ne value
		return ($this->exit_request_status = $requested);
	}

	/**
	 * Add work to the group of work to be processed
	 *
	 * @param array $new_work_units mixed array of items to be handed back to child in chunks
	 * @param string $identifier a unique identifier for this work
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

	/**
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
			if (($info['status'] != self::STOPPED) && ($info['identifier'] == $identifier) && ($info['bucket'] == $bucket))
			{
				return true;
			}
		}

		return false;
	}

	/**
	 * Return array of currently running children
	 *
	 * @param int $bucket the bucket
	 * @return bool true if child has work, false if not
	 */
	public function work_running($bucket = self::DEFAULT_BUCKET)
	{
		$results = array();
		foreach ($this->forked_children as $pid => $child)
		{
			if ($child['status'] != self::STOPPED && $child['bucket'] === $bucket)
			{
				$results[$pid] = $child;
			}
		}

		return $results;
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
	 * @param bool $show_pending True to show children that are done,
	 * 		but not yet had their results retrieved
	 * @return int the number of children running
	 */
	public function children_running($bucket = self::DEFAULT_BUCKET, $show_pending = false)
	{
		// force reaping of children
		$this->signal_handler_sigchild(SIGCHLD);

		// return global count if bucket is default
		if ($bucket == self::DEFAULT_BUCKET)
			return ($show_pending ? count($this->forked_children) : $this->forked_children_count);

		// count within the specified bucket
		$count = 0;
		foreach ($this->forked_children as $child)
		{
			if ($show_pending)
			{
				if ($child['bucket'] == $bucket)
					$count++;
			}
			else if (($child['bucket'] == $bucket) && ($child['status'] != self::STOPPED))
			{
				$count++;
			}
		}

		return $count;
	}

	/**
	 * Returns the number of pending child items, including running children and
	 * work sets that have not been allocated.  Children running includes those
	 * that have not had their results retrieved yet.
	 *
	 * @param type $bucket The bucket to check for pending children items
	 * @return int Number of pending children items
	 */
	public function children_pending($bucket = self::DEFAULT_BUCKET)
	{
		return $this->children_running($bucket, true) + $this->work_sets_count($bucket);
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
	 * Try to kill a given process and make sure it is safe to kill
	 *
	 * @param int $pid the PID to check if it is our child
	 * @param int $signal the kill signal to send to the given pid if possible
	 * @param string $log_message the message to log out upon success
	 * @param int $log_level the level at which to display the log_message
	 * @return bool true on successful kill, false if not our child or not able to kill
	 */
	public function safe_kill($pid, $signal, $log_message = '', $log_level = self::LOG_LEVEL_INFO)
	{
		if (!array_key_exists($pid, $this->forked_children))
		{
			return false;
		}

		$stat_pid_file = '/proc/' . $pid . '/stat';
		if (!file_exists($stat_pid_file))
		{
			$this->log('Unable to find info for PID ' . $pid . ' from ' . $stat_pid_file, self::LOG_LEVEL_DEBUG);
			return false;
		}

		$stat_pid_info = file_get_contents($stat_pid_file);
		if ($stat_pid_info === false)
		{
			$this->log('Unable to get info for PID ' . $pid, self::LOG_LEVEL_DEBUG);
			return false;
		}

		$stat_pid_info = explode(' ', $stat_pid_info);
		if (!array_key_exists(3, $stat_pid_info))
		{
			$this->log('Unable to find parent PID for PID ' . $pid, self::LOG_LEVEL_DEBUG);
			return false;
		}

		// the parent pid is the fourth entry in /proc/PID/stat
		if ($stat_pid_info[3] == getmypid())
		{
			if ($log_message)
			{
				$this->log($log_message, $log_level);
			}

			posix_kill($pid, $signal);
			return true;
		}

		$this->log('Failed to kill PID ' . $pid . ' with signal ' . $signal, self::LOG_LEVEL_WARN);
		return false;
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
	public function helper_process_spawn($function_name, $arguments = array(), $identifier = '', $respawn = true)
	{
		if ((is_array($function_name) && method_exists($function_name[0], $function_name[1])) || function_exists($function_name))
		{
			// init the IPC sockets
			list($socket_child, $socket_parent) = $this->ipc_init();

			// do not process signals while we are forking
			pcntl_async_signals(false);
			$pid = pcntl_fork();

			if ($pid == -1)
			{
				die("Forking error!\n");
			}
			elseif ($pid == 0)
			{
				/*
				 * Child process
				 */

				// set child properties
				$this->child_bucket = self::DEFAULT_BUCKET;

				// turn signals back on
				pcntl_async_signals(true);

				// close our socket (we only need the one to the parent)
				socket_close($socket_child);

				// execute the function
				$result = call_user_func_array($function_name, $arguments);

				// send the response to the parent
				self::socket_send($socket_parent, $result);

				exit(0);
			}
			else
			{
				/*
				 * Parent process
				 */

				// turn signals back on
				pcntl_async_signals(true);

				$this->log('Spawned new helper process with pid ' . $pid, self::LOG_LEVEL_INFO);

				// close our socket (we only need the one to the child)
				socket_close($socket_parent);

				// track the child
				$this->forked_children[$pid] = array(
					'ctime' => time(),
					'identifier' => $identifier,
					'status' => self::HELPER,
					'bucket' => self::DEFAULT_BUCKET,
					'respawn' => true,
					'function' => $function_name,
					'arguments' => $arguments,
					'socket' => $socket_child,
					'last_active' => microtime(true),
				);
				$this->forked_children_count++;
			}
		}
		else
		{
			$this->log("Unable to spawn undefined helper function '" . $function_name . "'", self::LOG_LEVEL_CRIT);
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
				$this->safe_kill($pid, SIGKILL, 'Forcing helper process \'' . $identifier . '\' with pid ' . $pid . ' to respawn', self::LOG_LEVEL_INFO);
			}
		}
	}

	/**
	 * Kill a specified child(ren) by pid
	 *
	 * Note: This method will block until all requested pids have exited
	 *
	 * @param int $pids the child pid to kill
	 * @param int $kill_delay how many seconds to wait before sending sig kill on stuck processes
	 * @access public
	 */
	public function kill_child_pid($pids, $kill_delay = 30)
	{
		if (!is_array($pids))
		{
			$pids = array($pids);
		}

		// send int sigs to the children
		foreach ($pids as $index => $pid)
		{
			// make sure we own this pid
			if (!array_key_exists($pid, $this->forked_children) || $this->forked_children[$pid]['status'] == self::STOPPED)
			{
				$this->log('Skipping kill request on pid ' . $pid . ' because we dont own it', self::LOG_LEVEL_INFO);
				unset($pids[$index]);
				continue;
			}

			$this->safe_kill($pid, SIGINT, 'Asking pid ' . $pid . ' to exit via sigint', self::LOG_LEVEL_INFO);
		}

		// store the requst time
		$request_time = microtime(true);
		$time = 0;

		// make sure the children exit
		while ((count($pids) > 0) && ($time < $kill_delay))
		{
			foreach ($pids as $index => $pid)
			{
				// check if the pid exited gracefully
				if (!array_key_exists($pid, $this->forked_children) || $this->forked_children[$pid]['status'] == self::STOPPED)
				{
					$this->log('Pid ' . $pid . ' has exited gracefully', self::LOG_LEVEL_INFO);
					unset($pids[$index]);
					continue;
				}

				$time = microtime(true) - $request_time;
				if ($time < $kill_delay)
				{
					$this->log('Waiting ' . ($kill_delay - round($time, 0)) . ' seconds for ' . count($pids) . ' to exit gracefully', self::LOG_LEVEL_INFO);
					sleep(1);
					continue;
				}

				$this->safe_kill($pid, SIGKILL, 'Force killing pid ' . $pid, self::LOG_LEVEL_INFO);
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
				if (!$this->child_persistent_mode[$bucket] && $this->work_sets_count($bucket) == 0)
					return true;

				$this->process_work_unit($bucket);
			}
		}

		return true;
	}

	/**
	 * Returns the first result available from the bucket.  This will run
	 * a non-blocking poll of the children for updated results.
	 *
	 * @param string $bucket The bucket to check
	 * @return mixed The data retrieved from a child process on the buckets
	 */
	public function get_result($bucket = self::DEFAULT_BUCKET)
	{
		// check for additional results
		$this->post_results($bucket);

		if (! $this->has_result($bucket))
			return null;

		return array_shift($this->results[$bucket]);
	}

	/**
	 * Returns all the results currently in the results queue.  This will
	 * run a non-blocking poll of the children for updated results.
	 *
	 * @param string $bucket The bucket to retrieves results
	 * @return mixed Array of results from each child that has finished.
	 */
	public function get_all_results($bucket = self::DEFAULT_BUCKET)
	{
		// check for additional results
		$this->post_results($bucket);

		if (! $this->has_result($bucket))
			return array();

		$results = $this->results[$bucket];
		$this->results[$bucket] = array();

		return $results;
	}

	/**
	 * Checks if there is a result on the bucket.  Before checking,
	 * runs a non-blocking poll of the children for updated results.
	 *
	 * @param string $bucket The bucket to check
	 * @return int Returns true if there is a result
	 */
	public function has_result($bucket = self::DEFAULT_BUCKET)
	{
		// check for additional results
		$this->post_results($bucket);

		return (! empty($this->results[$bucket]));
	}

	/**
	 * Send a message from the child to the parent
	 * @param string $result
	 */
	public function child_send_result_to_parent($result)
	{
		self::socket_send($this->child_socket_to_parent, $result);
	}

	/**
	 * Checks if any changed child sockets are in the bucket.
	 *
	 * @param type $bucket The bucket to get results in
	 * @return type Returns the number of changed sockets for children workers in $bucket,
	 * or empty array if none.
	 */
	protected function get_changed_sockets($bucket = self::DEFAULT_BUCKET, $timeout = 0)
	{
		$write_dummy = null;
		$exception_dummy = null;

		// grab all the children sockets
		$sockets = array();
		foreach ($this->forked_children as $pid => $child)
		{
			if ($child['bucket'] == $bucket)
				$sockets[$pid] = $child['socket'];
		}

		if (! empty($sockets))
		{
			// find changed sockets and return the array of them
			$result = @socket_select($sockets, $write_dummy, $exception_dummy, $timeout);
			if ($result !== false && $result > 0)
				return $sockets;
		}

		return null;
	}

	/**
	 * Returns any pending results from the child sockets.  If a
	 * child has no results and it has status self::STOPPED, this will remove
	 * the child record from $this->forked_children.
	 *
	 * NOTE: This must be polled to check for changed sockets.
	 *
	 * @param type $blocking Set to true to block until a result comes in
	 * @param type $bucket The bucket to look in
	 * @return type The result of the child worker
	 */
	protected function fetch_results($blocking = true, $timeout = 0, $bucket = self::DEFAULT_BUCKET)
	{
		// turn signals off while processing results
		pcntl_async_signals(false);

		$start = microtime(true);
		$results = array();

		// loop while there is pending children and pending sockets; this
		// will break early on timeouts and when not blocking.
		do
		{
			$ready_sockets = $this->get_changed_sockets($bucket, $timeout);
			if (is_array($ready_sockets))
			{
				foreach ($ready_sockets as $pid => $socket)
				{
					$result = $this->socket_receive($socket);
					if ($result !== false && (! is_null($result)))
					{
						$this->forked_children[$pid]['last_active'] = $start;
						$results[$pid] = $result;
					}
				}
			}

			// clean up forked children that have stopped and did not have recently
			// active sockets.
			foreach ($this->forked_children as $pid => &$child)
			{
				if (isset($child['last_active']) && ($child['last_active'] < $start) && ($child['status'] == self::STOPPED))
				{
					// close the socket from the parent
					unset($this->forked_children[$pid]);
				}
			}
			unset($child);

			// check if timed out
			if ($timeout && (microtime(true) - $start > $timeout))
			{
				pcntl_async_signals(true);
				return $results;
			}

			// return null if not blocking and we haven't seen results
			if (! $blocking)
			{
				pcntl_async_signals(true);
				return $results;
			}
		}
		while (count($this->forked_children) > 0);

		pcntl_async_signals(true);
		return $results;
	}

	/**
	 * Posts any new results to a callback function if one is available, or stores
	 * them to the internal results storage if not.  This does not block and will
	 * post any results that are available, so call while children are running
	 * to check and post more results.
	 *
	 * NOTE: This should be polled to update results.
	 *
	 * @param type $bucket The bucket to post the results in
	 * @return type Returns true on successfully posting results, even if none
	 * to post.  Returns false on error from this function or error from
	 * the $this->parent_function_results callback.
	 */
	protected function post_results($bucket = self::DEFAULT_BUCKET)
	{
		// fetch all the results up to this point
		$results = $this->fetch_results(false, 0, $bucket);
		if (is_array($results) && empty($results))
			return true;

		if (! empty($this->parent_function_results[$bucket]))
		{
			if ($this->invoke_callback($this->parent_function_results[$bucket], array($results), true) === false)
				return false;
		}
		elseif ($this->store_result === true)
		{
			$this->results[$bucket] += $results;
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
	protected function process_work_unit($bucket = self::DEFAULT_BUCKET)
	{
		$child_work_units = array_splice($this->work_units[$bucket], 0, $this->max_work_per_child[$bucket]);

		if ($this->child_persistent_mode[$bucket])
		{
			$data = isset($this->child_persistent_mode_data[$bucket]) ? $this->child_persistent_mode_data[$bucket] : null;
			$this->fork_work_unit($data, '', $bucket);
		}
		elseif (count($child_work_units) > 0)
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

		// Poll for results from children
		$this->post_results($bucket);
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
	protected function fork_work_unit($work_unit, $identifier = '', $bucket = self::DEFAULT_BUCKET)
	{
		// prefork callback
		foreach ($this->parent_function_prefork as $function)
		{
			$this->invoke_callback($function, array(), true);
		}

		// init the IPC sockets
		list($socket_child, $socket_parent) = $this->ipc_init();

		// turn off signals temporarily to prevent a SIGCHLD from interupting the parent before $this->forked_children is updated
		pcntl_async_signals(false);

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
				'socket' => $socket_child,
				'last_active' => microtime(true),
			);
			$this->forked_children_count++;

			// turn back on signals now that $this->forked_children has been updated
			pcntl_async_signals(true);

			// close our socket (we only need the one to the child)
			socket_close($socket_parent);

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
			$this->results = null;

			// save the socket from child to parent to support $this->child_send_result_to_parent()
			$this->child_socket_to_parent = $socket_parent;

			// set child properties
			$this->child_bucket = $bucket;

			// turn signals on for the child
			pcntl_async_signals(true);

			// close our socket (we only need the one to the parent)
			socket_close($socket_child);

			// re-seed the random generator to prevent clone from parent
			srand();

			// child run callback
			$result = $this->invoke_callback($this->child_function_run[$bucket], $work_unit, false);

			// send the result to the parent
			if (is_null($result))
			{
				self::socket_send($socket_parent, $result);
			}

			// delay the child's exit slightly to avoid race conditions
			usleep(500);

			// exit after we complete one unit of work
			exit;
		}

		return $pid;
	}

	/**
	 * Performs house keeping every housekeeping_check_interval seconds
	 * @access protected
	 */
	protected function housekeeping_check()
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
	 * @access protected
	 */
	protected function kill_maxtime_violators()
	{
		foreach ($this->forked_children as $pid => $pid_info)
		{
			if ($pid_info['status'] == self::STOPPED)
				continue;

			if ($this->child_max_run_time[$pid_info['bucket']] >= 0 && (time() - $pid_info['ctime']) > $this->child_max_run_time[$pid_info['bucket']])
			{
				$this->log('Force kill ' . $pid . ' has run too long', self::LOG_LEVEL_INFO);

				// notify app that child process timed out
				$this->invoke_callback($this->child_function_timeout[$pid_info['bucket']], array($pid, $pid_info['identifier']), true);

				$this->safe_kill($pid, SIGKILL); // its probably stuck on something, kill it immediately.
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
	 * @access protected
	 * @param string $function_name the name of the function to invoke
	 * @param array $parameters an array of parameters to pass to function
	 * @param bool $optional is set to true, don't error if function_name not available
	 * @return mixed false on error, otherwise return of callback function
	 */
	protected function invoke_callback($function_name, $parameters, $optional = false)
	{
		// call child function
		if (is_callable($function_name))
		{
			if (!is_array($parameters)) $parameters = array($parameters);
			return call_user_func_array($function_name, $parameters);
		}
		elseif (is_array($function_name) && method_exists($function_name[0], $function_name[1]))
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
	 * Initialize interprocess communication by setting up a pair
	 * of sockets and returning them as an array.
	 *
	 * @return type
	 */
	protected function ipc_init()
	{
		// windows needs AF_INET
		$domain = strtoupper(substr(PHP_OS, 0, 3)) == 'WIN' ? AF_INET : AF_UNIX;

		// create a socket pair for IPC
		$sockets = array();
		if (socket_create_pair($domain, SOCK_STREAM, 0, $sockets) === false)
		{
			$this->log('socket_create_pair failed: ' . socket_strerror(socket_last_error()), self::LOG_LEVEL_CRIT);
			return false;
		}

		// return the sockets
		return $sockets;
	}

	/**
	 * Sends a serializable message to the socket.
	 *
	 * @param type $socket The socket to send the message on
	 * @param type $message The serializable message to send
	 * @return type Returns true on success, false on failure
	 */
	protected function socket_send($socket, $message)
	{
		// do not process signals while we are sending an IPC message
		pcntl_async_signals(false);

		$serialized_message = @serialize($message);
		if ($serialized_message == false)
		{
			$this->log('socket_send failed to serialize message', self::LOG_LEVEL_CRIT);
			pcntl_async_signals(true);
			return false;
		}

		$header = pack('N', strlen($serialized_message));
		$data = $header . $serialized_message;
		$bytes_left = strlen($data);
		while ($bytes_left > 0)
		{
			$bytes_sent = @socket_write($socket, $data);
			if ($bytes_sent === false)
			{
				$this->log('socket_send error: ' . socket_strerror(socket_last_error()), self::LOG_LEVEL_CRIT);
				pcntl_async_signals(true);
				return false;
			}

			$bytes_left -= $bytes_sent;
			$data = substr($data, $bytes_sent);
		}

		pcntl_async_signals(true);
		return true;
	}

	/**
	 * Receives a serialized message from the socket.
	 *
	 * @param type $socket Thes socket to receive the message from
	 * @return type Returns true on success, false on failure
	 */
	protected function socket_receive($socket)
	{
		// do not process signals while we are receiving an IPC message
		pcntl_async_signals(false);

		// initially read to the length of the header size, then
		// expand to read more
		$bytes_total = self::SOCKET_HEADER_SIZE;
		$bytes_read = 0;
		$have_header = false;
		$socket_message = '';
		while ($bytes_read < $bytes_total)
		{
			$read = @socket_read($socket, $bytes_total - $bytes_read);
			if ($read === false)
			{
				$this->log('socket_receive error: ' . socket_strerror(socket_last_error()), self::LOG_LEVEL_CRIT);
				pcntl_async_signals(true);
				return false;
			}

			// blank socket_read means done
			if ($read == '')
				break;

			$bytes_read += strlen($read);
			$socket_message .= $read;

			if (!$have_header && $bytes_read >= self::SOCKET_HEADER_SIZE)
			{
				$have_header = true;
				list($bytes_total) = array_values(unpack('N', $socket_message));
				$bytes_read = 0;
				$socket_message = '';
			}
		}

		$message = @unserialize($socket_message);

		pcntl_async_signals(true);
		return $message;
	}

	/**
	 * Log a message
	 *
	 * @access protected
	 * @param string $message the text to log
	 * @param int $severity the severity of the message
	 * @param bool true on success, false on error
	 */
	protected function log($message, $severity)
	{
		if (!empty($this->log_function))
		{
			if (isset($this->log_function[$severity]))
			{
				return call_user_func_array($this->log_function[$severity], array($message, $severity));
			}
			elseif (isset($this->log_function[self::LOG_LEVEL_ALL]))
			{
				return call_user_func_array($this->log_function[self::LOG_LEVEL_ALL], array($message, $severity));
			}
		}
		// Barracuda specific logging class, to keep internal code working
		elseif (method_exists('Log', 'message'))
		{
			return Log::message($message, $severity);
		}

		return true;
	}
}
