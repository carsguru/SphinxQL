<?php
namespace Gkarims\SphinxQL;
	/*
	        Read Documentation, Luke.

	        $sphinxql = new SphinxQL();
			$query = $sphinxql->newQuery();
			$query->addIndex('my_index')
					->addField('field_name', 'alias')
					->addField('another_field')
					->addFields(array(array('field' => 'title', 'alias' => 'title_alias'), array('field' => 'user_id')))
					->search('some words to search for')
			// string (is given directly to sphinx, so can contain @field directives)
					->where('time', time()-3600, '>', FALSE)
			// field, value, operator='=', quote=TRUE
					->whereIn('tags_i_need', array(1, 2, 3), 'all')
					->whereIn('tags_i_do_not_want', array(4, 5, 6), 'none')
					->whereIn('tags_i_would_like_one_of', array(7, 8, 9), 'any')
			// field, array values, type='any'
					->order('@weight', 'desc')
			// field, sort='desc'
					->offset(10)->limit(50)
			// defaults are 0 and 20, same as the sphinx defaults
					->option('max_query_time', '100')
			// option name, option value
					->groupBy('field')
					->in_group_order_by('another_field', 'desc');
					// sphinx-specific, check their docs
			$result = $query->execute();

			@todo Нужно отрефакторить на вопрос работы с несколькими соединениями
	 *			и выкидывания различных Exception`ов при неправильном запросе.
 	*/


	class SphinxQL
	{

		/**
		 * @var SphinxQL_Client[] A collection of
		 */
		protected static $_handles = array();
		private static $instance = NULL;
		public static function getInstance()
		{
			if ( self::$instance === NULL )
			{
				$servers = array('sp1' => '127.0.0.1:9306');
				self::$instance = new SphinxQL($servers);
			}
			return self::$instance;
		}

		/**
		 * Constructor
		 *
		 * @throws Exception
		 * @param array  Config servers
		 */
		public function __construct( array $servers )
		{
			if( empty( $servers ) )
			{
				throw new \Exception('Wrong usage sphinx :: invalid config');
			}

			foreach( $servers as $k => $v )
			{
				$this->addServer( $k, $v );
			}

		}

		/**
		 * Create a new SphinxQL_Client for a server and add it to the pool of clients
		 *
		 * @param string An alias for this server
		 * @param string The address and port of a server
		 *
		 * @return boolean The status of the creation of the SphinxQL_Client
		 */
		public function addServer( $name, $server )
		{

			if ( is_string( $server ) )
			{
				if ( isset( self::$_handles[$server] ) )
				{
					return TRUE;
				}
				if ( $client = new SphinxQL_Client( $server ) )
				{
					self::$_handles[$name] = $client;
					return TRUE;
				}
			}

			return FALSE;
		}

		/**
		 * Create a new SphinxQL_Query, automatically add this SphinxQL as the constructor argument
		 *
		 * @return SphinxQL_Query|FALSE The resulting query or FALSE on error
		 */
		public function newQuery()
		{

			if ( $query = new SphinxQL_Query( $this ) )
			{
				return $query;
			}
			return FALSE;
		}

		/**
		 * Perform a query, given either a string or a SphinxQL_Query object
		 * Cycles through all available servers until it succeeds.
		 * In the event that it can't find a responsive server, returns FALSE.
		 *
		 * @param SphinxQL_Query|string A query as a string or a SphinxQL_Query object
		 *
		 * @return array|FALSE The result of the query or FALSE
		 *
		 * @throws SphinxqlQueryException Если при выполнении запроса возникла ошибка.
		 */
		public function query( $query )
		{
			if ( !($query instanceof SphinxQL_Query) && !is_string( $query ) )
			{
				return FALSE;
			}
			while ( ( $names = array_keys( self::$_handles ) ) && count( $names ) && ( $name = $names[intval( rand( 0, count( $names ) - 1 ) )] ) )
			{
				$client = self::$_handles[$name];

				try
				{
					$return = $client->query( (string) $query )->fetch_all();
				}
				catch(SphinxqlConnectException $e)
				{
					// Ошибка соединения
					error_log( $e->getMessage() );
					unset( self::$_handles[$name] );

					continue;
				}

				return $return;
			}

			// Если ни одно соединение не является успешным.
			return FALSE;
		}

		public function exec ( $query )
		{
			while ( ( $names = array_keys( self::$_handles ) ) && count( $names ) && ( $name = $names[intval( rand( 0, count( $names ) - 1 ) )] ) )
			{
				$client = self::$_handles[$name];

				try
				{
					$return = $client->query( (string) $query );
				}
				catch(SphinxqlConnectException $e)
				{
					// Ошибка соединения
					error_log( $e->getMessage() );
					unset( self::$_handles[$name] );

					continue;
				}

				return $return;
			}
		}

		/**
		 * Получение статистики по результатам запроса.
		 * Если передан параметр $value, то возвращается только значение запрошенного параметра.
		 *
		 * @param string $value
		 * @return array|string|boolean|null
		 */
		public function stats( $value = NULL )
		{
			$data = $this->query( 'SHOW META' );

			if ( empty( $data ) )
			{
				return FALSE;
			}

			$stats = array();
			foreach ( $data as $v )
			{
				$stats[$v['Variable_name']] = $v['Value'];
			}

			//если запрашивается конкретное поле
			if ( $value != NULL )
			{
				//возвращаем поле из массива статистики
				if ( isset( $stats[$value] ) )
				{
					return $stats[$value];
				}
				else
				{
					return NULL;
				}
			}
			else
			{
				return $stats;
			}
		}

		public function getLimit()
		{
			return 100000;
		}
	}


	class SphinxQL_Client
	{

		/**
		 * @var string The address and port of the server this client is to connect to
		 */
		protected $_server = FALSE;
		/**
		 * @var resource A reference to the mysql link that this client will be using
		 */
		protected $_handle = FALSE;
		/**
		 * @var boolean A flag to denote whether or not this client has tried to connect and failed
		 */
		protected $_failed = FALSE;
		/**
		 * @var resource A reference to the mysql result returned by a query that this client has performed
		 */
		protected $_result = FALSE;

		/**
		 * Constructor
		 *
		 * @param string $server The address and port of a sphinx server
		 */
		public function __construct( $server )
		{

			if ( !is_string( $server ) )
			{
				return FALSE;
			}
			$this->_server = $server;
		}

		/**
		 * Used to attempt connection to the sphinx server, keeps a record of whether it failed to connect or not
		 *
		 * @throws SphinxqlConnectException
		 *
		 * @return boolean Status of the connection attempt
		 */
		protected function connect()
		{

			if ( $this->_handle )
			{
				return TRUE;
			}
			if ( $this->_failed )
			{
				throw new SphinxqlConnectException('Connection already failed for server : ' . $this->_server);
			}
			if ( $this->_server === FALSE )
			{
				return FALSE;
			}
			try
			{
				$tmp = explode( ':', $this->_server );
				$this->_handle = new \mysqli( $tmp[0], '', '', '', $tmp[1] );
			}
			catch ( \Exception $e )
			{
				$this->_failed = TRUE;
				throw new SphinxqlConnectException($e->getMessage());
			}
			return TRUE;
		}

		/**
		 * Perform a query
		 *
		 * @param string $query to perform
		 *
		 * @return SphinxQL_Client This client object
		 *
		 * @throws SphinxqlQueryException Если при выполнении запроса возникла ошибка.
		 */
		public function query( $query )
		{

			$this->_result = FALSE;
			if ( is_string( $query ) && $this->connect() )
			{
				$this->_result = mysqli_query( $this->_handle, $query );

				if( $this->_result === FALSE )
				{
					$error = mysqli_error($this->_handle);
					throw new SphinxqlQueryException($error);
				}
			}
			return $this;
		}

		/**
		 * Fetch one row of the result set
		 *
		 * @return array|boolean The row or an error
		 */
		public function fetch_row()
		{

			if ( $this->_result === FALSE )
			{
				return FALSE;
			}
			if ( $arr = mysqli_fetch_assoc( $this->_result ) )
			{
				return $arr;
			}
			return FALSE;
		}

		/**
		 * Fetch the whole result set
		 *
		 * @return array|boolean The results or an error
		 */
		public function fetch_all()
		{
			if ( $this->_result === FALSE )
			{
				return FALSE;
			}
			$ret = array();
			while ( $arr = mysqli_fetch_assoc( $this->_result ) )
			{
				$ret[] = $arr;
			}
			return $ret;
		}
	}


	class SphinxQL_Query
	{

		/**
		 * @var array The indexes that are to be searched
		 */
		protected $_indexes = array();
		/**
		 * @var array The fields that are to be returned in the result set
		 */
		protected $_fields = array();
		/**
		 * @var string A string to be searched for in the indexes
		 */
		protected $_search = NULL;
		/**
		 * @var array A set of WHERE conditions
		 */
		protected $_wheres = array();
		/**
		 * @var array The GROUP BY field
		 */
		protected $_group = NULL;
		/**
		 * @var array The IN GROUP ORDER BY options
		 */
		protected $_group_order = NULL;
		/**
		 * @var array A set of ORDER clauses
		 */
		protected $_orders = array();
		/**
		 * @var integer The offset to start returning results from
		 */
		protected $_offset = 0;
		/**
		 * @var integer The maximum number of results to return
		 */
		protected $_limit = 20;
		/**
		 * @var array A set of OPTION clauses
		 */
		protected $_options = array();
		/**
		 * @var SphinxQL A reference to a SphinxQL object, used for the execute() function
		 */
		protected $_sphinx = NULL;

		protected $_sql_parts = array();

		/**
		 * Constructor
		 *
		 * @param SphinxQL $sphinx
		 */
		public function __construct( SphinxQL $sphinx )
		{

			$this->sphinx( $sphinx );
		}

		/**
		 * Magic method, returns the result of build().
		 *
		 * @return string
		 */
		public function __toString()
		{

			return $this->build();
		}

		/**
		 * Sets or gets the SphinxQL object associated with this query.
		 * If you pass it nothing, it'll return $this->_sphinx
		 * If you pass it a SphinxQL object, it'll return $this
		 * If you pass it anything else, it'll return FALSE
		 *
		 * @param SphinxQL|NULL
		 * @return SphinxQL_Query|SphinxQL|FALSE $this or $this->_sphinx or error
		 */
		public function sphinx( $sphinx = NULL )
		{

			if ( $sphinx instanceof SphinxQL )
			{
				$this->_sphinx = $sphinx;
				return $this;
			}
			elseif ( $sphinx === NULL )
			{
				return $sphinx;
			}

			return FALSE;
		}

		/**
		 * Builds the query string from the information you've given.
		 *
		 * @return string The resulting query
		 */
		public function build()
		{

			$fields  = array();
			$wheres  = array();
			$orders  = array();
			$options = array();
			$query   = '';

			foreach ( $this->_fields as $field )
			{
				if ( !isset( $field['field'] ) OR !is_string( $field['field'] ) )
				{
					next;
				}
				if ( isset( $field['alias'] ) AND is_string( $field['alias'] ) )
				{
					$fields[] = sprintf( "%s AS %s", $field['field'], $field['alias'] );
				}
				else
				{
					$fields[] = sprintf( "%s", $field['field'] );
				}
			}
			unset( $field );

			if ( is_string( $this->_search ) )
			{
				$wheres[] = sprintf( "MATCH('%s')", addslashes( $this->_search ) );
			}

			foreach ( $this->_wheres as $where )
			{
				$wheres[] = sprintf( "%s %s %s", $where['field'], $where['operator'], $where['value'] );
			}
			unset( $where );

			foreach ($this->_sql_parts as $where)
			{
				$wheres[] = $where;
			}
			unset($where);

			foreach ( $this->_orders as $order )
			{
				$orders[] = sprintf( "%s %s", $order['field'], $order['sort'] );
			}
			unset( $order );

			foreach ( $this->_options as $option )
			{
				$options[] = sprintf( "%s=%s", $option['name'], $option['value'] );
			}
			unset( $option );

			$query .= sprintf( 'SELECT %s ', count( $fields ) ? implode( ', ', $fields ) : '*' );
			$query .= sprintf( 'FROM %s ', implode( ',', $this->_indexes ) );
			if ( count( $wheres ) > 0 )
			{
				$query .= sprintf( 'WHERE %s ', implode( ' AND ', $wheres ) );
			}
			if ( is_string( $this->_group ) )
			{
				$query .= sprintf( 'GROUP BY %s ', $this->_group );
			}
			if ( is_array( $this->_group_order ) )
			{
				$query .= sprintf( 'WITHIN GROUP ORDER BY %s %s ', $this->_group_order['field'], $this->_group_order['sort'] );
			}
			if ( count( $orders ) > 0 )
			{
				$query .= sprintf( 'ORDER BY %s ', implode( ', ', $orders ) );
			}
			$query .= sprintf( 'LIMIT %d, %d ', $this->_offset, $this->_limit );
			if ( count( $options ) > 0 )
			{
				$query .= sprintf( 'OPTION %s ', implode( ', ', $options ) );
			}
			while ( substr( $query, -1, 1 ) == ' ' )
			{
				$query = substr( $query, 0, -1 );
			}

			return $query;
		}

		/**
		 * Adds an entry to the list of indexes to be searched.
		 *
		 * @param string The index to add
		 *
		 * @return SphinxQL_Query $this
		 */
		public function addIndex( $index )
		{

			if ( is_string( $index ) )
			{
				array_push( $this->_indexes, $index );
			}

			return $this;
		}

		/**
		 * Removes an entry from the list of indexes to be searched.
		 *
		 * @param string The index to remove
		 *
		 * @return SphinxQL_Query $this
		 */
		public function removeIndex( $index )
		{

			if ( is_string( $index ) )
			{
				while( ( $pos = array_search( $index, $this->_indexes ) ) !== FALSE )
				{
					unset( $this->_indexes[$pos] );
				}
			}

			return $this;
		}

		/**
		 * Adds a entry to the list of fields to return from the query.
		 *
		 * @param string Field to add
		 * @param string Alias for that field, optional
		 *
		 * @return SphinxQL_Query $this
		 */
		public function addField( $field, $alias = NULL )
		{

			if ( !is_string( $alias ) )
			{
				$alias = NULL;
			}

			if ( is_string( $field ) )
			{
				$this->_fields[] = array( 'field' => $field, 'alias' => $alias );
			}

			return $this;
		}

		/**
		 * Adds multiple entries at once to the list of fields to return.
		 * Takes an array structured as so:
		 * array(array('field' => 'user_id', 'alias' => 'user')), ...)
		 * The alias is optional.
		 *
		 * @param array Array of fields to add
		 *
		 * @return SphinxQL_Query $this
		 */
		public function addFields( $array )
		{

			if ( is_array( $array ) )
			{
				foreach ( $array as $entry )
				{
					if ( is_array( $entry ) AND isset( $entry['field'] ) )
					{
						if ( !isset( $entry['alias'] ) OR is_string( $entry['alias'] ) )
						{
							$entry['alias'] = NULL;
							$this->addField( $entry['field'], $entry['alias'] );
						}
					}
				}
			}

			return $this;
		}

		/**
		 * Removes a field from the list of fields to search.
		 *
		 * @param string Alias of the field to remove
		 *
		 * @return SphinxQL_Query $this
		 */
		public function removeField( $alias )
		{

			if ( is_string( $alias ) AND array_key_exists( $this->_fields, $alias ) )
			{
				unset( $this->_fields[$alias] );
			}

			return $this;
		}

		/**
		 * Removes multiple fields at once from the list of fields to search.
		 *
		 * @param array|mixed List of aliases of fields to remove
		 *
		 * @return SphinxQL_Query $this
		 */
		public function removeFields( $array )
		{
			if ( is_array( $array ) )
			{
				foreach ( $array as $alias )
				{
					$this->removeField( $alias );
				}
			}

			return $this;
		}

		/**
		 * Sets the text to be matched against the index(es)
		 *
		 * @param string Text to be searched
		 *
		 * @return SphinxQL_Query $this
		 */
		public function search( $search )
		{

			if ( is_string( $search ) )
			{
				$this->_search = $search;
			}

			return $this;
		}

		/**
		 * Removes the search text from the query.
		 *
		 * @return SphinxQL_Query $this
		 */
		public function removeSearch()
		{

			$this->_search = NULL;

			return $this;
		}

		/**
		 * Sets the offset for the query
		 *
		 * @param integer Offset
		 *
		 * @return SphinxQL_Query $this
		 */
		public function offset( $offset )
		{

			if ( is_integer( $offset ) )
			{
				$this->_offset = $offset;
			}

			return $this;
		}

		/**
		 * Sets the limit for the query
		 *
		 * @param integer Limit
		 *
		 * @return SphinxQL_Query $this
		 */
		public function limit( $limit )
		{

			if ( is_integer( $limit ) )
			{
				$this->_limit = $limit;
			}

			return $this;
		}

		/**
		 * Adds a WHERE condition to the query.
		 *
		 * @param string The field/expression for the condition
		 * @param string The field/expression/value to compare the field to
		 * @param string The operator (=, <, >, etc)
		 * @param bool Whether or not to quote the value, defaults to TRUE
		 *
		 * @return SphinxQL_Query $this
		 */
		public function where( $field, $value, $operator = NULL, $quote = TRUE )
		{

			if ( !in_array( $operator, array( '=', '!=', '>', '<', '>=', '<=', 'AND', 'NOT IN', 'IN', 'BETWEEN' ) ) )
			{
				$operator = '=';
			}
			if ( !is_string( $field ) )
			{
				return FALSE;
			}
			if ( !is_scalar( $value ) )
			{
				return FALSE;
			}

			$quote = ( $quote === TRUE ) ? TRUE : FALSE;

			$this->_wheres[] = array( 'field' => $field, 'operator' => $operator, 'value' => $value, 'quote' => $quote );

			return $this;
		}

		/**
		 * Adds a WHERE <field> <not> IN (<value x>, <value y>, <value ...>) condition to the query, mainly used for MVAs.
		 *
		 * @param string The field/expression for the condition
		 * @param array  The values to compare the field to
		 * @param string Whether this is a match-all, match-any (default) or match-none condition
		 *
		 * @return SphinxQL_Query $this
		 */
		public function whereIn( $field, $values, $how = 'any' )
		{

			if ( !is_array( $values ) )
			{
				$values = array( $values );
			}

			if ( $how == 'all' )
			{
				foreach ( $values as $value )
				{
					$this->where( $field, $value, '=' );
				}
			}
			elseif ( $how == 'none' )
			{
				/*foreach ( $values as $value )
				{
					$this->where( $field, $value, '!=' );
				}*/
				$this->where( $field, '(' . implode( ', ', $values ) . ')', 'NOT IN', FALSE );
			}
			else
			{
				$this->where( $field, '(' . implode( ', ', $values ) . ')', 'IN', FALSE );
			}

			return $this;
		}

		/**
		 * Add raw sql expression
		 *
		 * @param string $sql
		 * @return SphinxQL
		 */
		public function whereSql($sql)
		{
			if (!is_string($sql) || empty($sql))
			{
				return FALSE;
			}

			$this->_sql_parts[] = $sql;

			return $this;
		}

		/**
		 * Sets the GROUP BY condition for the query.
		 *
		 * @param string The field/expression for the condition
		 *
		 * @return SphinxQL_Query $this
		 */
		public function groupBy( $field )
		{

			if ( is_string( $field ) )
			{
				$this->_group = $field;
			}

			return $this;
		}

		/**
		 * Removes the GROUP BY condition from the query.
		 *
		 * @param string The field/expression for the condition
		 * @param string The alias for the result set (optional)
		 *
		 * @return SphinxQL_Query $this
		 */
		public function removeGroupBy( $field )
		{
			unset( $field );
			// :TODO: тут надо допилить до полной поддержки group by
			$this->_group = NULL;

			return $this;
		}

		/**
		 * Adds an ORDER condition to the query.
		 *
		 * @param string The field/expression for the condition
		 * @param string The sort type (can be 'asc' or 'desc', capitals are also OK)
		 *
		 * @return SphinxQL_Query $this
		 */
		public function order( $field, $sort )
		{

			if ( is_string( $field ) AND is_string( $sort ) )
			{
				$this->_orders[] = array( 'field' => $field, 'sort' => $sort );
			}

			return $this;
		}

		/**
		 * Sets the WITHIN GROUP ORDER BY condition for the query. This is a
		 * Sphinx-specific extension to SQL.
		 *
		 * @param string The field/expression for the condition
		 * @param string The sort type (can be 'asc' or 'desc', capitals are also OK)
		 *
		 * @return SphinxQL_Query $this
		 */
		public function groupOrder( $field, $sort )
		{

			if ( is_string( $field ) AND is_string( $sort ) )
			{
				$this->_group_order = array( 'field' => $field, 'sort' => $sort );
			}

			return $this;
		}

		/**
		 * Removes the WITHIN GROUP ORDER BY condition for the query. This is a
		 * Sphinx-specific extension to SQL.
		 *
		 * @return SphinxQL_Query $this
		 */
		public function removeGroupOrder()
		{

			$this->_group_order = NULL;

			return $this;
		}

		/**
		 * Adds an OPTION to the query. This is a Sphinx-specific extension to SQL.
		 *
		 * @param string $name  The option name
		 * @param string $value The option value
		 *
		 * @return SphinxQL_Query $this
		 */
		public function option( $name, $value )
		{

			if ( is_string( $name ) AND is_string( $value ) )
			{
				$this->_options[] = array( 'name' => $name, 'value' => $value );
			}

			return $this;
		}

		/**
		 * Removes an OPTION from the query.
		 *
		 * @param string $name  The option name
		 * @param string $value The option value, optional
		 *
		 * @return SphinxQL_Query $this
		 */
		public function removeOption( $name, $value = NULL )
		{

			$changed = FALSE;

			if ( is_string( $name ) AND ( ( $value == NULL ) OR is_string( $value ) ) )
			{
				foreach ( $this->_options as $key => $option )
				{
					if ( ( $option['name'] == $name ) AND ( ( $value == NULL ) OR ( $value == $option['value'] ) ) )
					{
						unset( $this->_options[$key] );
						$changed = TRUE;
					}
				}

				if ( $changed )
				{
					array_keys( $this->_options );
				}
			}

			return $this;
		}

		/**
		 * Executes the query and returns the results
		 *
		 * @param bool $dump ( Dump SphinxQL query )
		 *
		 * @return array Results of the query
		 */
		public function execute( $dump = FALSE )
		{

			if( $dump )
			{
				if( function_exists('d') )
				{
					d( (string) $this );
				}
				else
				{
					error_log( (string) $this );
				}
			}
			return $this->_sphinx->query( $this );
		}

		public function escapeString ( $string )
		{
			$from = array ( '\\', '(',')','|','-','!','@','~','"','&', '/', '^', '$', '=', '\'' );
			$to   = array ( '\\\\', '\(','\)','\|','\-','\!','\@','\~','\"', '\&', '\/', '\^', '\$', '\=', '\\\'' );

			return str_replace ( $from, $to, $string );
		}

		public function buildWithMap( $input, $search_map )
		{
			if ( empty($input) )
			{
				return $this;
			}
			foreach( $search_map as $key => $rule )
			{
				if ( !isset( $input[$key] ) )
				{
					continue;
				}
				$val = $input[$key];
				if ( in_array( $rule, array( '>', '<', '>=', '<=' ) ) )
				{
					if ( !is_scalar( $val ) )
					{
						continue;
					}
					$key = preg_replace( '~^(min|max)~', '', $key );
					$val = preg_replace( '~[^\d]~', '', $val );
					if ( $val )
					{
						$this->where( $key, $val, $rule );
					}
					continue;
				}
				if ( $rule == '=' )
				{
					if ( is_scalar( $val ) )
					{
						$val = intval( preg_replace( '~[^\d]~', '', $val ) );
						if ( $val )
						{
							$this->where( $key, $val, '=' );
						}
					}
					elseif ( is_array( $val ) )
					{
						foreach( $val as $k => $v )
						{
							$v = intval( preg_replace( '~[^\d]~', '', $v ) );
							if ( !$v )
							{
								unset( $val[$k] );
							}
							$val[$k] = $v;
						}
						if ( !empty( $val ) )
						{
							$this->whereIn( $key, $val );
						}
					}
					continue;
				}
				if ( substr( $rule, 0, 1 ) == '=' )
				{
					$preg = substr( $rule, 1 );
					if ( preg_match( $preg, $val ) )
					{
						$val = "'$val'";
						$this->where( $key, $val, '=' );
					}
				}
			}
			return $this;
		}

	}

	class SphinxqlConnectException extends \Exception{

		public function __construct($message)
		{
			parent::__construct($message);
		}
	}

	class SphinxqlQueryException extends \Exception{

		public function __construct($message)
		{
			parent::__construct($message);
		}
	}

