/********************************************************************
db.h - This file contains all the structures, defines, and function
prototype for the db.exe program.
*********************************************************************/
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>

#define MAX_IDENT_LEN   16
#define MAX_NUM_COL			16
#define MAX_TOK_LEN			32
#define KEYWORD_OFFSET	10
#define STRING_BREAK		" (),<>="
#define NUMBER_BREAK		" ),"
#define MAX_DATA_LENGTH 255
#define ROLLFORWARD_PENDING 1

/* Column descriptor sturcture = 20+4+4+4+4 = 36 bytes */
typedef struct cd_entry_def
{
	char		col_name[MAX_IDENT_LEN+4];
	int			col_id;                   /* Start from 0 */
	int			col_type;
	int			col_len;
	int 		not_null;
} cd_entry;

/* Table packed descriptor sturcture = 4+20+4+4+4 = 36 bytes
Minimum of 1 column in a table - therefore minimum size of
1 valid tpd_entry is 36+36 = 72 bytes. */
typedef struct tpd_entry_def
{
	int				tpd_size;
	char			table_name[MAX_IDENT_LEN+4];
	int				num_columns;
	int				cd_offset; //Offset where the entries of cd_entry will start
	int       tpd_flags;
} tpd_entry;

/* Table packed descriptor list = 4+4+4+36 = 48 bytes.  When no
table is defined the tpd_list is 48 bytes.  When there is 
at least 1 table, then the tpd_entry (36 bytes) will be
overlapped by the first valid tpd_entry. */
typedef struct tpd_list_def
{
	int				list_size; // Gives the size of list in bytes
	int				num_tables;
	int				db_flags;
	tpd_entry	tpd_start;
}tpd_list;

/* This token_list definition is used for breaking the command
string into separate tokens in function get_tokens().  For
each token, a new token_list will be allocated and linked 
together. */
typedef struct t_list
{
	char	tok_string[MAX_TOK_LEN];
	int		tok_class;
	int		tok_value;
	struct t_list *next;
} token_list;

/* This enum defines the different classes of tokens for 
semantic processing. */
typedef enum t_class
{
	keyword = 1,	// 1
	identifier,		// 2
	symbol, 			// 3
	type_name,		// 4
	constant,		  // 5
	function_name,// 6
	terminator,		// 7
	error			    // 8

} token_class;

/* This enum defines the different values associated with
a single valid token.  Use for semantic processing. */
typedef enum t_value
{
	T_INT = 10,		// 10 - new type should be added above this line
	T_CHAR,		    // 11 
	K_CREATE, 		// 12
	K_TABLE,			// 13
	K_NOT,				// 14
	K_NULL,				// 15
	K_DROP,				// 16
	K_LIST,				// 17
	K_SCHEMA,			// 18
	K_FOR,        // 19
	K_TO,				  // 20
	K_INSERT,     // 21
	K_INTO,       // 22
	K_VALUES,     // 23
	K_DELETE,     // 24
	K_FROM,       // 25
	K_WHERE,      // 26
	K_UPDATE,     // 27
	K_SET,        // 28
	K_SELECT,     // 29
	K_ORDER,      // 30
	K_BY,         // 31
	K_DESC,       // 32
	K_IS,         // 33
	K_AND,        // 34
	K_OR,         // 35 - new keyword should be added below this line
	K_BACKUP,     // 36
	K_RESTORE,		//37
	K_WITHOUT,		//38
	K_ROLLFORWARD,	//39
	K_RF,			//40
	F_SUM,        // 41
	F_AVG,        // 42
	F_COUNT,      // 43 - new function name should be added below this line
	S_LEFT_PAREN = 70,  // 70
	S_RIGHT_PAREN,		  // 71
	S_COMMA,			      // 72
	S_STAR,             // 73
	S_EQUAL,            // 74
	S_LESS,             // 75
	S_GREATER,          // 76
	IDENT = 85,			    // 85
	INT_LITERAL = 90,	  // 90
	STRING_LITERAL,     // 91
	EOC = 95,			      // 95
	INVALID = 99,		    // 99
	
} token_value;

/* This constants must be updated when add new keywords */
#define TOTAL_KEYWORDS_PLUS_TYPE_NAMES 34

/* New keyword must be added in the same position/order as the enum
definition above, otherwise the lookup will be wrong */
char *keyword_table[] = 
{
	"int", "char", "create", "table", "not", "null", "drop", "list", "schema",
	"for", "to", "insert", "into", "values", "delete", "from", "where", 
	"update", "set", "select", "order", "by", "desc", "is", "and", "or", "backup", "restore", "without", "rollforward", "rf", 
	"sum", "avg", "count"
};

/* This enum defines a set of possible statements */
typedef enum s_statement
{
	INVALID_STATEMENT = -199,	// -199
	CREATE_TABLE = 100,				// 100
	DROP_TABLE,								// 101
	LIST_TABLE,								// 102
	LIST_SCHEMA,							// 103
	INSERT,                   // 104
	DELETE,                   // 105
	UPDATE,                   // 106
	SELECT,                    // 107
	BACKUP,						//108
	ROLLFORWARD,				//109
	RESTORE						//110
} semantic_statement;

/* This enum has a list of all the errors that should be detected
by the program.  Can append to this if necessary. */
typedef enum error_return_codes
{
	INVALID_TABLE_NAME = -399,	// -399
	DUPLICATE_TABLE_NAME,				// -398
	TABLE_NOT_EXIST,						// -397
	INVALID_TABLE_DEFINITION,		// -396
	INVALID_COLUMN_NAME,				// -395
	DUPLICATE_COLUMN_NAME,			// -394
	COLUMN_NOT_EXIST,						// -393
	MAX_COLUMN_EXCEEDED,				// -392
	INVALID_TYPE_NAME,					// -391
	INVALID_COLUMN_DEFINITION,	// -390
	INVALID_COLUMN_LENGTH,			// -389
	INVALID_REPORT_FILE_NAME,		// -388
	INVALID_INSERT_DEFINITION,		//-387
	UNSUPPORTED_DATA_TYPE,			//-386
	TABLE_FILE_CORRUPTION,			//-385
	NOT_NULL_CONSTRAINT_VIOLATION,	//-384
	DATA_TYPE_MISMATCH,				//-383
	RECORDS_NOT_FOUND,				//-382
	INVALID_UPDATE_DEFINITION,		// -381
	INVALID_SELECT_DEFINITION,		// -380
	COLUMN_LENGTH_EXCEEDED,			//-379
	UNSUPPORTED_RELATIONAL_OPERATOR, //-378
	INVALID_DELETE_DEFINITION,     // -377
	INVALID_BACKUP_DEFINITION,      // -376
	INVALID_RESTORE_DEFINITION,      // -375
	INVALID_ROLLFORWARD_DEFINITION,      // -374
	BACKUP_FILE_ALREADY_EXISTS,      // -373
	BACKUP_IMAGE_NOT_FOUND,        //-372
	ROLLFORWARD_PENDING_STATE_FOUND,    //-371
	ROLLFORWARD_PENDING_STATE_NOT_FOUND, // -370
	RF_START_NOT_FOUND,                   //-369
	INVALID_TIMESTAMP_VALUE,			  //-368
	/* Must add all the possible errors from I/U/D + SELECT here */
	FILE_OPEN_ERROR = -299,			// -299
	DBFILE_CORRUPTION,					// -298
	MEMORY_ERROR							  // -297
} return_codes;

/* Set of function prototypes */
int get_token(char *command, token_list **tok_list);
void add_to_list(token_list **tok_list, char *tmp, int t_class, int t_value);
int do_semantic(token_list *tok_list);
int sem_create_table(token_list *t_list);
int sem_drop_table(token_list *t_list);
int sem_list_tables();
int sem_list_schema(token_list *t_list);
int create_table_file(cd_entry col_entry[], tpd_entry *tab_entry);
int sem_insert_into(token_list *t_list);
int find_col_type(tpd_entry *tab_entry, int col_id);
int read_table_file(tpd_entry *new_entry);
cd_entry* get_cd_entry_from_tpd_entry(tpd_entry *cur);
int sem_delete_from(token_list *t_list);
int sem_select(token_list *t_list);
int sem_update(token_list *t_list);
cd_entry* get_column_from_table(char *column_name, tpd_entry *tab_entry);
void print_string_data(char *data, cd_entry col_entry);
void print_int_data(int data, char* col_name, bool aggr_function);
int update_column_data(tpd_entry *new_entry,cd_entry *column_entry,cd_entry *column_entry1,token_list * cur_update,token_list *cur, int bytes_read,int relational_operator); 
int delete_record_data(tpd_entry *tab_entry,cd_entry *column_entry1,token_list *cur,int bytes_read,int relational_operator); //delete specific rows
int select_data(char *selected_data_ptr,tpd_entry *new_entry,cd_entry *where_column_entry1,cd_entry *where_column_entry2,token_list *where_data1,token_list *where_data2, int bytes_read,int relational_operator,int relational_operator1, int and_or_operator,int *counter_rows_selected_ptr);
void sort_data(char * selected_data,cd_entry *column_to_sort,tpd_entry *tab_entry,int num_of_records,bool desc);
int compare_columns(char *record_ptr1,char * record_ptr2,cd_entry *col_to_compare,tpd_entry *tab_entry);
char* get_timestamp();
int sem_backup_to(token_list *cur);
int sem_restore_from(token_list *cur);
int sem_rollforward(token_list *cur);
int sem_restore_from(token_list *t_list);
int restore_from_backup_file(char* backup_file_name);
int backup_log_file();
int parse_entered_timestamp(char* entered_timestamp);


/*
Keep a global list of tpd - in real life, this will be stored
in shared memory.  Build a set of functions/methods around this.
*/
tpd_list	*g_tpd_list;
int initialize_tpd_list();
int add_tpd_to_list(tpd_entry *tpd);
int drop_tpd_from_list(char *tabname);
tpd_entry* get_tpd_from_list(char *tabname);
int max_file_size;			//max_file_size is the total size of the record data in the file excluding header 


/* Structure for Table file header */
typedef struct table_file_header_def
{
	int	file_size;		//initially it is the size of the file header
	int	record_size;	// It will be calculated as (1 + col_length) of all columns
	int	num_records;	//It will be calculated as num_records = num_records + 1 whenever a new row is inserted
	int	record_offset;	// It will be calculated as sizeof(table_file_header)
	int	file_header_flag;
	tpd_entry *tpd_ptr; 

}table_file_header ;

table_file_header header1 ; 



char *tab_data_ptr;		//to represent record data inside the table

FILE *log_file_ptr ;   //log file ptr
char *command_to_execute ;




