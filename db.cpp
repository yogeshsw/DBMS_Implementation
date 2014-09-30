/************************************************************
Project#1:	CLP & DDL
************************************************************/


#include "db.h"



int main(int argc, char** argv)
{
	int rc = 0;
	token_list *tok_list=NULL, *tok_ptr=NULL, *tmp_tok_ptr=NULL;

	if ((argc != 2) || (strlen(argv[1]) == 0))
	{
		printf("Usage: db \"command statement\"");
		return 1;
	}
	command_to_execute = argv[1];
	rc = initialize_tpd_list();

	if (rc)
	{
		printf("\nError in initialize_tpd_list().\nrc = %d\n", rc);
	}
	else
	{   
		char *filename = "db.log" ;
		log_file_ptr = fopen(filename, "a+");
		if(log_file_ptr == NULL){
			rc = FILE_OPEN_ERROR ;
		}
		else{

			printf("\nTable created successfully\n");
			rc = get_token(argv[1], &tok_list);

			/* Test code */
			tok_ptr = tok_list;
			while (tok_ptr != NULL)
			{
				printf("%16s \t%d \t %d\n",tok_ptr->tok_string, tok_ptr->tok_class,
					tok_ptr->tok_value);
				tok_ptr = tok_ptr->next;
			}

			if (!rc)
			{
				rc = do_semantic(tok_list);
			}

			if (rc)
			{
				tok_ptr = tok_list;
				while (tok_ptr != NULL)
				{
					if ((tok_ptr->tok_class == error) ||
						(tok_ptr->tok_value == INVALID))
					{
						printf("\nError in the string: %s\n", tok_ptr->tok_string);
						printf("rc = %d\n", rc);
						break;
					}
					tok_ptr = tok_ptr->next;
				}
			}

			/* Whether the token list is valid or not, we need to free the memory */
			tok_ptr = tok_list;
			while (tok_ptr != NULL)
			{
				tmp_tok_ptr = tok_ptr->next;
				free(tok_ptr);
				tok_ptr=tmp_tok_ptr;
			}
			fclose(log_file_ptr);
		}
	}
	printf("\nRC at the end of main is %d\n", rc);
	return rc;
}

/************************************************************* 
This is a lexical analyzer for simple SQL statements
*************************************************************/
int get_token(char* command, token_list** tok_list)
{
	int rc=0,i,j;
	char *start, *cur, temp_string[MAX_TOK_LEN];
	bool done = false;

	start = cur = command;
	while (!done)
	{
		bool found_keyword = false;

		/* This is the TOP Level for each token */
		memset ((void*)temp_string, '\0', MAX_TOK_LEN);
		i = 0;

		/* Get rid of all the leading blanks */
		while (*cur == ' ')
			cur++;

		if (cur && isalpha(*cur))
		{
			// find valid identifier
			int t_class;
			do 
			{
				temp_string[i++] = *cur++;
			}
			while ((isalnum(*cur)) || (*cur == '_'));

			if (!(strchr(STRING_BREAK, *cur)))
			{
				/* If the next char following the keyword or identifier
				is not a blank, (, ), or a comma, then append this
				character to temp_string, and flag this as an error */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
			else
			{

				// We have an identifier with at least 1 character
				// Now check if this ident is a keyword
				for (j = 0, found_keyword = false; j < TOTAL_KEYWORDS_PLUS_TYPE_NAMES; j++)
				{
					if ((_stricmp(keyword_table[j], temp_string) == 0))
					{
						found_keyword = true;
						break;
					}
				}

				if (found_keyword)
				{
					if (KEYWORD_OFFSET+j < K_CREATE)
						t_class = type_name;
					else if (KEYWORD_OFFSET+j >= F_SUM)
						t_class = function_name;
					else
						t_class = keyword;

					add_to_list(tok_list, temp_string, t_class, KEYWORD_OFFSET+j);
				}
				else
				{
					if (strlen(temp_string) <= MAX_IDENT_LEN)
						add_to_list(tok_list, temp_string, identifier, IDENT);
					else
					{
						add_to_list(tok_list, temp_string, error, INVALID);
						rc = INVALID;
						done = true;
					}
				}

				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
				}
			}
		}
		else if (isdigit(*cur) || *cur == '-')
		{
			// find valid number, positive or negative
			if(*cur == '-') {
				if(!isdigit(*(cur + 1))) {
					// No number present after the '-' sign
					temp_string[i++] = *cur++;
					add_to_list(tok_list, temp_string, error, INVALID);
					rc = INVALID;
					done = true;
				}
			}
			if(!rc) {
				do 
				{
					temp_string[i++] = *cur++;
				}
				while (isdigit(*cur));

				if (!(strchr(NUMBER_BREAK, *cur)))
				{
					/* If the next char following the keyword or identifier
					is not a blank or a ), then append this
					character to temp_string, and flag this as an error */
					temp_string[i++] = *cur++;
					add_to_list(tok_list, temp_string, error, INVALID);
					rc = INVALID;
					done = true;
				}
				else
				{
					add_to_list(tok_list, temp_string, constant, INT_LITERAL);

					if (!*cur)
					{
						add_to_list(tok_list, "", terminator, EOC);
						done = true;
					}
				}
			}
		}
		else if ((*cur == '(') || (*cur == ')') || (*cur == ',') || (*cur == '*')
			|| (*cur == '=') || (*cur == '<') || (*cur == '>'))
		{
			/* Catch all the symbols here. Note: no look ahead here. */
			int t_value;
			switch (*cur)
			{
			case '(' : t_value = S_LEFT_PAREN; break;
			case ')' : t_value = S_RIGHT_PAREN; break;
			case ',' : t_value = S_COMMA; break;
			case '*' : t_value = S_STAR; break;
			case '=' : t_value = S_EQUAL; break;
			case '<' : t_value = S_LESS; break;
			case '>' : t_value = S_GREATER; break;
			}

			temp_string[i++] = *cur++;

			add_to_list(tok_list, temp_string, symbol, t_value);

			if (!*cur)
			{
				add_to_list(tok_list, "", terminator, EOC);
				done = true;
			}
		}
		else if (*cur == '\'')
		{
			/* Find STRING_LITERRAL */
			int t_class;
			cur++;
			do 
			{
				temp_string[i++] = *cur++;
			}
			while ((*cur) && (*cur != '\''));

			temp_string[i] = '\0';

			if (!*cur)
			{
				/* If we reach the end of line */
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
			else /* must be a ' */
			{
				add_to_list(tok_list, temp_string, constant, STRING_LITERAL);
				cur++;
				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
				}
			}
		}
		else
		{
			if (!*cur)
			{
				add_to_list(tok_list, "", terminator, EOC);
				done = true;
			}
			else
			{
				/* not a ident, number, or valid symbol */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
		}
	}

	return rc;
}

void add_to_list(token_list **tok_list, char *tmp, int t_class, int t_value)
{
	token_list *cur = *tok_list;
	token_list *ptr = NULL;

	// printf("%16s \t%d \t %d\n",tmp, t_class, t_value);

	ptr = (token_list*)calloc(1, sizeof(token_list));
	strcpy(ptr->tok_string, tmp);
	ptr->tok_class = t_class;
	ptr->tok_value = t_value;
	ptr->next = NULL;

	if (cur == NULL)
		*tok_list = ptr;
	else
	{
		while (cur->next != NULL)
			cur = cur->next;

		cur->next = ptr;
	}
	return;
}

int do_semantic(token_list *tok_list)
{
	int rc = 0, cur_cmd = INVALID_STATEMENT;
	bool unique = false;
	token_list *cur = tok_list;

	if ((cur->tok_value == K_CREATE) &&
		((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
	{
		printf("CREATE TABLE statement\n");
		cur_cmd = CREATE_TABLE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_DROP) &&
		((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
	{
		printf("DROP TABLE statement\n");
		cur_cmd = DROP_TABLE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_DELETE) &&
		((cur->next != NULL) && (cur->next->tok_value == K_FROM)))
	{
		printf("DELETE FROM statement\n");
		cur_cmd = DELETE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_SELECT) &&
		((cur->next != NULL) ))
	{
		printf("SELECT statement\n");
		cur_cmd = SELECT;
		cur = cur->next;
	}
	else if((cur->tok_value == K_INSERT) &&
		((cur->next != NULL) && (cur->next->tok_value == K_INTO)))
	{
		printf("INSERT INTO statement\n");
		cur_cmd = INSERT;
		cur = cur->next->next;
	}
	else if((cur->tok_value == K_UPDATE) &&
		((cur->next != NULL)))
	{
		printf("UPDATE statement\n");
		cur_cmd = UPDATE;
		cur = cur->next;
	}
	else if((cur->tok_value == K_BACKUP) &&
		(cur->next->tok_value == K_TO) && (cur->next->next != NULL))
	{
		printf("BACKUP TO statement\n");
		cur_cmd = BACKUP;
		cur = cur->next->next;
	}
	else if((cur->tok_value == K_RESTORE) &&
		(cur->next->tok_value == K_FROM) && (cur->next->next != NULL))
	{
		printf("RESTORE FROM statement\n");
		cur_cmd = RESTORE;
		cur = cur->next->next;
	}
	else if((cur->tok_value == K_ROLLFORWARD) &&
		(cur->next != NULL))
	{
		printf("ROLLFORWARD statement\n");
		cur_cmd = ROLLFORWARD;
		cur = cur->next;
	}
	else if ((cur->tok_value == K_LIST) &&
		((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
	{
		printf("LIST TABLE statement\n");
		cur_cmd = LIST_TABLE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_LIST) &&
		((cur->next != NULL) && (cur->next->tok_value == K_SCHEMA)))
	{
		printf("LIST SCHEMA statement\n");
		cur_cmd = LIST_SCHEMA;
		cur = cur->next->next;
	}
	else
	{
		printf("Invalid statement\n");
		rc = cur_cmd;
	}

	if (cur_cmd != INVALID_STATEMENT)
	{
		if(g_tpd_list->db_flags == ROLLFORWARD_PENDING) {
			switch(cur_cmd){
			case SELECT:
				rc = sem_select(cur);
				break;
			case ROLLFORWARD:
				rc = sem_rollforward(cur);
				break;
			case LIST_TABLE:
				rc = sem_list_tables();
				break;
			case LIST_SCHEMA:
				rc = sem_list_schema(cur);
				break;
			default:
				rc = ROLLFORWARD_PENDING_STATE_FOUND;
				break;
			}
		} else{
			switch(cur_cmd)
			{
			case CREATE_TABLE:
				rc = sem_create_table(cur);
				break;
			case DROP_TABLE:
				rc = sem_drop_table(cur);
				break;
			case DELETE:
				rc = sem_delete_from(cur);
				break;
			case SELECT:
				rc = sem_select(cur);
				break;
			case INSERT:
				rc = sem_insert_into(cur);
				break;
			case UPDATE:
				rc = sem_update(cur);
				break;
			case BACKUP:
				rc = sem_backup_to(cur);
				break;
			case RESTORE:
				rc = sem_restore_from(cur);
				break;
			case ROLLFORWARD:
				rc = ROLLFORWARD_PENDING_STATE_NOT_FOUND;
				break;
			case LIST_TABLE:
				rc = sem_list_tables();
				break;
			case LIST_SCHEMA:
				rc = sem_list_schema(cur);
				break;
			default:
				; /* no action */
			}
		}
	}
	return rc;
}

int sem_create_table(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry tab_entry;
	tpd_entry *new_entry = NULL;
	bool column_done = false;
	int cur_id = 0;
	cd_entry	col_entry[MAX_NUM_COL];


	memset(&tab_entry, '\0', sizeof(tpd_entry));
	cur = t_list;
	if ((cur->tok_class != keyword) &&
		(cur->tok_class != identifier) &&
		(cur->tok_class != type_name))
	{
		// Error
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
	}
	else
	{
		if ((new_entry = get_tpd_from_list(cur->tok_string)) != NULL)
		{
			rc = DUPLICATE_TABLE_NAME;
			cur->tok_value = INVALID;
		}
		else
		{
			strcpy(tab_entry.table_name, cur->tok_string);
			cur = cur->next;
			if (cur->tok_value != S_LEFT_PAREN)
			{
				//Error
				rc = INVALID_TABLE_DEFINITION;
				cur->tok_value = INVALID;
			}
			else
			{
				memset(&col_entry, '\0', (MAX_NUM_COL * sizeof(cd_entry)));

				/* Now build a set of column entries */
				cur = cur->next;
				do
				{
					if ((cur->tok_class != keyword) &&
						(cur->tok_class != identifier) &&
						(cur->tok_class != type_name))
					{
						// Error
						rc = INVALID_COLUMN_NAME;
						cur->tok_value = INVALID;
					}
					else
					{
						int i;
						for(i = 0; i < cur_id; i++)
						{
							/* make column name case sensitive */
							if (strcmp(col_entry[i].col_name, cur->tok_string)==0)
							{
								rc = DUPLICATE_COLUMN_NAME;
								cur->tok_value = INVALID;
								break;
							}
						}

						if (!rc)
						{
							strcpy(col_entry[cur_id].col_name, cur->tok_string);
							col_entry[cur_id].col_id = cur_id;
							col_entry[cur_id].not_null = false;    /* set default */

							cur = cur->next;
							if (cur->tok_class != type_name)
							{
								// Error
								rc = INVALID_TYPE_NAME;
								cur->tok_value = INVALID;
							}
							else
							{
								/* Set the column type here, int or char */
								col_entry[cur_id].col_type = cur->tok_value;
								cur = cur->next;

								if (col_entry[cur_id].col_type == T_INT)
								{
									if ((cur->tok_value != S_COMMA) &&
										(cur->tok_value != K_NOT) &&
										(cur->tok_value != S_RIGHT_PAREN))
									{
										rc = INVALID_COLUMN_DEFINITION;
										cur->tok_value = INVALID;
									}
									else
									{
										col_entry[cur_id].col_len = sizeof(int);

										if ((cur->tok_value == K_NOT) &&
											(cur->next->tok_value != K_NULL))
										{
											rc = INVALID_COLUMN_DEFINITION;
											cur->tok_value = INVALID;
										}	
										else if ((cur->tok_value == K_NOT) &&
											(cur->next->tok_value == K_NULL))
										{					
											col_entry[cur_id].not_null = true;
											cur = cur->next->next;
										}

										if (!rc)
										{
											/* I must have either a comma or right paren */
											if ((cur->tok_value != S_RIGHT_PAREN) &&
												(cur->tok_value != S_COMMA))
											{
												rc = INVALID_COLUMN_DEFINITION;
												cur->tok_value = INVALID;
											}
											else
											{
												if (cur->tok_value == S_RIGHT_PAREN)
												{
													column_done = true;
												}
												cur = cur->next;
											}
										}
									}
								}   // end of S_INT processing
								else
								{
									// It must be char()
									if (cur->tok_value != S_LEFT_PAREN)
									{
										rc = INVALID_COLUMN_DEFINITION;
										cur->tok_value = INVALID;
									}
									else
									{
										/* Enter char(n) processing */
										cur = cur->next;

										if (cur->tok_value != INT_LITERAL)
										{
											rc = INVALID_COLUMN_LENGTH;
											cur->tok_value = INVALID;
										}
										else
										{
											/* Got a valid integer - convert */
											col_entry[cur_id].col_len = atoi(cur->tok_string);
											cur = cur->next;

											if (cur->tok_value != S_RIGHT_PAREN)
											{
												rc = INVALID_COLUMN_DEFINITION;
												cur->tok_value = INVALID;
											}
											else
											{
												cur = cur->next;

												if ((cur->tok_value != S_COMMA) &&
													(cur->tok_value != K_NOT) &&
													(cur->tok_value != S_RIGHT_PAREN))
												{
													rc = INVALID_COLUMN_DEFINITION;
													cur->tok_value = INVALID;
												}
												else
												{
													if ((cur->tok_value == K_NOT) &&
														(cur->next->tok_value != K_NULL))
													{
														rc = INVALID_COLUMN_DEFINITION;
														cur->tok_value = INVALID;
													}
													else if ((cur->tok_value == K_NOT) &&
														(cur->next->tok_value == K_NULL))
													{					
														col_entry[cur_id].not_null = true;
														cur = cur->next->next;
													}

													if (!rc)
													{
														/* I must have either a comma or right paren */
														if ((cur->tok_value != S_RIGHT_PAREN) &&															  
															(cur->tok_value != S_COMMA))
														{
															rc = INVALID_COLUMN_DEFINITION;
															cur->tok_value = INVALID;
														}
														else
														{
															if (cur->tok_value == S_RIGHT_PAREN)
															{
																column_done = true;
															}
															cur = cur->next;
														}
													}
												}
											}
										}	/* end char(n) processing */
									}
								} /* end char processing */
							}
						}  // duplicate column name
					} // invalid column name

					/* If rc=0, then get ready for the next column */
					if (!rc)
					{
						cur_id++;
					}

				} while ((rc == 0) && (!column_done));

				if ((column_done) && (cur->tok_value != EOC))
				{
					rc = INVALID_TABLE_DEFINITION;
					cur->tok_value = INVALID;
				}

				if (!rc)
				{
					/* Now finished building tpd and add it to the tpd list */
					tab_entry.num_columns = cur_id;
					tab_entry.tpd_size = sizeof(tpd_entry) + 
						sizeof(cd_entry) *	tab_entry.num_columns;
					tab_entry.cd_offset = sizeof(tpd_entry);
					new_entry = (tpd_entry*)calloc(1, tab_entry.tpd_size);

					if (new_entry == NULL)
					{
						rc = MEMORY_ERROR;
					}
					else
					{
						memcpy((void*)new_entry,
							(void*)&tab_entry,
							sizeof(tpd_entry));

						memcpy((void*)((char*)new_entry + sizeof(tpd_entry)),
							(void*)col_entry,
							sizeof(cd_entry) * tab_entry.num_columns);

						rc = add_tpd_to_list(new_entry);

						free(new_entry);
						create_table_file(col_entry, &tab_entry);	//after semantic check of create statement code, call create_table_file procedure to create the table file
					}
				}
			}
		}
	}
	if(!rc){
		fwrite(get_timestamp(),1,14,log_file_ptr);
		fwrite(" \"",1,2,log_file_ptr);
		fwrite(command_to_execute,1,strlen(command_to_execute),log_file_ptr);
		fwrite("\"\n",1,2,log_file_ptr);
	}
	return rc;
}

int create_table_file(cd_entry col_entry[], tpd_entry *tab_entry) {
	int i = 0;
	int rc = 0;
	table_file_header header;
	header.file_size = sizeof(header);
	header.record_size = 0;
	for(i = 0; i< (*tab_entry).num_columns; i++) {
		header.record_size = (1 + col_entry[i].col_len) + header.record_size;
	}
	if((header.record_size % 4) != 0) {
		header.record_size = header.record_size + 4 - (header.record_size % 4) ;
	}

	header.num_records = 0 ;
	header.record_offset = sizeof(header);
	header.file_header_flag = 0;
	header.tpd_ptr = NULL;

	//printf("Header is %d %d %d %d %d\n",header.file_size, header.record_size, header.num_records, header.record_offset, header.file_header_flag); 
	FILE *fp,*fp1;
	char *filename = (*tab_entry).table_name  ;
	strcat(filename,".tab");
	//printf("Filename is: %s\n", filename);
	fp=fopen(filename, "w+");
	fwrite(&header,1,sizeof(table_file_header),fp);   //write file
	fclose(fp);
	printf("\nTable created successfully\n");
	return rc;

}

int sem_drop_table(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;

	cur = t_list;
	if ((cur->tok_class != keyword) &&
		(cur->tok_class != identifier) &&
		(cur->tok_class != type_name))
	{
		// Error
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
	}
	else
	{
		if (cur->next->tok_value != EOC)
		{
			rc = INVALID_STATEMENT;
			cur->next->tok_value = INVALID;
		}
		else
		{
			if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
			{
				rc = TABLE_NOT_EXIST;
				printf("\nTable to be dropped doesnot exist\n");
				cur->tok_value = INVALID;
			}
			else
			{
				/* delete the specific table file created by create statement*/
				char filename[MAX_IDENT_LEN+4];
				int status;
				strcpy(filename,(*tab_entry).table_name);
				strcat(filename,".tab");
				status = remove(filename);	//delete file
				//printf("\ndrop table filename in drop table is:%s\n", filename);
				if( status == 0 )
					printf("\nTable dropped successfully.\n");
				else {
					printf("\nUnable to drop the Table\n");
					perror("Error");
				}

				/* Found a valid tpd, drop it from tpd list */
				rc = drop_tpd_from_list(cur->tok_string);
			}
		}
	}
	if(!rc){
		fwrite(get_timestamp(),1,14,log_file_ptr);
		fwrite(" \"",1,2,log_file_ptr);
		fwrite(command_to_execute,1,strlen(command_to_execute),log_file_ptr);
		fwrite("\"\n",1,2,log_file_ptr);
	}
	return rc;
}

int sem_list_tables()
{
	int rc = 0;
	int num_tables = g_tpd_list->num_tables;
	tpd_entry *cur = &(g_tpd_list->tpd_start);

	if (num_tables == 0)
	{
		printf("\nThere are currently no tables defined\n");
	}
	else
	{
		printf("\nTable List\n");
		printf("*****************\n");
		while (num_tables-- > 0)
		{
			printf("%s\n", cur->table_name);
			if (num_tables > 0)
			{
				cur = (tpd_entry*)((char*)cur + cur->tpd_size);
			}
		}
		printf("****** End ******\n");
	}

	return rc;
}

int sem_list_schema(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;
	cd_entry  *col_entry = NULL;
	char tab_name[MAX_IDENT_LEN+1];
	char filename[MAX_IDENT_LEN+1];
	bool report = false;
	FILE *fhandle = NULL;
	int i = 0;

	cur = t_list;

	if (cur->tok_value != K_FOR)
	{
		rc = INVALID_STATEMENT;
		cur->tok_value = INVALID;
	}
	else
	{
		cur = cur->next;

		if ((cur->tok_class != keyword) &&
			(cur->tok_class != identifier) &&
			(cur->tok_class != type_name))
		{
			// Error
			rc = INVALID_TABLE_NAME;
			cur->tok_value = INVALID;
		}
		else
		{
			memset(filename, '\0', MAX_IDENT_LEN+1);
			strcpy(tab_name, cur->tok_string);
			cur = cur->next;

			if (cur->tok_value != EOC)
			{
				if (cur->tok_value == K_TO)
				{
					cur = cur->next;

					if ((cur->tok_class != keyword) &&
						(cur->tok_class != identifier) &&
						(cur->tok_class != type_name))
					{
						// Error
						rc = INVALID_REPORT_FILE_NAME;
						cur->tok_value = INVALID;
					}
					else
					{
						if (cur->next->tok_value != EOC)
						{
							rc = INVALID_STATEMENT;
							cur->next->tok_value = INVALID;
						}
						else
						{
							/* We have a valid file name */
							strcpy(filename, cur->tok_string);
							report = true;
						}
					}
				}
				else
				{ 
					/* Missing the TO keyword */
					rc = INVALID_STATEMENT;
					cur->tok_value = INVALID;
				}
			}

			if (!rc)
			{
				if ((tab_entry = get_tpd_from_list(tab_name)) == NULL)
				{
					rc = TABLE_NOT_EXIST;
					cur->tok_value = INVALID;
				}
				else
				{
					if (report)
					{
						if((fhandle = fopen(filename, "a+tc")) == NULL)
						{
							rc = FILE_OPEN_ERROR;
						}
					}

					if (!rc)
					{
						/* Find correct tpd, need to parse column and index information */

						/* First, write the tpd_entry information */
						printf("Table PD size            (tpd_size)    = %d\n", tab_entry->tpd_size);
						printf("Table Name               (table_name)  = %s\n", tab_entry->table_name);
						printf("Number of Columns        (num_columns) = %d\n", tab_entry->num_columns);
						printf("Column Descriptor Offset (cd_offset)   = %d\n", tab_entry->cd_offset);
						printf("Table PD Flags           (tpd_flags)   = %d\n\n", tab_entry->tpd_flags); 

						if (report)
						{
							fprintf(fhandle, "Table PD size            (tpd_size)    = %d\n", tab_entry->tpd_size);
							fprintf(fhandle, "Table Name               (table_name)  = %s\n", tab_entry->table_name);
							fprintf(fhandle, "Number of Columns        (num_columns) = %d\n", tab_entry->num_columns);
							fprintf(fhandle, "Column Descriptor Offset (cd_offset)   = %d\n", tab_entry->cd_offset);
							fprintf(fhandle, "Table PD Flags           (tpd_flags)   = %d\n\n", tab_entry->tpd_flags); 
						}

						/* Next, write the cd_entry information */
						for(i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
							i < tab_entry->num_columns; i++, col_entry++)
						{
							printf("Column Name   (col_name) = %s\n", col_entry->col_name);
							printf("Column Id     (col_id)   = %d\n", col_entry->col_id);
							printf("Column Type   (col_type) = %d\n", col_entry->col_type);
							printf("Column Length (col_len)  = %d\n", col_entry->col_len);
							printf("Not Null flag (not_null) = %d\n\n", col_entry->not_null);

							if (report)
							{
								fprintf(fhandle, "Column Name   (col_name) = %s\n", col_entry->col_name);
								fprintf(fhandle, "Column Id     (col_id)   = %d\n", col_entry->col_id);
								fprintf(fhandle, "Column Type   (col_type) = %d\n", col_entry->col_type);
								fprintf(fhandle, "Column Length (col_len)  = %d\n", col_entry->col_len);
								fprintf(fhandle, "Not Null Flag (not_null) = %d\n\n", col_entry->not_null);
							}
						}

						if (report)
						{
							fflush(fhandle);
							fclose(fhandle);
						}
					} // File open error							
				} // Table not exist
			} // no semantic errors
		} // Invalid table name
	} // Invalid statement

	return rc;
}

int initialize_tpd_list()
{
	int rc = 0;
	FILE *fhandle = NULL;
	struct _stat file_stat;

	/* Open for read */
	if((fhandle = fopen("dbfile.bin", "r+")) == NULL)
	{
		if((fhandle = fopen("dbfile.bin", "w+")) == NULL)
		{
			rc = FILE_OPEN_ERROR;
		}
		else
		{
			g_tpd_list = NULL;
			g_tpd_list = (tpd_list*)calloc(1, sizeof(tpd_list));

			if (!g_tpd_list)
			{
				rc = MEMORY_ERROR;
			}
			else
			{
				g_tpd_list->list_size = sizeof(tpd_list);
				fwrite(g_tpd_list, sizeof(tpd_list), 1, fhandle);
				fflush(fhandle);
				fclose(fhandle);
			}
		}
	}
	else
	{
		/* There is a valid dbfile.bin file - get file size */
		_fstat(_fileno(fhandle), &file_stat);
		printf("dbfile.bin size = %d\n", file_stat.st_size);

		g_tpd_list = (tpd_list*)calloc(1, file_stat.st_size);

		if (!g_tpd_list)
		{
			rc = MEMORY_ERROR;
		}
		else
		{
			fread(g_tpd_list, file_stat.st_size, 1, fhandle);
			fflush(fhandle);
			fclose(fhandle);

			if (g_tpd_list->list_size != file_stat.st_size)
			{
				printf("DB flags =  %d\n", g_tpd_list->db_flags);
				printf("list size =  %d\n", g_tpd_list->list_size);
				printf("num of tables =  %d\n", g_tpd_list->num_tables);
				rc = DBFILE_CORRUPTION;
			}

		}
	}

	return rc;
}

int add_tpd_to_list(tpd_entry *tpd)
{
	int rc = 0;
	int old_size = 0;
	FILE *fhandle = NULL;

	if((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
	{
		rc = FILE_OPEN_ERROR;
	}
	else
	{
		old_size = g_tpd_list->list_size;

		if (g_tpd_list->num_tables == 0)
		{
			/* If this is an empty list, overlap the dummy header */
			g_tpd_list->num_tables++;
			g_tpd_list->list_size += (tpd->tpd_size - sizeof(tpd_entry));
			fwrite(g_tpd_list, old_size - sizeof(tpd_entry), 1, fhandle);
		}
		else
		{
			/* There is at least 1, just append at the end */
			g_tpd_list->num_tables++;
			g_tpd_list->list_size += tpd->tpd_size;
			fwrite(g_tpd_list, old_size, 1, fhandle);
		}

		fwrite(tpd, tpd->tpd_size, 1, fhandle);
		fflush(fhandle);
		fclose(fhandle);
	}

	return rc;
}

int drop_tpd_from_list(char *tabname)
{
	int rc = 0;
	tpd_entry *cur = &(g_tpd_list->tpd_start);		//pointing cur to the start of g_tpd_list
	int num_tables = g_tpd_list->num_tables;
	bool found = false;
	int count = 0;

	if (num_tables > 0)
	{
		while ((!found) && (num_tables-- > 0))
		{
			if (_stricmp(cur->table_name, tabname) == 0)		//comparing tablename with the tablenames in the g_tpd_list
			{
				/* found it */
				found = true;
				int old_size = 0;
				FILE *fhandle = NULL;

				if((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
				{
					rc = FILE_OPEN_ERROR;
				}
				else
				{
					old_size = g_tpd_list->list_size;

					if (count == 0)
					{
						/* If this is the first entry */
						g_tpd_list->num_tables--;

						if (g_tpd_list->num_tables == 0)
						{
							/* This is the last table, null out dummy header */
							memset((void*)g_tpd_list, '\0', sizeof(tpd_list));
							g_tpd_list->list_size = sizeof(tpd_list);
							fwrite(g_tpd_list, sizeof(tpd_list), 1, fhandle);
						}
						else
						{
							/* First in list, but not the last one */
							g_tpd_list->list_size -= cur->tpd_size;

							/* First, write the 8 byte header */
							fwrite(g_tpd_list, sizeof(tpd_list) - sizeof(tpd_entry),
								1, fhandle);

							/* Now write everything starting after the cur entry */
							fwrite((char*)cur + cur->tpd_size,
								old_size - cur->tpd_size -
								(sizeof(tpd_list) - sizeof(tpd_entry)),
								1, fhandle);
						}
					}
					else
					{
						/* This is NOT the first entry - count > 0 */
						g_tpd_list->num_tables--;
						g_tpd_list->list_size -= cur->tpd_size;

						/* First, write everything from beginning to cur */
						fwrite(g_tpd_list, ((char*)cur - (char*)g_tpd_list),
							1, fhandle);

						/* Check if cur is the last entry. Note that g_tdp_list->list_size
						has already subtracted the cur->tpd_size, therefore it will
						point to the start of cur if cur was the last entry */
						if ((char*)g_tpd_list + g_tpd_list->list_size == (char*)cur)
						{
							/* If true, nothing else to write */
						}
						else
						{
							/* NOT the last entry, copy everything from the beginning of the
							next entry which is (cur + cur->tpd_size) and the remaining size */
							fwrite((char*)cur + cur->tpd_size,
								old_size - cur->tpd_size -
								((char*)cur - (char*)g_tpd_list),							     
								1, fhandle);
						}
					}

					fflush(fhandle);
					fclose(fhandle);
				}


			}
			else
			{
				if (num_tables > 0)
				{
					cur = (tpd_entry*)((char*)cur + cur->tpd_size);
					count++;
				}
			}
		}
	}

	if (!found)
	{
		rc = INVALID_TABLE_NAME;
	}

	return rc;
}

tpd_entry* get_tpd_from_list(char *tabname)
{
	tpd_entry *tpd = NULL;
	tpd_entry *cur = &(g_tpd_list->tpd_start);
	int num_tables = g_tpd_list->num_tables;
	bool found = false;

	if (num_tables > 0)
	{
		while ((!found) && (num_tables-- > 0))
		{
			if (_stricmp(cur->table_name, tabname) == 0)
			{
				/* found it */
				found = true;
				tpd = cur;
			}
			else
			{
				if (num_tables > 0)
				{
					cur = (tpd_entry*)((char*)cur + cur->tpd_size);
				}
			}
		}
	}

	return tpd;
}

int sem_insert_into(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry tab_entry;
	tpd_entry *new_entry = NULL;
	bool column_done = false;
	int cur_id = 0;
	cd_entry	col_entry[MAX_NUM_COL];


	//memset(&tab_entry, '\0', sizeof(tpd_entry));
	cur = t_list;
	if((cur->tok_class != keyword) &&
		(cur->tok_class != identifier) &&
		(cur->tok_class != type_name))
	{
		// Error
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
	}
	else
	{

		if ((new_entry = get_tpd_from_list(cur->tok_string)) == NULL)			//if new_entry pointer is not pointing to any table in tpd list
		{
			rc = TABLE_NOT_EXIST;
			cur->tok_value = INVALID;
		}
		else
		{
			//strcpy(tab_entry.table_name, cur->tok_string);
			cur = cur->next;
			if (cur->tok_value != K_VALUES)
			{
				//Error
				rc = INVALID_INSERT_DEFINITION;
				cur->tok_value = INVALID;
			}
			else {
				cur = cur->next;
				if (cur->tok_value != S_LEFT_PAREN)
				{
					//Error
					rc = INVALID_INSERT_DEFINITION;
					cur->tok_value = INVALID;
				}
				else
				{
					int bytes_read = read_table_file(new_entry);		//read total bytes from the table file
					if(bytes_read < 0){
						rc = bytes_read;								//return rc if total bytes_read are not equal to bytes in file
					}

					else{
						cur = cur->next;
						token_list *data_ptr = cur;
						if(cur->tok_value == S_COMMA || cur->tok_value == S_RIGHT_PAREN) {
							rc = INVALID_INSERT_DEFINITION;
							cur->tok_value = INVALID;
						}
						else{
							cd_entry *first_col_entry = get_cd_entry_from_tpd_entry(new_entry);
							cd_entry *local_col_entry = first_col_entry ;
							for(int i = 0; i<(*new_entry).num_columns; i++) {
								if(cur->tok_value == S_RIGHT_PAREN){
									rc = INVALID_INSERT_DEFINITION;
								}
								else {
									//if(local_col_entry[i].not_null == true) {
									if(local_col_entry[i].not_null == true && cur->tok_value == K_NULL) {
										rc = NOT_NULL_CONSTRAINT_VIOLATION;
										cur->tok_value = INVALID;
										printf("\nnot null violation while data entry for col name: %s\n", local_col_entry[i].col_name);
										break;
									}
									//}
									else{
										if(cur->tok_class == constant) {
											switch(cur->tok_value) {
											case INT_LITERAL:
												if(local_col_entry[i].col_type != T_INT){
													printf("\nData type mismatch\n");
													//printf("problem at column id %d and col name is %s\n", i,local_col_entry[i].col_name);
													rc = DATA_TYPE_MISMATCH;
												}
												break;

											case STRING_LITERAL:
												if(local_col_entry[i].col_type != T_CHAR){
													printf("\nData type mismatch\n");
													//printf("problem at column id %d and col name is %s\n", i,local_col_entry[i].col_name);
													rc = DATA_TYPE_MISMATCH;
												}
												else{
													if((strlen(cur->tok_string)) > local_col_entry[i].col_len){
														rc = COLUMN_LENGTH_EXCEEDED ;
													}
												}
												break;

											default: 
												rc = UNSUPPORTED_DATA_TYPE;
												break;
											} //end switch
										}
										if(rc){
											break;		//break from the for loop
										}
										else{
											//printf("successfully read the column id %d and col name is %s\n", i,local_col_entry[i].col_name);
											cur = cur->next;
											if(i != ((*new_entry).num_columns - 1)){
												if(cur->tok_value != S_COMMA) {
													rc = INVALID_INSERT_DEFINITION ; 
													break;
												}
												else{
													cur = cur->next;
												}
											}

										}
									}
								}


							} //end of for and completing semantic check for insert statement


							//everything correct,now write in memory
							if(rc == 0) {
								if(cur->tok_value == S_RIGHT_PAREN){              //to check if the insert statement is ending with right parenthesis
									cur = cur->next;
									if(cur->tok_value == EOC){                    //to check that the command gets over after end of command

										char *tab_data_ptr_memory  = (char*)tab_data_ptr + bytes_read;	//for going to the end of data in memory to insert the new record
										cd_entry *col_entry = get_cd_entry_from_tpd_entry(new_entry);



										//int record_size = 0;

										for(int i = 0; i<(*new_entry).num_columns; i++){
											int temp_int_data;
											char len;
											char str_len;
											int null_value =  0;


											switch(data_ptr->tok_value) {
											case INT_LITERAL:
												temp_int_data = atoi(data_ptr->tok_string);
												//str_len = strlen(data_ptr->tok_string);
												len = 4;
												memcpy(tab_data_ptr_memory, &len,1);    //write length of data before writing data
												tab_data_ptr_memory = (char*)tab_data_ptr_memory + 1 ; 
												memcpy(tab_data_ptr_memory, &temp_int_data,4);		//writing actual int data after its length
												//memcpy(tab_data_ptr_memory, &str_len,4);
												tab_data_ptr_memory = (char*)tab_data_ptr_memory + 4 ; 
												//printf("\nLength of integer is: %d and integer value is %d\n", len, temp_int_data);
												//printf("\nLength of integer is: %d and integer value is %s\n", len, data_ptr->tok_string);
												//record_size = record_size + 5 ;
												break;

											case STRING_LITERAL:
												str_len = strlen(data_ptr->tok_string);
												memcpy(tab_data_ptr_memory, &str_len,1);
												tab_data_ptr_memory = (char*)tab_data_ptr_memory + 1 ;
												memcpy(tab_data_ptr_memory, data_ptr->tok_string,str_len);
												tab_data_ptr_memory = (char*)tab_data_ptr_memory + col_entry[i].col_len;
												//printf("\nLength of string data is: %d and string data is %s\n", str_len, data_ptr->tok_string);
												//record_size = record_size + 1 + str_len;
												break;

											case K_NULL:
												memcpy(tab_data_ptr_memory,&null_value,1);
												//printf("\nLength of data is: 0 and data is NULL");
												tab_data_ptr_memory = (char*)tab_data_ptr_memory + col_entry[i].col_len + 1 ;
												//record_size = record_size + 1 ;
												break;
											} //end switch

											data_ptr = data_ptr->next->next;
										}	//end for

										//updating header
										header1.file_size = sizeof(table_file_header) + header1.record_size + bytes_read ;
										int total_records_data = header1.record_size + bytes_read ;
										header1.num_records += 1;
										header1.tpd_ptr = NULL;

										FILE *fp1;
										char filename[MAX_IDENT_LEN+4];
										strcpy(filename,(*new_entry).table_name);
										strcat(filename,".tab");
										//printf("filename of the file which has to be updated by insert statement is: %s " , filename);

										fp1=fopen(filename, "w");
										if(fp1 == NULL){
											rc = FILE_OPEN_ERROR;
										}
										else {//writing header and records in the file
											//printf("\nHeader fwrite data: %d\n", fwrite(&header1,1,sizeof(table_file_header),fp1)); 
											//printf("\nRecords fwrite data: %d\n", fwrite(tab_data_ptr,1,total_records_data,fp1)); 
											//printf("Writing headersize: %d", header1.file_size);
											fwrite(&header1,1,sizeof(table_file_header),fp1);
											fwrite(tab_data_ptr,1,total_records_data,fp1);
											fclose(fp1);
											printf("Header1: %d %d %d %d %d",header1.file_size, header1.record_size, header1.num_records, header1.record_offset, header1.file_header_flag); 
										}

									}


									else {
										rc = INVALID_INSERT_DEFINITION;
									}
								}
								else {
									rc = INVALID_INSERT_DEFINITION;
								}
							}
						}

					}

				}
			}
		}
	}
	if(!rc){
		fwrite(get_timestamp(),1,14,log_file_ptr);
		fwrite(" \"",1,2,log_file_ptr);
		fwrite(command_to_execute,1,strlen(command_to_execute),log_file_ptr);
		fwrite("\"\n",1,2,log_file_ptr);
	}
	return rc;
}

// This populates global header1 and tab_data_ptr (pointitng the data aftr the record_offset)
int read_table_file(tpd_entry *new_entry)
{
	FILE *fp1;
	int rc = 0;
	int i;
	char filename[MAX_IDENT_LEN+4];
	strcpy(filename,(*new_entry).table_name);
	strcat(filename,".tab");
	//printf("filename in read_table_file is: %s " , filename);

	fp1=fopen(filename, "r");
	if(fp1 == NULL){
		rc = FILE_OPEN_ERROR;
	}
	else {

		int header_data = fread(&header1,1,sizeof(table_file_header),fp1);   //read file
		if(header_data < sizeof(table_file_header)){
			rc = TABLE_FILE_CORRUPTION;
		}
		else
		{
			//printf("Read header with file size: %d", header1.file_size);
			//printf("Header1: %d %d %d %d %d",header1.file_size, header1.record_size, header1.num_records, header1.record_offset, header1.file_header_flag); 
			max_file_size = header1.record_size * 1000 ;		
			tab_data_ptr = (char*)malloc(max_file_size);
			if(tab_data_ptr == NULL) {
				printf("\nError: Out of memory while allocating memory for reading table data\n");
				rc = MEMORY_ERROR;
			}
			else{
				// char *cur_data_ptr = (char *)tab_data_ptr;

				memset (tab_data_ptr, '\0', max_file_size);  //max_file_size is total file size excluding header
				//fseek(fp1,header1.record_offset,SEEK_SET);
				int bytes_read = fread(tab_data_ptr,1,max_file_size,fp1); //bytes_read returns the total bytes of records excluding header
				//printf("Bytes read is: %d" , bytes_read);
				rc = bytes_read;
			}//else ends
		}
		fclose(fp1);

	}
	return rc;
}

int find_col_type(tpd_entry *tab_entry, int col_id)
{
	//for getting the column type


	tpd_entry *cur;
	cur = tab_entry;
	cd_entry *col_entry = (cd_entry*)((char*)cur + cur->cd_offset);
	cd_entry *desired_col_entry = col_entry + col_id ;
	return (*desired_col_entry).col_type;
}

/* This method gives the pointer to first cd_entry of the tpd_entry */
cd_entry* get_cd_entry_from_tpd_entry(tpd_entry *cur) {
	tpd_entry *tab_entry = cur;
	return (cd_entry*)((char*)tab_entry+tab_entry->cd_offset);
}

int sem_delete_from(token_list *t_list) {
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;
	cd_entry *column_entry1;
	int bytes_read = 0;


	cur = t_list;
	if ((cur->tok_class != keyword) &&
		(cur->tok_class != identifier) &&
		(cur->tok_class != type_name))
	{
		// Error
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
	}
	else
	{
		if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
		{
			rc = TABLE_NOT_EXIST;
			printf("\nTable to be deleted doesnot exist\n");
			cur->tok_value = INVALID;
		}


		else
		{
			if (cur->next->tok_value == EOC)
			{
				bytes_read = read_table_file(tab_entry);		//read total bytes from the table file
				if(bytes_read < 0){
					rc = bytes_read;								//return rc if total bytes_read are not equal to bytes in file
				}

				else{

					if(header1.num_records == 0){
						rc = RECORDS_NOT_FOUND;
						printf("\nRecords not found\n");	
					}

					else{
						//updating header
						header1.file_size = sizeof(table_file_header);
						header1.num_records = 0;


						char filename[MAX_IDENT_LEN+4];
						strcpy(filename,(*tab_entry).table_name);
						strcat(filename,".tab");
						//printf("filename of the file whose records have to be deleted is: %s " , filename);

						FILE *fp1;
						fp1=fopen(filename,"w");
						if(fp1 == NULL){
							rc = FILE_OPEN_ERROR;
						}
						else {//writing data in the file after deleting all the records
							//printf("Header fwrite data: %d", fwrite(&header1,1,sizeof(table_file_header),fp1));
							fwrite(&header1,1,sizeof(table_file_header),fp1);
							printf("Table deleted successfully");
							fclose(fp1);
						}
					}

				}
			}

			else{

				cur = cur->next ;
				token_list *cur_delete = cur;

				if(cur->tok_value != K_WHERE){
					rc = INVALID_DELETE_DEFINITION;
					cur->tok_value = INVALID;
				}
				else{
					cur = cur->next ;
					if((cur->tok_class != keyword) &&
						(cur->tok_class != identifier) &&
						(cur->tok_class != type_name))
					{
						// Error
						rc = INVALID_DELETE_DEFINITION;
						cur->tok_value = INVALID;
					}

					else{
						//cd_entry *first_col_entry = get_cd_entry_from_tpd_entry(new_entry);
						if ((column_entry1 = get_column_from_table((cur->tok_string), tab_entry)) == NULL)			//if column_entry pointer is not pointing to any column 
						{
							rc = COLUMN_NOT_EXIST;
							cur->tok_value = INVALID;
						}
						else{
							//printf("\ncolumn found\n");
							cur = cur->next;
							if(cur->tok_value != S_EQUAL && cur->tok_value != S_GREATER && cur->tok_value != S_LESS){
								//Error
								rc = INVALID_DELETE_DEFINITION;
								cur->tok_value = INVALID;
							}
							else  {
								int relational_operator = cur->tok_value ;
								cur = cur->next;
								if(cur->tok_class != constant){
									rc = INVALID_DELETE_DEFINITION;
									cur->tok_value = INVALID;
								}
								else{

									switch (cur->tok_value)
									{
									case INT_LITERAL:
										if((*column_entry1).col_type != T_INT){
											printf("\nData type mismatch\n");
											rc = DATA_TYPE_MISMATCH;
										}

										break;

									case STRING_LITERAL:
										if((*column_entry1).col_type != T_CHAR){
											printf("\nData type mismatch\n");
											rc = DATA_TYPE_MISMATCH;
										}
										else{
											if((strlen(cur->tok_string)) > (*column_entry1).col_len){
												rc = COLUMN_LENGTH_EXCEEDED ;
											}
										}
										break;

									default: 
										rc = UNSUPPORTED_DATA_TYPE;
										break;
									}

									if(!rc){
										if(cur->next->tok_value != EOC){
											rc = INVALID_DELETE_DEFINITION;
											cur->tok_value = INVALID;
										}
										else{
											bytes_read = read_table_file(tab_entry);
											rc =  delete_record_data(tab_entry,column_entry1,cur,bytes_read,relational_operator);
										}
									}
								}

							}
						}

					}

				}

			}
		}
	}
	if(!rc){
		fwrite(get_timestamp(),1,14,log_file_ptr);
		fwrite(" \"",1,2,log_file_ptr);
		fwrite(command_to_execute,1,strlen(command_to_execute),log_file_ptr);
		fwrite("\"\n",1,2,log_file_ptr);
	}
	return rc;

}

int sem_update(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry tab_entry;
	tpd_entry *new_entry = NULL;
	bool column_done = false;
	int cur_id = 0;
	cd_entry	col_entry[MAX_NUM_COL];
	cd_entry *column_entry = NULL;
	cd_entry *column_entry1 = NULL;


	cur = t_list;

	if((cur->tok_class != keyword) &&
		(cur->tok_class != identifier) &&
		(cur->tok_class != type_name))
	{
		// Error
		rc = INVALID_UPDATE_DEFINITION;
		cur->tok_value = INVALID;
	}
	else
	{

		if ((new_entry = get_tpd_from_list(cur->tok_string)) == NULL)			//if new_entry pointer is not pointing to any table in tpd list
		{
			rc = TABLE_NOT_EXIST;
			cur->tok_value = INVALID;
		}
		else
		{
			cur = cur->next;
			if (cur->tok_value != K_SET)
			{
				//Error
				rc = INVALID_UPDATE_DEFINITION;
				cur->tok_value = INVALID;
			}
			else {
				int bytes_read = read_table_file(new_entry);		//read total bytes from the table file
				if(bytes_read < 0){
					rc = bytes_read;								//return rc if total bytes_read are not equal to bytes in file
				}
				else{

					if(header1.num_records == 0){
						rc = RECORDS_NOT_FOUND;
						printf("\nRecords not found\n");
					}
					else {
						cur = cur->next;
						if((cur->tok_class != keyword) &&
							(cur->tok_class != identifier) &&
							(cur->tok_class != type_name))
						{
							// Error
							rc = INVALID_UPDATE_DEFINITION;
							cur->tok_value = INVALID;
						}
						else{
							cd_entry *first_col_entry = get_cd_entry_from_tpd_entry(new_entry);
							if ((column_entry = get_column_from_table((cur->tok_string), new_entry)) == NULL)			//if new_entry pointer is not pointing to any table in tpd list
							{
								rc = COLUMN_NOT_EXIST;
								cur->tok_value = INVALID;
							}
							else{
								//printf("\ncolumn found\n");
								cur = cur->next;
								if(cur->tok_value != S_EQUAL){
									//Error
									rc = INVALID_UPDATE_DEFINITION;
									cur->tok_value = INVALID;
								}
								else{
									cur = cur->next ;
									if((*column_entry).not_null == true && cur->tok_value == K_NULL) {
										rc = NOT_NULL_CONSTRAINT_VIOLATION;
										cur->tok_value = INVALID;
										printf("\nNOT NULL violation\n");
									}
									else if(cur->tok_class != constant && cur->tok_value != K_NULL) {
										rc = INVALID_UPDATE_DEFINITION;
										cur->tok_value = INVALID;
									}
									else{
										switch (cur->tok_value)
										{
										case INT_LITERAL:
											if((*column_entry).col_type != T_INT){
												printf("\nData type mismatch\n");
												rc = DATA_TYPE_MISMATCH;
											}
											break;

										case STRING_LITERAL:
											if((*column_entry).col_type != T_CHAR){
												printf("\nData type mismatch\n");
												rc = DATA_TYPE_MISMATCH;
											}
											else{
												if((strlen(cur->tok_string)) > (*column_entry).col_len){
													rc = COLUMN_LENGTH_EXCEEDED ;
												}
											}
											break;
										case K_NULL:
											break;

										default: 
											rc = UNSUPPORTED_DATA_TYPE;
											break;
										}

										if(!rc) {
											/*if(cur->next->tok_value != EOC){
											rc = INVALID_UPDATE_DEFINITION;
											cur->tok_value = INVALID;
											}*/
											if(cur->next->tok_value == EOC){
												rc = update_column_data(new_entry,column_entry,NULL,cur,NULL,bytes_read,-1);

											}

											else{
												//Looking for WHERE clause
												token_list *cur_update = cur;

												if(cur->next->tok_value != K_WHERE){
													rc = INVALID_UPDATE_DEFINITION;
													cur->tok_value = INVALID;
												}
												else{
													cur = cur->next->next ;
													if((cur->tok_class != keyword) &&
														(cur->tok_class != identifier) &&
														(cur->tok_class != type_name))
													{
														// Error
														rc = INVALID_UPDATE_DEFINITION;
														cur->tok_value = INVALID;
													}

													else{
														//cd_entry *first_col_entry = get_cd_entry_from_tpd_entry(new_entry);
														if ((column_entry1 = get_column_from_table((cur->tok_string), new_entry)) == NULL)			//if column_entry pointer is not pointing to any column 
														{
															rc = COLUMN_NOT_EXIST;
															cur->tok_value = INVALID;
														}
														else{
															//printf("\ncolumn found\n");
															cur = cur->next;
															if(cur->tok_value != S_EQUAL && cur->tok_value != S_GREATER && cur->tok_value != S_LESS){
																//Error
																rc = INVALID_UPDATE_DEFINITION;
																cur->tok_value = INVALID;
															}
															else  {
																int relational_operator = cur->tok_value ;
																cur = cur->next;
																if(cur->tok_class != constant){
																	rc = INVALID_UPDATE_DEFINITION;
																	cur->tok_value = INVALID;
																}
																else{

																	switch (cur->tok_value)
																	{
																	case INT_LITERAL:
																		if((*column_entry1).col_type != T_INT){
																			printf("\nData type mismatch\n");
																			rc = DATA_TYPE_MISMATCH;
																		}

																		break;

																	case STRING_LITERAL:
																		if((*column_entry1).col_type != T_CHAR){
																			printf("\nData type mismatch\n");
																			rc = DATA_TYPE_MISMATCH;
																		}

																		break;

																	default: 
																		rc = UNSUPPORTED_DATA_TYPE;
																		break;
																	}

																	if(!rc){


																		if(cur->next->tok_value != EOC){
																			rc = INVALID_UPDATE_DEFINITION;
																			cur->tok_value = INVALID;
																		}
																		else{

																			rc =  update_column_data(new_entry,column_entry,column_entry1,cur_update,cur,bytes_read,relational_operator);

																		}

																	}
																}

															}
														}

													}


												}

											}
										}
									}
								}
							}


						}

					}
				}

			}

		}
	}
	if(!rc){
		fwrite(get_timestamp(),1,14,log_file_ptr);
		fwrite(" \"",1,2,log_file_ptr);
		fwrite(command_to_execute,1,strlen(command_to_execute),log_file_ptr);
		fwrite("\"\n",1,2,log_file_ptr);
	}
	return rc;
}


int sem_select(token_list *t_list) {
	int rc = 0;
	token_list *cur,*aggregate_column_tok = NULL,*where_column1_tok = NULL,*where_column2_tok = NULL;
	tpd_entry *tab_entry = NULL;
	char * select_data_ptr1;
	char *selected_data_ptr ;  // to store selected rows
	char *col_names[MAX_NUM_COL] ;
	int num_columns = 0;
	bool select_star = false;
	int function_name = -1 ;  //to store type of aggregate function
	cd_entry *aggregate_col_entry = NULL;
	cd_entry *where_column_entry1 = NULL,*where_column_entry2= NULL ;
	int and_or_operator = -1;
	int counter_rows_selected = 0;
	int relational_operator1 = -1;
	bool aggregate_count_star_checked = false ;

	cur = t_list;

	if(cur->tok_value == S_STAR){
		//select * from tablename 
		cur = cur->next;
		if((cur->tok_class != keyword && cur->tok_value != K_FROM)){
			// Error
			rc = INVALID_SELECT_DEFINITION;
			cur->tok_value = INVALID;
		}
		select_star = true;
	}
	else{
		if(cur->tok_value != F_AVG && cur->tok_value != F_SUM && cur->tok_value != F_COUNT){
			while (cur->tok_class != terminator && cur->tok_value != EOC && num_columns < MAX_NUM_COL)
			{
				//parse for columns
				if((cur->tok_class != keyword) &&
					(cur->tok_class != identifier) &&
					(cur->tok_class != type_name))
				{
					// Error
					rc = INVALID_SELECT_DEFINITION;
					cur->tok_value = INVALID;
					break;
				}
				col_names[num_columns] = cur->tok_string;
				num_columns++ ;
				cur = cur->next;
				if((cur->tok_class == keyword && cur->tok_value == K_FROM)){
					break;
				}
				else{
					// it should be a comma
					if(cur->tok_value != S_COMMA){
						// Error
						rc = INVALID_SELECT_DEFINITION;
						cur->tok_value = INVALID;
						break;
					}
					else{
						cur = cur->next;
					}
				}
			} //end while

			if(!rc){
				if(cur->tok_value == EOC){
					// Error
					rc = INVALID_SELECT_DEFINITION;
					cur->tok_value = INVALID;
				}
				else if(num_columns == MAX_NUM_COL && cur->tok_class != keyword && cur->tok_value != K_FROM){
					// Error
					rc = INVALID_SELECT_DEFINITION;
					cur->tok_value = INVALID;
				}

			}
		}
		else{
			//code for sum, avg and count
			function_name = cur->tok_value ;
			if(function_name != F_SUM && function_name != F_AVG && function_name != F_COUNT){
				rc = INVALID_SELECT_DEFINITION ;
				cur->tok_value = INVALID;
			}
			else{
				cur = cur->next;
				if(cur->tok_value != S_LEFT_PAREN && cur->next->next->tok_value != S_RIGHT_PAREN){
					rc = INVALID_SELECT_DEFINITION ;
					cur->tok_value = INVALID;
				}
				else{
					cur = cur->next ;

					if(function_name == F_COUNT){
						if(cur->tok_value == S_STAR){
							aggregate_count_star_checked = true;
							aggregate_column_tok = cur ;
						}
					}
					if(aggregate_count_star_checked ==  false){
						if((cur->tok_class != keyword) &&
							(cur->tok_class != identifier) &&
							(cur->tok_class != type_name))
						{
							// Error

							rc = INVALID_SELECT_DEFINITION;
							cur->tok_value = INVALID;
						}
						else{
							aggregate_column_tok = cur ;

						}
					}
					if(!rc){
						cur = cur->next->next ; // now cur will point to FROM keyword
					}
				}
			}
		}
	}	//at this moment cur is pointing to "FROM" keyword

	if(!rc) {
		if(cur->tok_value != K_FROM){
			rc = INVALID_SELECT_DEFINITION ;
			cur->tok_value = INVALID;
		}

		if(!rc){
			cur = cur->next;

			if ((cur->tok_class != keyword) &&
				(cur->tok_class != identifier) &&
				(cur->tok_class != type_name))
			{
				// Error
				rc = INVALID_TABLE_NAME;
				cur->tok_value = INVALID;
			}
			else
			{
				if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
				{
					rc = TABLE_NOT_EXIST;
					printf("\nTable for which the data has to be selected doesnot exist\n");
					cur->tok_value = INVALID;
				}
				else
				{
					//Table found
					int bytes_read = read_table_file(tab_entry);		//read total bytes from the table file
					if(bytes_read < 0){
						rc = bytes_read;								//return rc if total bytes_read are not equal to bytes in file
					}
					else{
						if(header1.num_records == 0){
							if(function_name == F_COUNT){
								printf("\nCOUNT\n0") ;
							}
							else{
								rc = RECORDS_NOT_FOUND;
								printf("\nRecords not found\n");	
							}
						}

						else{
							//some records are there in the table

							//check if aggregate function exist
							if(function_name > 0){								
								if(aggregate_count_star_checked ==  false){
									if ((aggregate_col_entry = get_column_from_table((aggregate_column_tok->tok_string), tab_entry)) == NULL)			//if aggregate_col_entry pointer is not pointing to any column in tab_entry
									{
										rc = COLUMN_NOT_EXIST;
										aggregate_column_tok->tok_value = INVALID;
										cur = aggregate_column_tok;
									}
									else{							
										if(function_name == F_SUM || function_name == F_AVG){
											// Data type of aggregate column should be int
											if(aggregate_col_entry->col_type != T_INT){
												rc = DATA_TYPE_MISMATCH;
											}
										}
									}
								}
							}

							if(!rc) {
								cd_entry *order_by_col_entry = NULL; 										
								bool desc = false ;
								selected_data_ptr = (char *)malloc(header1.file_size);
								memset(selected_data_ptr,'\0',header1.file_size);
								cur = cur->next ; //either EOC or where or ORDER BY
								if(cur->tok_value != EOC && cur->tok_value != K_ORDER && cur->tok_value != K_WHERE){
									rc = INVALID_SELECT_DEFINITION;
									cur->tok_value = INVALID;
								}
								else{
									switch(cur->tok_value){
									case K_WHERE:
										cur = cur->next ;
										if((cur->tok_class != keyword) &&
											(cur->tok_class != identifier) &&
											(cur->tok_class != type_name))
										{
											// Error
											rc = INVALID_SELECT_DEFINITION;
											cur->tok_value = INVALID;
										}

										else{
											if ((where_column_entry1 = get_column_from_table((cur->tok_string), tab_entry)) == NULL)			 
											{
												rc = COLUMN_NOT_EXIST;
												cur->tok_value = INVALID;
											}
											else{											
												cur = cur->next;
												if(cur->tok_value != S_EQUAL && cur->tok_value != S_GREATER && cur->tok_value != S_LESS && !(cur->tok_value == K_IS && cur->next->tok_value == K_NULL) && !(cur->tok_value == K_IS &&  cur->next->tok_value == K_NOT && cur->next->next->tok_value == K_NULL) ){
													//Error
													rc = INVALID_SELECT_DEFINITION;
													cur->tok_value = INVALID;
												}
												else  {

													int relational_operator = cur->tok_value ;

													if(relational_operator == K_IS){
														if(cur->next->tok_value == K_NULL){
															relational_operator = K_NULL ;
															printf("\nfound IS NULL\n");
															cur = cur->next->next ;
														}
														else if(cur->next->tok_value == K_NOT){
															relational_operator = K_NOT ;
															printf("\nfound IS NOT NULL\n");
															cur = cur->next->next->next ;
														}
													}
													else{
														cur=cur->next;
														if(cur->tok_class != constant){
															rc = INVALID_SELECT_DEFINITION;
															cur->tok_value = INVALID;
														}
														else{
															switch (cur->tok_value)
															{
															case INT_LITERAL:
																if((*where_column_entry1).col_type != T_INT){
																	printf("\nData type mismatch\n");
																	rc = DATA_TYPE_MISMATCH;
																}
																break;

															case STRING_LITERAL:
																if((*where_column_entry1).col_type != T_CHAR){
																	printf("\nData type mismatch\n");
																	rc = DATA_TYPE_MISMATCH;
																}														
																break;

															default: 
																rc = UNSUPPORTED_DATA_TYPE;
																break;
															}
														}
														if(!rc){
															where_column1_tok = cur;
															cur= cur->next;
														}
													}   // now cur is pointing to either of AND , OR, EOC, ORDER

													//checking for AND or OR
													if(!rc) {
														if(cur->tok_value != EOC && cur->tok_value != K_ORDER){
															if(cur->tok_value != K_AND && cur->tok_value != K_OR){
																rc = INVALID_SELECT_DEFINITION;
																cur->tok_value = INVALID;														
															}
															else{
																and_or_operator = cur->tok_value ;  // it would be either AND or OR
																cur = cur->next ; //now cur is pointing to another column for compare

																if ((where_column_entry2 = get_column_from_table((cur->tok_string), tab_entry)) == NULL)			 
																{
																	rc = COLUMN_NOT_EXIST;
																	cur->tok_value = INVALID;
																}
																else{											
																	cur = cur->next;
																	if(cur->tok_value != S_EQUAL && cur->tok_value != S_GREATER && cur->tok_value != S_LESS && !(cur->tok_value == K_IS && cur->next->tok_value == K_NULL) && !(cur->tok_value == K_IS &&  cur->next->tok_value == K_NOT && cur->next->next->tok_value == K_NULL) ){
																		//Error
																		rc = INVALID_SELECT_DEFINITION;
																		cur->tok_value = INVALID;
																	}
																	else{

																		relational_operator1 = cur->tok_value ;

																		if(relational_operator1 == K_IS){
																			if(cur->next->tok_value == K_NULL){
																				relational_operator1 = K_NULL ;
																				printf("\nfound IS NULL\n");
																				cur = cur->next->next ;
																			}
																			else if(cur->next->tok_value == K_NOT){
																				relational_operator1 = K_NOT ;
																				printf("\nfound IS NOT NULL\n");
																				cur = cur->next->next->next ;
																			}
																		}
																		else{
																			cur=cur->next;
																			if(cur->tok_class != constant){
																				rc = INVALID_SELECT_DEFINITION;
																				cur->tok_value = INVALID;
																			}
																			else{
																				switch (cur->tok_value)
																				{
																				case INT_LITERAL:
																					if((*where_column_entry2).col_type != T_INT){
																						printf("\nData type mismatch\n");
																						rc = DATA_TYPE_MISMATCH;
																					}
																					break;

																				case STRING_LITERAL:
																					if((*where_column_entry2).col_type != T_CHAR){
																						printf("\nData type mismatch\n");
																						rc = DATA_TYPE_MISMATCH;
																					}														
																					break;

																				default: 
																					rc = UNSUPPORTED_DATA_TYPE;
																					break;
																				}
																			}
																			if(!rc){
																				where_column2_tok = cur;
																				cur= cur->next;
																			}
																		} 
																	}
																}

															}
														}
													}
													if(!rc){
														if(cur->tok_value == K_ORDER){														
															if(!rc){
																// for order by
																cur = cur->next;
																if(cur->tok_value != K_BY){
																	rc = INVALID_SELECT_DEFINITION ;
																	cur->tok_value = INVALID ;
																}
																else{
																	cur = cur->next ;  //now cur is pointing to column name
																	if((cur->tok_class != keyword) &&
																		(cur->tok_class != identifier) &&
																		(cur->tok_class != type_name))
																	{
																		// Error
																		rc = INVALID_SELECT_DEFINITION;
																		cur->tok_value = INVALID;
																	}
																	else{
																		//cd_entry *first_col_entry = get_cd_entry_from_tpd_entry(tab_entry);
																		if ((order_by_col_entry = get_column_from_table((cur->tok_string), tab_entry)) == NULL)			//if new_entry pointer is not pointing to any table in tpd list
																		{
																			rc = COLUMN_NOT_EXIST;
																			cur->tok_value = INVALID;
																		}
																		else{
																			//column found
																			if(cur->next->tok_value != EOC){
																				cur = cur->next;
																				if(cur->tok_value != K_DESC){
																					rc = INVALID_SELECT_DEFINITION;
																					cur->tok_value = INVALID;
																				}
																				else{
																					desc = true ;
																					cur = cur->next ; // the pointer should be pointing to EOC now
																				}
																			}
																			else{
																				//ascending order
																				desc = false;
																			}

																			if(!rc){
																				rc = select_data(selected_data_ptr,tab_entry,where_column_entry1,where_column_entry2,where_column1_tok,where_column2_tok, bytes_read, relational_operator,relational_operator1,and_or_operator, &counter_rows_selected);
																				sort_data(selected_data_ptr,order_by_col_entry,tab_entry,counter_rows_selected,desc);
																			}
																		}
																	}
																}
															}
														}

														else{
															// it should be EOC
															if(cur->tok_value != EOC){
																rc = INVALID_SELECT_DEFINITION;
																cur->tok_value = INVALID;
															}
															else{
																rc = select_data(selected_data_ptr,tab_entry,where_column_entry1,where_column_entry2,where_column1_tok,where_column2_tok, bytes_read, relational_operator,relational_operator1,and_or_operator, &counter_rows_selected);
															}
														}													
													} 
												}
											}
										}

										break;
									case EOC:
										if(!rc) {
											rc = select_data(selected_data_ptr,tab_entry,NULL,NULL,NULL,NULL, bytes_read, -1, -1,-1,&counter_rows_selected);											
										}
										break;
									case K_ORDER:
										if(!rc){
											cur = cur->next;
											if(cur->tok_value != K_BY){
												rc = INVALID_SELECT_DEFINITION ;
												cur->tok_value = INVALID ;
											}
											else{
												cur = cur->next;  //now cur is pointing to column name
												if((cur->tok_class != keyword) &&
													(cur->tok_class != identifier) &&
													(cur->tok_class != type_name))
												{
													// Error
													rc = INVALID_SELECT_DEFINITION;
													cur->tok_value = INVALID;
												}
												else{
													//cd_entry *first_col_entry = get_cd_entry_from_tpd_entry(tab_entry);
													if ((order_by_col_entry = get_column_from_table((cur->tok_string), tab_entry)) == NULL)			//if new_entry pointer is not pointing to any table in tpd list
													{
														rc = COLUMN_NOT_EXIST;
														cur->tok_value = INVALID;
													}
													else{
														//column found
														if(cur->next->tok_value != EOC){
															cur = cur->next;
															if(cur->tok_value != K_DESC){
																rc = INVALID_SELECT_DEFINITION;
																cur->tok_value = INVALID;
															}
															else{
																desc = true ;
																cur = cur->next ; // the pointer should be pointing to EOC now
															}
														}
														else{
															//ascending order
															desc = false;
														}

														if(!rc){
															rc = select_data(selected_data_ptr,tab_entry,NULL,NULL,NULL,NULL, bytes_read, -1, -1,-1,&counter_rows_selected);
															sort_data(selected_data_ptr,order_by_col_entry,tab_entry,counter_rows_selected,desc);
														}
													}
												}
											}

										}
										break;
									default:
										rc = INVALID_SELECT_DEFINITION;
										cur->tok_value = INVALID;
										break;
									}
									if(!rc){
										if(function_name > 0){
											int sum = 0;
											char *tab_data_ptr_column_compare = selected_data_ptr ;
											int data_length_compare = 0;

											char *selected_record_ptr = selected_data_ptr ;
											cd_entry *first_col_entry = get_cd_entry_from_tpd_entry(tab_entry);
											int avg = 0;			
											char* aggregate_col_name = (char*)malloc(strlen(aggregate_column_tok->tok_string) + 8);
											memset(aggregate_col_name, '\0', strlen(aggregate_column_tok->tok_string) + 8);
											int total_non_null_rows = 0;
											switch(function_name){
											case F_SUM:										
											case F_AVG:
												if(counter_rows_selected == 0){
													rc = RECORDS_NOT_FOUND ;
													printf("\nRecords not found\n");
												}

												else{
													total_non_null_rows = 0;
													for(int i=0; i<counter_rows_selected ; i++){
														tab_data_ptr_column_compare  = selected_record_ptr ;   // putting tab_data_ptr_column_compare to the starting of record
														for(int j = 0 ; j < aggregate_col_entry->col_id ; j++){
															tab_data_ptr_column_compare = tab_data_ptr_column_compare + first_col_entry[j].col_len + 1 ;
														}													

														data_length_compare = *tab_data_ptr_column_compare;											
														int column_data_to_compare = 0;
														tab_data_ptr_column_compare +=1 ;

														if(data_length_compare > 0){
															total_non_null_rows++;
															memcpy(&column_data_to_compare,tab_data_ptr_column_compare,data_length_compare);														
															sum = sum + column_data_to_compare ;														
														}
														selected_record_ptr = selected_record_ptr + header1.record_size ;  //moving the pointer to the next selected record
													}

													if(function_name == F_SUM){
														strcpy(aggregate_col_name, "SUM(");
														strcat(aggregate_col_name, aggregate_column_tok->tok_string);
														strcat(aggregate_col_name, ")");
														printf("\n%s\n",aggregate_col_name);
														print_int_data(sum, aggregate_col_name, true);
													}

													else{
														//avg found
														avg = sum / total_non_null_rows ;
														strcpy(aggregate_col_name, "AVG(");
														strcat(aggregate_col_name, aggregate_column_tok->tok_string);
														strcat(aggregate_col_name, ")");
														printf("\n%s\n",aggregate_col_name);
														print_int_data(avg, aggregate_col_name, true);
													}
												}
												break;
											case F_COUNT:
												total_non_null_rows = 0;
												if(aggregate_count_star_checked == false) {
													for(int i=0; i<counter_rows_selected ; i++){
														tab_data_ptr_column_compare  = selected_record_ptr ;   // putting tab_data_ptr_column_compare to the starting of record
														for(int j = 0 ; j < aggregate_col_entry->col_id ; j++){
															tab_data_ptr_column_compare = tab_data_ptr_column_compare + first_col_entry[j].col_len + 1 ;
														}													
														data_length_compare = *tab_data_ptr_column_compare;											
														tab_data_ptr_column_compare +=1 ;
														if(data_length_compare > 0){
															total_non_null_rows++;													
														}
														selected_record_ptr = selected_record_ptr + header1.record_size ;  //moving the pointer to the next selected record
													}
												}
												strcpy(aggregate_col_name, "COUNT(");
												strcat(aggregate_col_name, aggregate_column_tok->tok_string);
												strcat(aggregate_col_name, ")");
												printf("\n%s\n",aggregate_col_name);
												if(aggregate_count_star_checked == true) {
													print_int_data(counter_rows_selected, aggregate_col_name, true);
												} else {
													print_int_data(total_non_null_rows, aggregate_col_name, true);
												}												
												break;

											default:
												total_non_null_rows = 0;
												rc = INVALID_SELECT_DEFINITION;
												cur->tok_value = INVALID;
												break;
											}
										}
										else{
											if (counter_rows_selected == 0){
												rc = RECORDS_NOT_FOUND ;
											}
											if(!rc){
												if (select_star == true) {
													cd_entry *first_cd_entry;
													if((first_cd_entry = get_cd_entry_from_tpd_entry(tab_entry))==NULL){

														rc = TABLE_FILE_CORRUPTION ;			// not able to get column entry from table
													}

													else{
														for(int i=0 ; i<(*tab_entry).num_columns ; i++){
															col_names[i] = first_cd_entry[i].col_name ;
															num_columns++;
														}
													}
												}
												if(!rc) {
													int select_col_id [MAX_NUM_COL] ;			//to store col ids of columns for which we have to select data
													for(int i =0 ; i<MAX_NUM_COL ; i++) {
														select_col_id[i] = -1 ;
													}

													//printf("Column names for select statement are: \n ");
													printf("\n");

													for(int i =0 ; i<num_columns ; i++) {
														cd_entry *col_details = get_column_from_table(col_names[i],tab_entry) ;
														if(col_details == NULL) {
															rc = COLUMN_NOT_EXIST;
															break;
														}
														else{
															//printf("%s", col_names[i]);
															select_col_id[i] = (*col_details).col_id ;
															print_string_data(col_names[i], *col_details);
														}
													}
													if(!rc) {
														printf("\n");

														//Here the actual select code goes after doing semantic analysis for select statement
														select_data_ptr1 = (char *)selected_data_ptr; // This global data pointer is populated by select_data method that applies the WHERE clause's condition
														cd_entry *col_entry = get_cd_entry_from_tpd_entry(tab_entry) ;
														for(int i = 0;i<counter_rows_selected;i++) {
															for(int j = 0; j<num_columns; j++)
															{  
																char *select_data_ptr = select_data_ptr1;

																for(int k=0 ; k<select_col_id[j] ; k++){
																	select_data_ptr = select_data_ptr + col_entry[k].col_len + 1;
																}

																char col_data[MAX_DATA_LENGTH];
																memset(col_data,'\0',MAX_DATA_LENGTH);

																char col_data_len_char = *select_data_ptr;   //col_data_len_char is a byte which stores the length of that col data
																select_data_ptr = select_data_ptr + 1; //move the pointer 1 byte ahead for reading actual data

																//printf("\ncol_data_len_char is %c\n", col_data_len_char);
																int col_data_length = col_data_len_char;

																//printf("\nread col data length is %d at column id %d",col_data_length,j);
																if(col_data_length == 0){
																	//column data is null

																	print_string_data("-", col_entry[select_col_id[j]]);


																	//printf("%s","NULL");

																}
																else{//it would be either char or int
																	int col_int_data ;
																	//	memcpy(col_data,select_data_ptr,col_data_length);			//storing the data in col_data array
																	switch(find_col_type(tab_entry,select_col_id[j])) {
																	case T_INT:
																		memcpy(&col_int_data,select_data_ptr,col_data_length);	
																		print_int_data(col_int_data, col_entry[select_col_id[j]].col_name, false);
																		break;
																	case T_CHAR:
																		memcpy(col_data,select_data_ptr,col_data_length);
																		print_string_data(col_data, col_entry[select_col_id[j]]);
																		//printf("%-10s", col_data);

																		break;

																	}


																}

															}
															select_data_ptr1 = select_data_ptr1 + header1.record_size  ;
															printf("\n");
														}

													}
												}
											}
										}
									}
								}
							}
						}
					}
				}

			}

		}
	}


	return rc;

}


cd_entry* get_column_from_table(char *column_name, tpd_entry *tab_entry)
{
	cd_entry *cd = NULL;
	cd_entry *cur = get_cd_entry_from_tpd_entry(tab_entry);
	if(cur == NULL){
		return NULL ;
	}
	bool found = false;
	int num_columns = (*tab_entry).num_columns ;

	if (num_columns > 0)
	{
		for(int i=0;i<num_columns;i++) {
			if (_stricmp(cur[i].col_name, column_name) == 0)
			{
				/* found it */
				cd = cur + i;
				break;
			}
		}
	}

	return cd;
}

void print_string_data(char *data, cd_entry col_entry) {
	int col_len = col_entry.col_len;
	int str_len = strlen(data);
	int col_name_len = strlen(col_entry.col_name);
	int empty_space = 0;
	if(col_entry.col_type == T_CHAR) {
		if(col_name_len > col_len) {
			empty_space = col_name_len - str_len;
			if(col_name_len < 4) {
				//Keep space to print NULL
				empty_space = empty_space + 4;
			}
		} else {
			empty_space = col_len - str_len;
			if(col_len < 4) {
				empty_space = empty_space + 4;
			}
		}
		printf("%s", data);

		for(int i = 0; i < empty_space ; i++) {
			printf(" ");
		}
	} else {
		// Its is int datatype, which means we want to print the column name
		if(col_name_len < 10) {
			empty_space = 10 - str_len;
		} else {
			if(str_len < 10) { // This is used when we want to print NULL for int column
				empty_space = col_name_len - str_len;
			}
		}

		for(int i = 0 ; i < empty_space;i++) {
			printf(" ");
		}
		printf("%s", data);
	}
	printf("| ");

}

void print_int_data(int data, char* col_name, bool aggr_function) {

	char str[10];
	_itoa(data, str, 10);
	int data_len = strlen(str);
	int empty_space = 10;
	int col_name_len = strlen(col_name);
	if(col_name_len > empty_space) {
		empty_space = col_name_len;
	}
	empty_space = empty_space - data_len;
	for(int i = 0 ; i < empty_space; i++) {
		printf(" ");
	}
	printf("%d", data);
	if(aggr_function == false) {
		printf("| ");
	}
}


// updates all rows if relational_operator is negative 
// cur_update is the token list pointer with which the data to be updated in the column
int update_column_data(tpd_entry *new_entry,cd_entry *column_entry,cd_entry *column_entry1,token_list *cur_update,token_list *cur, int bytes_read,int relational_operator){
	int rc = 0;
	char *tab_data_ptr_column_update = tab_data_ptr ;
	char *tab_record_ptr = tab_data_ptr ;
	cd_entry *first_col_entry = get_cd_entry_from_tpd_entry(new_entry);
	char *tab_data_ptr_column_compare = tab_data_ptr ;
	int counter_rows_updated = 0;


	for( int i=0; i< header1.num_records ; i++) {
		bool data_found = true ;
		tab_data_ptr_column_update = tab_record_ptr ;
		tab_data_ptr_column_compare = tab_record_ptr ;

		for(int j=0; j<(*column_entry).col_id ;j++) {
			tab_data_ptr_column_update = tab_data_ptr_column_update + first_col_entry[j].col_len +1 ;
		}
		if(relational_operator > 0){
			for(int j=0; j<(*column_entry1).col_id ;j++) {
				tab_data_ptr_column_compare = tab_data_ptr_column_compare + first_col_entry[j].col_len +1 ;
			}
		}
		int temp_int_data ;
		int len ;

		int null_value = 0;
		char col_data[MAX_DATA_LENGTH];
		memset(col_data,'\0',MAX_DATA_LENGTH);
		int condition_data = 0;
		int column_data_to_compare = 0 ;
		int compare_data_len = 0;
		int data_length_compare = 0 ;

		if(relational_operator > 0){
			switch(cur->tok_value) {
			case INT_LITERAL:
				data_length_compare = *tab_data_ptr_column_compare;
				tab_data_ptr_column_compare +=1 ;

				if(data_length_compare == 0){
					data_found = false;
				}
				else{

					memcpy(&column_data_to_compare,tab_data_ptr_column_compare,data_length_compare);
					tab_data_ptr_column_compare +=4 ;
					condition_data = atoi(cur->tok_string);

					switch(relational_operator){
					case S_EQUAL:
						if(column_data_to_compare != condition_data){
							data_found = false;
						}
						break;

					case S_GREATER:
						if(column_data_to_compare <= condition_data){
							data_found = false;
						}

						break;

					case S_LESS:
						if(column_data_to_compare >= condition_data){
							data_found = false;
						}
						break;

					default: 
						rc = UNSUPPORTED_RELATIONAL_OPERATOR;

						break;
					}
				}
				break;

			case STRING_LITERAL:
				compare_data_len = *tab_data_ptr_column_compare ;
				tab_data_ptr_column_compare +=1 ;

				if(compare_data_len == 0){
					data_found = false;
				}
				else{
					memcpy(col_data,tab_data_ptr_column_compare,compare_data_len);
					tab_data_ptr_column_compare +=compare_data_len ;
					//int condition_data = atoi(cur->tok_string);
					//	if(relational_operator > 0){
					switch(relational_operator){
					case S_EQUAL:
						if(strcmp(col_data,cur->tok_string) != 0){
							data_found = false;
						}
						break;

					case S_GREATER:
						if(strcmp(col_data,cur->tok_string) <= 0){
							data_found = false;
						}

						break;

					case S_LESS:
						if(strcmp(col_data,cur->tok_string) >= 0){
							data_found = false;
						}
						break;

					default: 
						rc = UNSUPPORTED_RELATIONAL_OPERATOR;

						break;
					}
				}
				break;
			}


		}
		if(!rc && data_found == true){
			counter_rows_updated++;
			switch(cur_update->tok_value) {
			case INT_LITERAL:
				temp_int_data = atoi(cur_update->tok_string);				
				len = 4;
				memcpy(tab_data_ptr_column_update, &len,1);    //write length of data before writing data
				tab_data_ptr_column_update = (char*)tab_data_ptr_column_update + 1 ; 
				memcpy(tab_data_ptr_column_update, &temp_int_data,4);		//writing actual int data after its length
				tab_data_ptr_column_update = (char*)tab_data_ptr_column_update + 4 ; 
				break;

			case STRING_LITERAL:
				len = strlen(cur_update->tok_string);
				memcpy(tab_data_ptr_column_update, &len,1);
				tab_data_ptr_column_update = (char*)tab_data_ptr_column_update + 1 ;
				memset(tab_data_ptr_column_update,'\0',(*column_entry).col_len);
				memcpy(tab_data_ptr_column_update, cur_update->tok_string,len);
				tab_data_ptr_column_update = (char*)tab_data_ptr_column_update + (*column_entry).col_len;
				break;

			case K_NULL:
				memcpy(tab_data_ptr_column_update,&null_value,1);
				tab_data_ptr_column_update = (char*)tab_data_ptr_column_update + (*column_entry).col_len + 1 ;
				break;

			}


		}

		tab_record_ptr = tab_record_ptr + header1.record_size ;		
	}
	if (counter_rows_updated == 0){
		rc = RECORDS_NOT_FOUND ;
	}
	else{
		printf("\nNumber of rows updated: %d\n",counter_rows_updated);
		FILE *fp1;
		char filename[MAX_IDENT_LEN+4];
		strcpy(filename,(*new_entry).table_name);
		strcat(filename,".tab");
		//printf("filename of the file which has to be updated by update statement is: %s " , filename);

		fp1=fopen(filename, "w");
		if(fp1 == NULL){
			rc = FILE_OPEN_ERROR;
		}
		else {//writing header and records in the file
			//printf("Header fwrite data: %d", fwrite(&header1,1,sizeof(table_file_header),fp1)); 
			//printf("Records fwrite data: %d", fwrite(tab_data_ptr,1,bytes_read,fp1)); 
			fwrite(&header1,1,sizeof(table_file_header),fp1);
			fwrite(tab_data_ptr,1,bytes_read,fp1);
			printf("\nTable updated successfully\n");
			fclose(fp1);
		}
	}

	return rc;

}

int delete_record_data(tpd_entry *new_entry,cd_entry *column_entry1,token_list *cur,int bytes_read,int relational_operator){
	int rc = 0;
	char *tab_data_ptr_record_delete = tab_data_ptr ;
	char *tab_record_ptr = tab_data_ptr ;
	cd_entry *first_col_entry = get_cd_entry_from_tpd_entry(new_entry);
	char *tab_data_ptr_column_compare = tab_data_ptr ;
	int counter_rows_deleted = 0;


	for( int i=0; i< header1.num_records ; i++) {
		bool data_found = true ;
		tab_data_ptr_record_delete = tab_record_ptr ;
		tab_data_ptr_column_compare = tab_record_ptr ;

		if(relational_operator > 0){
			for(int j=0; j<(*column_entry1).col_id ;j++) {
				tab_data_ptr_column_compare = tab_data_ptr_column_compare + first_col_entry[j].col_len +1 ;
			}
		}
		int temp_int_data ;
		int len ;

		int null_value = 0;
		char col_data[MAX_DATA_LENGTH];
		memset(col_data,'\0',MAX_DATA_LENGTH);
		int condition_data = 0;
		int column_data_to_compare = 0 ;
		int compare_data_len = 0;
		int data_length_compare = 0 ;

		if(relational_operator > 0){
			switch(cur->tok_value) {
			case INT_LITERAL:
				data_length_compare = *tab_data_ptr_column_compare;
				tab_data_ptr_column_compare +=1 ;

				if(data_length_compare == 0){
					data_found = false;
				}
				else{

					memcpy(&column_data_to_compare,tab_data_ptr_column_compare,data_length_compare);
					tab_data_ptr_column_compare +=data_length_compare ;
					condition_data = atoi(cur->tok_string);

					switch(relational_operator){
					case S_EQUAL:
						if(column_data_to_compare != condition_data){
							data_found = false;
						}
						break;

					case S_GREATER:
						if(column_data_to_compare <= condition_data){
							data_found = false;
						}

						break;

					case S_LESS:
						if(column_data_to_compare >= condition_data){
							data_found = false;
						}
						break;

					default: 
						rc = UNSUPPORTED_RELATIONAL_OPERATOR;

						break;
					}
				}
				break;

			case STRING_LITERAL:
				compare_data_len = *tab_data_ptr_column_compare ;
				tab_data_ptr_column_compare +=1 ;

				if(compare_data_len == 0){
					data_found = false;
				}
				else{
					memcpy(col_data,tab_data_ptr_column_compare,compare_data_len);
					tab_data_ptr_column_compare +=compare_data_len ;
					//int condition_data = atoi(cur->tok_string);
					//	if(relational_operator > 0){
					switch(relational_operator){
					case S_EQUAL:
						if(strcmp(col_data,cur->tok_string) != 0){
							data_found = false;
						}
						break;

					case S_GREATER:
						if(strcmp(col_data,cur->tok_string) <= 0){
							data_found = false;
						}

						break;

					case S_LESS:
						if(strcmp(col_data,cur->tok_string) >= 0){
							data_found = false;
						}
						break;

					default: 
						rc = UNSUPPORTED_RELATIONAL_OPERATOR;

						break;
					}
				}
				break;
			}


		}
		if(!rc && data_found == true){
			counter_rows_deleted++;
			memset(tab_record_ptr,'\0',header1.record_size);
			if(i == (header1.num_records-1)){
				break;			// this is the last record
			}
			else{
				char *last_record = tab_data_ptr + (header1.num_records - 1) * header1.record_size ;
				memcpy(tab_record_ptr,last_record,header1.record_size);
				memset(last_record,'\0',header1.record_size);
				header1.num_records-- ;
				header1.file_size = header1.file_size - header1.record_size;
				i--;
			}


		}

		tab_record_ptr = tab_record_ptr + header1.record_size ;		
	}
	if (counter_rows_deleted == 0){
		rc = RECORDS_NOT_FOUND ;
	}
	else{
		printf("\nNumber of rows deleted: %d\n",counter_rows_deleted);
		FILE *fp1;
		char filename[MAX_IDENT_LEN+4];
		strcpy(filename,(*new_entry).table_name);
		strcat(filename,".tab");

		fp1=fopen(filename, "w");
		if(fp1 == NULL){
			rc = FILE_OPEN_ERROR;
		}
		else {//writing header and records in the file
			fwrite(&header1,1,sizeof(table_file_header),fp1);
			fwrite(tab_data_ptr,1,bytes_read,fp1);
			printf("\nRecords deleted successfully\n");
			fclose(fp1);
		}
	}

	return rc;


}

int select_data(char *selected_data_ptr,tpd_entry *new_entry,cd_entry *where_column_entry1,cd_entry *where_column_entry2,token_list *where_data1,token_list *where_data2, int bytes_read,int relational_operator,int relational_operator1, int and_or_operator, int *counter_rows_selected_ptr){
	int rc = 0;
	char *temp_selected_data_ptr = selected_data_ptr ;
	char *tab_record_ptr = tab_data_ptr ;
	cd_entry *first_col_entry = get_cd_entry_from_tpd_entry(new_entry);
	char *tab_data_ptr_column_compare = tab_data_ptr ;
	char *tab_data_ptr_column_compare1 = tab_data_ptr ;
	int counter_rows_selected = 0;


	for( int i=0; i< header1.num_records ; i++) {
		bool data_found = true ;
		bool data_found1 = true;

		tab_data_ptr_column_compare = tab_record_ptr ;
		tab_data_ptr_column_compare1 = tab_record_ptr ;

		if(relational_operator > 0){
			for(int j=0; j<(*where_column_entry1).col_id ;j++) {
				tab_data_ptr_column_compare = tab_data_ptr_column_compare + first_col_entry[j].col_len +1 ;
			}
		}
		int temp_int_data ;
		int len ;

		int null_value = 0;
		char col_data[MAX_DATA_LENGTH];
		memset(col_data,'\0',MAX_DATA_LENGTH);
		char col_data1[MAX_DATA_LENGTH];
		memset(col_data1,'\0',MAX_DATA_LENGTH);
		int condition_data = 0;
		int column_data_to_compare = 0 ;
		int compare_data_len = 0;
		int data_length_compare = 0 ;

		if(relational_operator > 0){
			if(relational_operator == K_NULL || relational_operator == K_NOT){
				data_length_compare = *tab_data_ptr_column_compare;
				switch(relational_operator){
				case K_NULL:
					if(data_length_compare != 0){
						data_found = false;
					}
					break;

				case K_NOT:
					if(data_length_compare <= 0 ){
						data_found = false;
					}
					break;
				}

			}
			else{
				switch(where_data1->tok_value) {
				case INT_LITERAL:
					data_length_compare = *tab_data_ptr_column_compare;
					tab_data_ptr_column_compare +=1 ;

					if(data_length_compare > 0){
						memcpy(&column_data_to_compare,tab_data_ptr_column_compare,data_length_compare);
						tab_data_ptr_column_compare +=4 ;
						condition_data = atoi(where_data1->tok_string);
					}

					switch(relational_operator){
					case S_EQUAL:
						if(data_length_compare <= 0){
							data_found = false;
						}
						else if(column_data_to_compare != condition_data){
							data_found = false;
						}
						break;

					case S_GREATER:
						if(data_length_compare <= 0){
							data_found = false;
						}
						else if(column_data_to_compare <= condition_data){
							data_found = false;
						}

						break;

					case S_LESS:
						if(data_length_compare <= 0){
							data_found = false;
						}
						else if(column_data_to_compare >= condition_data){
							data_found = false;
						}
						break;

					default: 
						rc = UNSUPPORTED_RELATIONAL_OPERATOR;

						break;
					}

					break;

				case STRING_LITERAL:
					compare_data_len = *tab_data_ptr_column_compare ;
					tab_data_ptr_column_compare +=1 ;

					if(compare_data_len > 0){
						memcpy(col_data,tab_data_ptr_column_compare,compare_data_len);
						tab_data_ptr_column_compare +=compare_data_len ;
					}
					switch(relational_operator){
					case S_EQUAL:
						if(compare_data_len <= 0){
							data_found = false;
						}
						else if(strcmp(col_data,where_data1->tok_string) != 0){
							data_found = false;
						}
						break;

					case S_GREATER:
						if(compare_data_len <= 0){
							data_found = false;
						}
						else if(strcmp(col_data,where_data1->tok_string) <= 0){
							data_found = false;
						}

						break;

					case S_LESS:
						if(compare_data_len <= 0){
							data_found = false;
						}
						else if(strcmp(col_data,where_data1->tok_string) >= 0){
							data_found = false;
						}
						break;

					default: 
						rc = UNSUPPORTED_RELATIONAL_OPERATOR;

						break;
					}

					break;
				}

			}
		}

		// for AND OR column
		if(relational_operator1 > 0){

			for(int j=0; j<(*where_column_entry2).col_id ;j++) {
				tab_data_ptr_column_compare1 = tab_data_ptr_column_compare1 + first_col_entry[j].col_len +1 ;
			}

			bool check_second_column = false;
			if(and_or_operator == K_AND) {
				if(data_found == true) {
					check_second_column = true;
				}
			} else {
				if(data_found == false) {
					check_second_column = true;
				}
			}
			if(check_second_column == true) {
				if(relational_operator1 == K_NULL || relational_operator1 == K_NOT){
					data_length_compare = *tab_data_ptr_column_compare1;
					switch(relational_operator1){
					case K_NULL:
						if(data_length_compare != 0){
							data_found1 = false;
						}
						break;

					case K_NOT:
						if(data_length_compare <= 0 ){
							data_found1 = false;
						}
						break;
					}

				}
				else{
					switch(where_data2->tok_value) {
					case INT_LITERAL:
						data_length_compare = *tab_data_ptr_column_compare1;
						tab_data_ptr_column_compare1 +=1 ;

						if(data_length_compare > 0){
							memcpy(&column_data_to_compare,tab_data_ptr_column_compare1,data_length_compare);
							tab_data_ptr_column_compare1 +=4 ;
							condition_data = atoi(where_data2->tok_string);
						}

						switch(relational_operator1){
						case S_EQUAL:
							if(data_length_compare <= 0){
								data_found1 = false;
							}
							else if(column_data_to_compare != condition_data){
								data_found1 = false;
							}
							break;

						case S_GREATER:
							if(data_length_compare <= 0){
								data_found1 = false;
							}
							else if(column_data_to_compare <= condition_data){
								data_found1 = false;
							}

							break;

						case S_LESS:
							if(data_length_compare <= 0){
								data_found1 = false;
							}
							else if(column_data_to_compare >= condition_data){
								data_found1 = false;
							}
							break;

						default: 
							rc = UNSUPPORTED_RELATIONAL_OPERATOR;

							break;
						}

						break;

					case STRING_LITERAL:
						compare_data_len = *tab_data_ptr_column_compare1 ;
						tab_data_ptr_column_compare1 +=1 ;

						if(compare_data_len > 0){
							memcpy(col_data1,tab_data_ptr_column_compare1,compare_data_len);
							tab_data_ptr_column_compare1 +=compare_data_len ;
						}
						switch(relational_operator1){
						case S_EQUAL:
							if(compare_data_len <= 0){
								data_found1 = false;
							}
							else if(strcmp(col_data1,where_data2->tok_string) != 0){
								data_found1 = false;
							}
							break;

						case S_GREATER:
							if(compare_data_len <= 0){
								data_found1 = false;
							}
							else if(strcmp(col_data1,where_data2->tok_string) <= 0){
								data_found1 = false;
							}

							break;

						case S_LESS:
							if(compare_data_len <= 0){
								data_found1 = false;
							}
							else if(strcmp(col_data1,where_data2->tok_string) >= 0){
								data_found1 = false;
							}
							break;

						default: 
							rc = UNSUPPORTED_RELATIONAL_OPERATOR;

							break;
						}

						break;
					}

				}
			}
		}// end of AND OR column operation

		bool print_data = true;
		if(and_or_operator > 0) {
			if(and_or_operator == K_AND) {
				print_data = data_found && data_found1;
			} else {
				print_data = data_found || data_found1;
			}
		} else {
			print_data = data_found;
		}
		if(!rc && print_data == true){
			counter_rows_selected++;
			memcpy(temp_selected_data_ptr,tab_record_ptr,header1.record_size);
			temp_selected_data_ptr = temp_selected_data_ptr + header1.record_size ;
		}

		tab_record_ptr = tab_record_ptr + header1.record_size ;		
	}

	*counter_rows_selected_ptr = counter_rows_selected; // Return turn the number of rows selected by this method.
	return rc;

}

void sort_data(char * selected_data,cd_entry *column_to_sort,tpd_entry *tab_entry,int num_of_records,bool desc){
	char* swap_record_location = (char*) malloc(header1.record_size);
	memset(swap_record_location, '\0', header1.record_size);

	for (int c = 0 ; c < num_of_records - 1; c++)	{
		for (int d = 0 ; d < num_of_records - c - 1; d++)		{
			char *record_ptr1 = NULL, *record_ptr2 = NULL;
			record_ptr1 = selected_data + d*header1.record_size;
			record_ptr2 = selected_data + (d+1)*header1.record_size;
			if(desc == false) {
				if (compare_columns(record_ptr1, record_ptr2, column_to_sort, tab_entry) > 0) 
				{
					// Ascending order
					memcpy(swap_record_location, record_ptr1, header1.record_size);
					memcpy(record_ptr1, record_ptr2, header1.record_size);
					memcpy(record_ptr2, swap_record_location, header1.record_size);
					memset(swap_record_location, '\0', header1.record_size);
				}
			} else {
				// Dscending order
				if (compare_columns(record_ptr1, record_ptr2, column_to_sort, tab_entry) < 0) 
				{
					// Ascending order
					memcpy(swap_record_location, record_ptr1, header1.record_size);
					memcpy(record_ptr1, record_ptr2, header1.record_size);
					memcpy(record_ptr2, swap_record_location, header1.record_size);
					memset(swap_record_location, '\0', header1.record_size);
				}
			}
		}
	}
}


// returns negative if col data of record 1 is less than col data of record 2, 0 when both are equal, else positive
int compare_columns(char *record_ptr1,char * record_ptr2,cd_entry *col_to_compare,tpd_entry *tab_entry){
	char *temp_record_ptr1 = record_ptr1 ;
	char *temp_record_ptr2 = record_ptr2 ;
	int col_data_to_compare1 = 0;
	int col_data_to_compare2 = 0;
	cd_entry *first_col_entry = get_cd_entry_from_tpd_entry(tab_entry) ;
	for(int i=0 ; i<col_to_compare->col_id ; i++){
		temp_record_ptr1 = temp_record_ptr1 + first_col_entry[i].col_len + 1;
		temp_record_ptr2 = temp_record_ptr2 + first_col_entry[i].col_len + 1;
	}

	if(col_to_compare->col_type == T_INT){
		int data_length_compare1 = *temp_record_ptr1;
		temp_record_ptr1 +=1 ;

		if(data_length_compare1 == 0) {
			return -1;
		}
		if(data_length_compare1 > 0){
			memcpy(&col_data_to_compare1,temp_record_ptr1,data_length_compare1);
		}

		//reading the col data of next record
		int data_length_compare2 = *temp_record_ptr2;
		temp_record_ptr2 +=1 ;

		if(data_length_compare2 == 0) {
			return 1;
		}
		if(data_length_compare2 > 0){
			memcpy(&col_data_to_compare2,temp_record_ptr2,data_length_compare2);
		}
		return col_data_to_compare1 - col_data_to_compare2;
	}
	else{
		// it is string column
		char col_data1[MAX_DATA_LENGTH];
		memset(col_data1, '\0', MAX_DATA_LENGTH);
		char col_data2[MAX_DATA_LENGTH];
		memset(col_data2, '\0', MAX_DATA_LENGTH);

		int compare_data_len1 = *temp_record_ptr1;
		temp_record_ptr1 +=1 ;

		if(compare_data_len1 == 0){
			return -1;
		}
		if(compare_data_len1 > 0){
			memcpy(col_data1,temp_record_ptr1,compare_data_len1);
		}

		int compare_data_len2 = *temp_record_ptr2;
		temp_record_ptr2 +=1 ;

		if(compare_data_len2 == 0){
			return 1;
		}
		if(compare_data_len2 > 0){
			memcpy(col_data2,temp_record_ptr2,compare_data_len2);
		}
		return strcmp(col_data1, col_data2);
	}

}


char* get_timestamp(){
	struct tm *newtime;
	time_t ltime;
	char *time_str = NULL;
	time_str = (char *) malloc(15*sizeof(char));

	/* Get the time in seconds */
	time(&ltime);
	/* Convert it to the structure tm */
	newtime = localtime(&ltime);

	/* Print the local time as a string */
	sprintf(time_str, "%04d%02d%02d%02d%02d%02d", (newtime->tm_year+1900),(newtime->tm_mon+1),newtime->tm_mday,newtime->tm_hour,newtime->tm_min,newtime->tm_sec);
	time_str[14] = '\0' ;

	return (char *)time_str;

}

int sem_backup_to(token_list *t_list){
	int rc = 0;
	token_list *cur = t_list;
	if((cur->tok_class != keyword) &&
		(cur->tok_class != identifier) &&
		(cur->tok_class != type_name))
	{
		// Error
		rc = INVALID_BACKUP_DEFINITION;
		cur->tok_value = INVALID;
	} else {
		char* image_file_name = cur->tok_string;
		char image_file_with_extension[MAX_IDENT_LEN+5];
		memset(image_file_with_extension, '\0', MAX_IDENT_LEN+5);
		strcpy(image_file_with_extension, image_file_name);
		strcat(image_file_with_extension,".bak");
		FILE *image_file__read_ptr = fopen(image_file_with_extension, "r+");
		if(image_file__read_ptr != NULL) {
			rc = BACKUP_FILE_ALREADY_EXISTS;
		} else {
			FILE *image_file_ptr = fopen(image_file_with_extension, "w+");
			if(image_file_ptr == NULL) {
				rc = FILE_OPEN_ERROR;
			} else {
				// Copy dbfile.bin to backup image file
				FILE* dbfile_bin_ptr = fopen("dbfile.bin", "r+");
				if(dbfile_bin_ptr == NULL) {
					rc = FILE_OPEN_ERROR;
				} else {
					char ch;
					int db_file_size = g_tpd_list->list_size;
					char* db_file_memory_ptr = (char*)malloc(db_file_size);
					memset(db_file_memory_ptr,'\0', db_file_size);
					fread(db_file_memory_ptr, 1, db_file_size, dbfile_bin_ptr);
					fwrite(db_file_memory_ptr,1,db_file_size, image_file_ptr);
					fclose(dbfile_bin_ptr);
					printf("Backed up DBfile\n");
					struct stat db_stat;
					stat("dbfile.bin", &db_stat);
					printf("db file size in backup = %d", db_stat.st_size);

					int num_tables = g_tpd_list->num_tables;
					tpd_entry *cur_tab = &(g_tpd_list->tpd_start);

					if (num_tables > 0)
					{
						while (num_tables-- > 0)
						{
							printf("Backing up table: %s\n", cur_tab->table_name);
							int bytes_read = read_table_file(cur_tab);
							int tab_file_size = header1.file_size;
							printf("File size: %d and bytes read is: %d\n", tab_file_size, bytes_read);
							fwrite(&tab_file_size, 1, 4, image_file_ptr);

							char tab_file_with_extension[MAX_IDENT_LEN+5];
							memset(tab_file_with_extension, '\0', MAX_IDENT_LEN+5);
							strcpy(tab_file_with_extension, cur_tab->table_name);
							strcat(tab_file_with_extension,".tab");

							FILE* tab_file_ptr = fopen(tab_file_with_extension, "r+");
							if(tab_file_ptr == NULL) {
								rc = FILE_OPEN_ERROR;
								break;
							}
							int count = 0;
							while((fread(&ch,1,1,tab_file_ptr) > 0) && count < tab_file_size) {
								count++;
								fputc(ch, image_file_ptr);
							}
							printf("copied: %d bytes\n", count);
							fclose(tab_file_ptr);
							printf("Backed up table: %s\n", cur_tab->table_name);
							if (num_tables > 0){
								cur_tab = (tpd_entry*)((char*)cur_tab + cur_tab->tpd_size);
							}
						}
					}
					fclose(image_file_ptr);
					if(!rc) {
						fwrite("BACKUP ",1,7,log_file_ptr);
						fwrite(image_file_name,1,strlen(image_file_name),log_file_ptr);
						fwrite("\n",1,1,log_file_ptr);
					}
				}

			}
		}
	}
	return rc;
}

int sem_restore_from(token_list *t_list){
	int rc = 0;
	token_list *cur = t_list;
	bool rf_flag = true;
	bool backup_found = false;
	char current_char;
	if((cur->tok_class != keyword) &&
		(cur->tok_class != identifier) &&
		(cur->tok_class != type_name))
	{
		// Error
		rc = INVALID_RESTORE_DEFINITION;
		cur->tok_value = INVALID;
	} else {
		char* image_file_name = cur->tok_string;
		if(cur->next->tok_value != EOC && cur->next->tok_value != K_WITHOUT){
			rc = INVALID_RESTORE_DEFINITION;
			cur->tok_value = INVALID;
		}
		else{
			// EOC or without
			cur = cur->next;
			if(cur->tok_value == K_WITHOUT){
				cur = cur->next;
				if(cur->tok_value != K_RF){
					rc = INVALID_RESTORE_DEFINITION;
					cur->tok_value = INVALID;
				}
				else{
					rf_flag = false;
					cur = cur->next;
				}
			}
			if(!rc){
				if(cur->tok_value != EOC){
					rc = INVALID_RESTORE_DEFINITION;
					cur->tok_value = INVALID;
				}
				else{
					char image_file_with_extension[MAX_IDENT_LEN+5];
					memset(image_file_with_extension, '\0', MAX_IDENT_LEN+5);
					strcpy(image_file_with_extension, image_file_name);
					strcat(image_file_with_extension,".bak");
					FILE *image_file_ptr = fopen(image_file_with_extension, "a+");
					if(image_file_ptr == NULL) {
						rc = FILE_OPEN_ERROR;
					} else {
						FILE *restore_log_ptr = NULL;
						if((restore_log_ptr = fopen("db.log","a+")) == NULL){
							rc= FILE_OPEN_ERROR;	
							printf("unable to open log file for restore\n");
						} else{
							struct stat log_file_stat;
							stat("db.log", &log_file_stat);
							int log_file_size = log_file_stat.st_size;
							log_file_size = log_file_size + 9; //Give space to add RF_START in log file
							char* log_file_data_ptr = (char*) malloc(log_file_size);
							memset(log_file_data_ptr, '\0', log_file_size);
							int log_file_char_count = 0;

							while((current_char = fgetc(restore_log_ptr)) != EOF && (backup_found == false)) {
								log_file_data_ptr[log_file_char_count++] = current_char;
								if(current_char != 'B') {
									// Current line does not contain backup keyword...jump to next line
									while((current_char = fgetc(restore_log_ptr)) != EOF) {
										log_file_data_ptr[log_file_char_count++] = current_char;
										if(current_char == '\n') {
											break;
										}
									}
									if(current_char == EOF) {
										log_file_data_ptr[log_file_char_count++] = current_char;
										break;
									}
								} else {
									while((current_char = fgetc(restore_log_ptr)) != ' ') {
										// reached ' ' before the image file name
										log_file_data_ptr[log_file_char_count++] = current_char;
									}
									log_file_data_ptr[log_file_char_count++] = current_char;
									char current_backup_filename[MAX_IDENT_LEN + 1];
									memset(current_backup_filename, '\0', MAX_IDENT_LEN + 1);
									int char_count = 0;
									while((current_char = fgetc(restore_log_ptr)) != EOF) {
										log_file_data_ptr[log_file_char_count++] = current_char;
										if(current_char == '\n') {
											break;
										}
										// reached ' ' before the image file name
										current_backup_filename[char_count] = current_char;
										char_count++;
									}
									current_backup_filename[char_count] = '\0';
									if(stricmp(current_backup_filename, image_file_name) == 0) {
										printf("Found the desired backup file entry\n");
										backup_found = true;
									}
								}
							} // Now log_file_data_ptr and fseek position of file are at data till BACKUP <desired_Image_file_name>
							if(backup_found == false) {
								rc = BACKUP_IMAGE_NOT_FOUND;
							} else {
								if(!rc){
									rc = restore_from_backup_file(image_file_name);
									if(!rc) {
										if(rf_flag == true) {
											// with RF
											g_tpd_list->db_flags = ROLLFORWARD_PENDING;
											FILE* update_db_flags_ptr = fopen("dbfile.bin", "wbc");
											if(update_db_flags_ptr == NULL) {
												rc = FILE_OPEN_ERROR;
											} else {
												fwrite(g_tpd_list,g_tpd_list->list_size, 1, update_db_flags_ptr); 
												fclose(update_db_flags_ptr);

												printf("Set the db flag to ROLLFORWARD_PENDING\n");
												// insert RF_START log
												strcat(log_file_data_ptr, "RF_START\n");
												log_file_char_count = log_file_char_count + 9;
												log_file_data_ptr[log_file_char_count++] = current_char;
												while((current_char = fgetc(restore_log_ptr)) != EOF) {
													log_file_data_ptr[log_file_char_count++] = current_char;
												}
											}
										}
										if(rf_flag == false) {
											backup_log_file();
										}
										// if rf_flag is true, ti would write the concatenated data, and if rf_flag is false, it would prune the log file.
										fclose(restore_log_ptr);
										restore_log_ptr = fopen("db.log", "w+");
										fwrite(log_file_data_ptr,1,log_file_char_count, restore_log_ptr);
									}
								}
							}
							fclose(restore_log_ptr);
						}
						fclose(image_file_ptr);
					}
				}
			}
		}
	}
	return rc;
}

int sem_rollforward(token_list *t_list){
	token_list *cur = t_list;
	int rc = 0;
	char entered_timestamp_value[MAX_IDENT_LEN];
	memset(entered_timestamp_value,'\0',MAX_IDENT_LEN);
	if(cur->tok_value != EOC && cur->tok_value != K_TO) {
		rc = INVALID_ROLLFORWARD_DEFINITION;
		cur->tok_value = INVALID;
	} else {
		bool stop_on_timestamp = false;
		if(cur->tok_value == K_TO) {
			// with timestamp specified
			cur = cur->next;
			if(cur->tok_class != constant) {
				rc = INVALID_ROLLFORWARD_DEFINITION;
				cur->tok_value = INVALID;
			} else if(cur->next->tok_value != EOC){
				rc = INVALID_ROLLFORWARD_DEFINITION;
				cur->tok_value = INVALID;
			} else {
				memcpy(entered_timestamp_value, cur->tok_string, strlen(cur->tok_string));
				stop_on_timestamp = true;
				cur = cur->next;
				rc = parse_entered_timestamp(entered_timestamp_value);
			}
		}
		printf("RC after parsing is: %d\n", rc);
		if(!rc) {
			if(cur->tok_value != EOC) {
				rc = INVALID_ROLLFORWARD_DEFINITION;
				cur->tok_value = INVALID;
			} else {
				FILE *log_file_ptr_rollforward = fopen("db.log", "r+");
				if(log_file_ptr_rollforward == NULL) {
					rc = FILE_OPEN_ERROR;
				} else {

					struct stat log_file_stat;
					stat("db.log", &log_file_stat);
					int log_file_size = log_file_stat.st_size;

					char* log_file_memory_ptr = (char*) malloc(log_file_size);
					memset(log_file_memory_ptr, '\0', log_file_size);
					int log_file_char_count = 0;
					char current_char;
					bool rf_start_found = false;
					int rollforward_start_position = 0;
					while((rf_start_found == false) && fread(&current_char,1,1,log_file_ptr_rollforward) > 0 ){
						if(current_char != 'R') {
							log_file_memory_ptr[log_file_char_count++] = current_char;
							// Current line does not contain backup keyword...jump to next line
							while(fread(&current_char,1,1,log_file_ptr_rollforward) > 0){
								log_file_memory_ptr[log_file_char_count++] = current_char;
								if(current_char == '\n') {
									break;
								}
							}
							if(current_char == EOF) {
								log_file_memory_ptr[log_file_char_count++] = current_char;
								break;
							}
						} else {
							rf_start_found = true;
							while(fread(&current_char,1,1,log_file_ptr_rollforward) > 0){
								if(current_char == '\n') {
									break;
								}
							} // Now we are the line after "RF_START" in log file
						}
					}
					if(rf_start_found == false) {
						// RF_START_NOT found
						rc = RF_START_NOT_FOUND;
					} else {
						rollforward_start_position = log_file_char_count;

						char logged_timestamp_value[15];
						bool timestamp_value_found = false;
						while((timestamp_value_found == false) && fread(&current_char,1,1,log_file_ptr_rollforward) > 0){
							int current_timestamp_char = 0;
							memset(logged_timestamp_value,'\0',15);
							log_file_memory_ptr[log_file_char_count++] = current_char;
							logged_timestamp_value[current_timestamp_char++] = current_char;

							while(fread(&current_char,1,1,log_file_ptr_rollforward) > 0){
								if(current_char != ' ') {
									log_file_memory_ptr[log_file_char_count++] = current_char;
									logged_timestamp_value[current_timestamp_char++] = current_char;
								} else {
									break;
								}
							}
							log_file_memory_ptr[log_file_char_count++] = current_char;
							current_timestamp_char++;
							while(fread(&current_char,1,1,log_file_ptr_rollforward) > 0){
								log_file_memory_ptr[log_file_char_count++] = current_char;
								current_timestamp_char++;
								if(current_char == '\n') {
									break;
								}
							}
							if(stop_on_timestamp == true) {
								int timestamp_compare = stricmp(logged_timestamp_value, entered_timestamp_value);
								if( timestamp_compare > 0 ){
									timestamp_value_found = true;
									char* greater_timestamp_memory_ptr = log_file_memory_ptr + log_file_char_count - current_timestamp_char;
									log_file_char_count = log_file_char_count - current_timestamp_char;
									memset(greater_timestamp_memory_ptr,'\0',current_timestamp_char);
								} else if(timestamp_compare == 0) {
									timestamp_value_found = true;
								}
							}
						}
						fclose(log_file_ptr_rollforward);
						if(timestamp_value_found == false) {
							if(stop_on_timestamp == true) {
								rc = INVALID_TIMESTAMP_VALUE;
							}
						}

						if(!rc) {
							if(stop_on_timestamp == true){
								backup_log_file();
							}

							char *rollforward_command_ptr = log_file_memory_ptr + rollforward_start_position;
							int rollforward_char_count = rollforward_start_position;
							g_tpd_list->db_flags = 0;
							while(rollforward_char_count < log_file_char_count && !rc) {
								rollforward_command_ptr = rollforward_command_ptr + 16; // Jump to start of command skipping timestamp value and space
								rollforward_char_count = rollforward_char_count + 16;

								char* current_command = (char*)malloc(500);
								memset(current_command,'\0', 500);
								int current_command_char_count = 0;
								if(rollforward_char_count > log_file_char_count) {
									break;
								}
								while(rollforward_char_count <= log_file_char_count && (current_char = *rollforward_command_ptr) != '"') {
									rollforward_command_ptr++;
									rollforward_char_count++;
									current_command[current_command_char_count++] = current_char;
								}
								rollforward_command_ptr = rollforward_command_ptr + 2;
								rollforward_char_count = rollforward_char_count + 2;
								token_list *tok_list=NULL, *tok_ptr = NULL;
								printf("Running command: %s\n",current_command);
								if(current_char == '"') {
									rc = get_token(current_command, &tok_list);
									if (!rc)
									{
										rc = do_semantic(tok_list);
									}
								}
							}
							if(!rc) {

								FILE* update_db_flags_ptr = fopen("dbfile.bin", "wbc");
								if(update_db_flags_ptr == NULL) {
									rc = FILE_OPEN_ERROR;
								} else {
									fwrite(g_tpd_list,g_tpd_list->list_size, 1, update_db_flags_ptr); 
									fclose(update_db_flags_ptr);
									printf("Updated the db_flags to 0 to remove ROLLFORWARD_PENDING state lock\n");
								}
								if(!rc) {
									fclose(log_file_ptr);
									FILE* original_log_file_ptr = fopen("db.log", "w");
									if(original_log_file_ptr == NULL) {
										rc = FILE_OPEN_ERROR;
									} else {
										fwrite(log_file_memory_ptr, 1, log_file_char_count, original_log_file_ptr);
										fwrite("\n",1,1,original_log_file_ptr);
										fclose(original_log_file_ptr);
										log_file_ptr = fopen("db.log", "a+");
										printf("Removed RF_START from the log file and pruned it if required\n");
									}
								}
							}
						}
					}

				}
			}
		}
	}
	return rc;
}

int restore_from_backup_file(char* backup_file_name) {
	int rc = 0;
	char backup_filename_with_extension[MAX_IDENT_LEN + 5];
	memset(backup_filename_with_extension, '\0', MAX_IDENT_LEN + 5);
	strcpy(backup_filename_with_extension, backup_file_name);
	strcat(backup_filename_with_extension, ".bak");

	FILE *backup_file_ptr = fopen(backup_filename_with_extension, "r+");
	if(backup_file_ptr == NULL) {
		rc = FILE_OPEN_ERROR;
	} else {
		int count = 0;
		FILE* db_file_ptr = fopen("dbfile.bin", "wb+");
		if(db_file_ptr == NULL) {
			rc = FILE_OPEN_ERROR;
		} else {
			int db_file_size = 0;
			fread(&db_file_size, 1, 4, backup_file_ptr);
			printf("Db file size to be restored: %d", db_file_size);

			fseek(backup_file_ptr, 0 ,SEEK_SET);

			char* db_file_memory_ptr = (char*)malloc(db_file_size);
			memset(db_file_memory_ptr,'\0', db_file_size);
			fread(db_file_memory_ptr, 1, db_file_size, backup_file_ptr);
			int written = fwrite(db_file_memory_ptr,1, db_file_size, db_file_ptr);
			printf("Written to db file = %d bytes\n",written);
			//free(db_file_memory_ptr);
			fclose(db_file_ptr);
			struct stat db_stat;
			stat("dbfile.bin", &db_stat);
			printf("db file size after restore = %d\n", db_stat.st_size);

			printf("recovered the dbfile\n");

			rc = initialize_tpd_list(); // Reintialize the global tpd_list with restored dbfile
			if(!rc) {

				int num_tables = g_tpd_list->num_tables;
				tpd_entry *cur_tab = &(g_tpd_list->tpd_start);

				if (num_tables > 0)
				{
					while (num_tables-- > 0)
					{
						printf("Restoring table: %s\n", cur_tab->table_name);
						int tab_file_size = 0;
						fread(&tab_file_size,1,4,backup_file_ptr);
						printf("Tab File size: %d\n", tab_file_size);

						char* tab_file_data_ptr = (char*)malloc(tab_file_size);
						memset(tab_file_data_ptr,'\0', tab_file_size);
						fread(tab_file_data_ptr, 1, tab_file_size, backup_file_ptr);

						char tab_file_with_extension[MAX_IDENT_LEN+5];
						memset(tab_file_with_extension, '\0', MAX_IDENT_LEN+5);
						strcpy(tab_file_with_extension, cur_tab->table_name);
						strcat(tab_file_with_extension,".tab");

						FILE* tab_file_ptr = fopen(tab_file_with_extension, "w+");
						if(tab_file_ptr == NULL) {
							rc = FILE_OPEN_ERROR;
							break;
						}
						fwrite(tab_file_data_ptr, 1, tab_file_size, tab_file_ptr);
						free(tab_file_data_ptr);
						fclose(tab_file_ptr);
						printf("Restored table: %s\n", cur_tab->table_name);

						if (num_tables > 0){
							cur_tab = (tpd_entry*)((char*)cur_tab + cur_tab->tpd_size);
						}
					}
				}
			}
		}
		fclose(backup_file_ptr);
	}
	return rc;
}

int backup_log_file() {
	int rc = 0;
	FILE* original_log_file_ptr, *backup_log_file_ptr;
	if((original_log_file_ptr = fopen("db.log","r+")) == NULL) {
		rc = FILE_OPEN_ERROR;
	} else {
		int MAX_FILE_LOG_SUFFIX = 100;
		char backup_file_name_with_suffix[10];
		memset(backup_file_name_with_suffix,'\0', 10);
		for(int i = 1; i < MAX_FILE_LOG_SUFFIX ; i++) {
			memset(backup_file_name_with_suffix,'\0', 10);
			memcpy(backup_file_name_with_suffix, "db.log",6);
			char int_buffer[4];
			strcat(backup_file_name_with_suffix, itoa(i,int_buffer, 10));
			if((backup_log_file_ptr = fopen(backup_file_name_with_suffix, "r+")) != NULL) {
				printf("log file backup: %s already exists\n", backup_file_name_with_suffix);
				fclose(backup_log_file_ptr);
			} else {
				printf("log file backup: %s doesn't exist.", backup_file_name_with_suffix);
				break;
			}
		}
		if((backup_log_file_ptr = fopen(backup_file_name_with_suffix, "w+")) == NULL) {
			rc = FILE_OPEN_ERROR;
		} else {
			char ch;
			while((ch = fgetc(original_log_file_ptr)) != EOF){
				fputc(ch, backup_log_file_ptr);
			}
			fclose(backup_log_file_ptr);
			printf("Backed up original log file\n");
		}
		fclose(original_log_file_ptr);
	}
	return rc;
}

int parse_entered_timestamp(char* entered_timestamp) {
	int entered_length = strlen(entered_timestamp);
	if(entered_length != 14) {
		printf("entered length = %d", entered_length);
		return INVALID_TIMESTAMP_VALUE;
	}
	for(int i = 0; i < 14; i++) {
		printf("isdigit %d %d\n",isdigit(entered_timestamp[i]), entered_timestamp[i]);
		if(!isdigit(entered_timestamp[i])) {
			return INVALID_TIMESTAMP_VALUE;
		}
	}
	return 0;
}