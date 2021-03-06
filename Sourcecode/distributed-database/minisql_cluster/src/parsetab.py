
# parsetab.py
# This file is automatically generated. Do not edit.
# pylint: disable=W,C,R
_tabversion = '3.10'

_lr_method = 'LALR'

_lr_signature = 'AND CHAR COMMA CREATE DELETE DOT DROP EQ EXECUTE FCONST FLOAT FROM GE GT ICONST ID INDEX INSERT INT INTO KEY LE LPAREN LT NE ON OR PRIMARY QUIT RPAREN SCONST SELECT SEMICOLON STAR TABLE UNIQUE VALUES WHERE\n        sql_statement : create_statement\n                        | insert_statement\n                        | select_statement\n                        | delete_statement\n                        | drop_statement\n                        | quit_statement\n                        | execute_statement\n    \n        create_statement : create_table\n                          | create_index\n    \n        insert_statement : INSERT INTO ID VALUES LPAREN value_list RPAREN SEMICOLON\n    \n        select_statement : select_all\n                        | conditional_select\n    \n        delete_statement : delete_all\n                        | conditional_delete\n    \n        drop_statement : drop_table\n                        | drop_index\n    \n        quit_statement : QUIT SEMICOLON\n    \n        create_table : CREATE TABLE ID LPAREN column_list RPAREN SEMICOLON\n                    | CREATE TABLE ID LPAREN column_list COMMA primary_clause RPAREN SEMICOLON\n    \n        create_index : CREATE INDEX ID ON ID LPAREN ID RPAREN SEMICOLON\n    \n        column_list : column\n                    | column_list COMMA column\n    \n        column :  ID column_type\n                | ID column_type UNIQUE\n    \n        column_type : INT\n                    | FLOAT\n                    | CHAR LPAREN ICONST RPAREN\n    \n        primary_clause : PRIMARY KEY LPAREN ID RPAREN\n    \n        value_list : value\n                    | value_list COMMA value\n    \n        value : ICONST\n                | FCONST\n                | SCONST\n    \n        select_all : SELECT STAR FROM ID SEMICOLON\n    \n        conditional_select : SELECT STAR FROM ID WHERE conditions SEMICOLON\n    \n        conditions : condition\n                    | conditions AND condition\n                    | conditions OR condition\n    \n        condition :  ID GT value\n                    | ID LT value\n                    | ID EQ value\n                    | ID GE value\n                    | ID LE value\n                    | ID NE value\n    \n        delete_all : DELETE FROM ID SEMICOLON\n    \n        conditional_delete : DELETE FROM ID WHERE conditions SEMICOLON\n    \n        drop_table : DROP TABLE ID SEMICOLON\n    \n        drop_index : DROP INDEX ID SEMICOLON\n    \n        execute_statement : EXECUTE ID SEMICOLON\n                            | EXECUTE ID DOT ID SEMICOLON\n    '
    
_lr_action_items = {'INSERT':([0,],[11,]),'QUIT':([0,],[18,]),'EXECUTE':([0,],[19,]),'CREATE':([0,],[20,]),'SELECT':([0,],[21,]),'DELETE':([0,],[22,]),'DROP':([0,],[23,]),'$end':([1,2,3,4,5,6,7,8,9,10,12,13,14,15,16,17,25,34,47,49,50,52,57,81,88,93,102,109,111,],[0,-1,-2,-3,-4,-5,-6,-7,-8,-9,-11,-12,-13,-14,-15,-16,-17,-49,-45,-47,-48,-50,-34,-46,-18,-35,-10,-19,-20,]),'INTO':([11,],[24,]),'SEMICOLON':([18,26,39,40,41,43,46,60,61,64,65,66,71,74,84,94,95,96,97,98,99,100,101,105,107,],[25,34,47,49,50,52,57,81,-36,-31,-32,-33,88,93,102,-39,-40,-41,-42,-43,-44,-37,-38,109,111,]),'ID':([19,24,27,28,30,31,32,35,38,44,45,48,58,72,73,82,83,110,],[26,33,36,37,39,40,41,43,46,53,56,59,59,53,92,59,59,112,]),'TABLE':([20,23,],[27,31,]),'INDEX':([20,23,],[28,32,]),'STAR':([21,],[29,]),'FROM':([22,29,],[30,38,]),'DOT':([26,],[35,]),'VALUES':([33,],[42,]),'LPAREN':([36,42,56,70,106,],[44,51,73,87,110,]),'ON':([37,],[45,]),'WHERE':([39,46,],[48,58,]),'ICONST':([51,75,76,77,78,79,80,85,87,],[64,64,64,64,64,64,64,64,104,]),'FCONST':([51,75,76,77,78,79,80,85,],[65,65,65,65,65,65,65,65,]),'SCONST':([51,75,76,77,78,79,80,85,],[66,66,66,66,66,66,66,66,]),'INT':([53,],[68,]),'FLOAT':([53,],[69,]),'CHAR':([53,],[70,]),'RPAREN':([54,55,62,63,64,65,66,67,68,69,86,89,90,92,103,104,108,112,113,],[71,-21,84,-29,-31,-32,-33,-23,-25,-26,-24,105,-22,107,-30,108,-27,113,-28,]),'COMMA':([54,55,62,63,64,65,66,67,68,69,86,90,103,108,],[72,-21,85,-29,-31,-32,-33,-23,-25,-26,-24,-22,-30,-27,]),'GT':([59,],[75,]),'LT':([59,],[76,]),'EQ':([59,],[77,]),'GE':([59,],[78,]),'LE':([59,],[79,]),'NE':([59,],[80,]),'AND':([60,61,64,65,66,74,94,95,96,97,98,99,100,101,],[82,-36,-31,-32,-33,82,-39,-40,-41,-42,-43,-44,-37,-38,]),'OR':([60,61,64,65,66,74,94,95,96,97,98,99,100,101,],[83,-36,-31,-32,-33,83,-39,-40,-41,-42,-43,-44,-37,-38,]),'UNIQUE':([67,68,69,108,],[86,-25,-26,-27,]),'PRIMARY':([72,],[91,]),'KEY':([91,],[106,]),}

_lr_action = {}
for _k, _v in _lr_action_items.items():
   for _x,_y in zip(_v[0],_v[1]):
      if not _x in _lr_action:  _lr_action[_x] = {}
      _lr_action[_x][_k] = _y
del _lr_action_items

_lr_goto_items = {'sql_statement':([0,],[1,]),'create_statement':([0,],[2,]),'insert_statement':([0,],[3,]),'select_statement':([0,],[4,]),'delete_statement':([0,],[5,]),'drop_statement':([0,],[6,]),'quit_statement':([0,],[7,]),'execute_statement':([0,],[8,]),'create_table':([0,],[9,]),'create_index':([0,],[10,]),'select_all':([0,],[12,]),'conditional_select':([0,],[13,]),'delete_all':([0,],[14,]),'conditional_delete':([0,],[15,]),'drop_table':([0,],[16,]),'drop_index':([0,],[17,]),'column_list':([44,],[54,]),'column':([44,72,],[55,90,]),'conditions':([48,58,],[60,74,]),'condition':([48,58,82,83,],[61,61,100,101,]),'value_list':([51,],[62,]),'value':([51,75,76,77,78,79,80,85,],[63,94,95,96,97,98,99,103,]),'column_type':([53,],[67,]),'primary_clause':([72,],[89,]),}

_lr_goto = {}
for _k, _v in _lr_goto_items.items():
   for _x, _y in zip(_v[0], _v[1]):
       if not _x in _lr_goto: _lr_goto[_x] = {}
       _lr_goto[_x][_k] = _y
del _lr_goto_items
_lr_productions = [
  ("S' -> sql_statement","S'",1,None,None,None),
  ('sql_statement -> create_statement','sql_statement',1,'p_sql_statement','interpreter.py',99),
  ('sql_statement -> insert_statement','sql_statement',1,'p_sql_statement','interpreter.py',100),
  ('sql_statement -> select_statement','sql_statement',1,'p_sql_statement','interpreter.py',101),
  ('sql_statement -> delete_statement','sql_statement',1,'p_sql_statement','interpreter.py',102),
  ('sql_statement -> drop_statement','sql_statement',1,'p_sql_statement','interpreter.py',103),
  ('sql_statement -> quit_statement','sql_statement',1,'p_sql_statement','interpreter.py',104),
  ('sql_statement -> execute_statement','sql_statement',1,'p_sql_statement','interpreter.py',105),
  ('create_statement -> create_table','create_statement',1,'p_create_statement','interpreter.py',112),
  ('create_statement -> create_index','create_statement',1,'p_create_statement','interpreter.py',113),
  ('insert_statement -> INSERT INTO ID VALUES LPAREN value_list RPAREN SEMICOLON','insert_statement',8,'p_insert_statement','interpreter.py',130),
  ('select_statement -> select_all','select_statement',1,'p_select_statement','interpreter.py',146),
  ('select_statement -> conditional_select','select_statement',1,'p_select_statement','interpreter.py',147),
  ('delete_statement -> delete_all','delete_statement',1,'p_delete_statement','interpreter.py',169),
  ('delete_statement -> conditional_delete','delete_statement',1,'p_delete_statement','interpreter.py',170),
  ('drop_statement -> drop_table','drop_statement',1,'p_drop_statement','interpreter.py',184),
  ('drop_statement -> drop_index','drop_statement',1,'p_drop_statement','interpreter.py',185),
  ('quit_statement -> QUIT SEMICOLON','quit_statement',2,'p_quit_statement','interpreter.py',204),
  ('create_table -> CREATE TABLE ID LPAREN column_list RPAREN SEMICOLON','create_table',7,'p_create_table','interpreter.py',214),
  ('create_table -> CREATE TABLE ID LPAREN column_list COMMA primary_clause RPAREN SEMICOLON','create_table',9,'p_create_table','interpreter.py',215),
  ('create_index -> CREATE INDEX ID ON ID LPAREN ID RPAREN SEMICOLON','create_index',9,'p_create_index','interpreter.py',231),
  ('column_list -> column','column_list',1,'p_column_list','interpreter.py',243),
  ('column_list -> column_list COMMA column','column_list',3,'p_column_list','interpreter.py',244),
  ('column -> ID column_type','column',2,'p_column','interpreter.py',256),
  ('column -> ID column_type UNIQUE','column',3,'p_column','interpreter.py',257),
  ('column_type -> INT','column_type',1,'p_column_type','interpreter.py',267),
  ('column_type -> FLOAT','column_type',1,'p_column_type','interpreter.py',268),
  ('column_type -> CHAR LPAREN ICONST RPAREN','column_type',4,'p_column_type','interpreter.py',269),
  ('primary_clause -> PRIMARY KEY LPAREN ID RPAREN','primary_clause',5,'p_primary_clause','interpreter.py',282),
  ('value_list -> value','value_list',1,'p_value_list','interpreter.py',290),
  ('value_list -> value_list COMMA value','value_list',3,'p_value_list','interpreter.py',291),
  ('value -> ICONST','value',1,'p_value','interpreter.py',303),
  ('value -> FCONST','value',1,'p_value','interpreter.py',304),
  ('value -> SCONST','value',1,'p_value','interpreter.py',305),
  ('select_all -> SELECT STAR FROM ID SEMICOLON','select_all',5,'p_select_all','interpreter.py',313),
  ('conditional_select -> SELECT STAR FROM ID WHERE conditions SEMICOLON','conditional_select',7,'p_conditional_select','interpreter.py',323),
  ('conditions -> condition','conditions',1,'p_conditions','interpreter.py',334),
  ('conditions -> conditions AND condition','conditions',3,'p_conditions','interpreter.py',335),
  ('conditions -> conditions OR condition','conditions',3,'p_conditions','interpreter.py',336),
  ('condition -> ID GT value','condition',3,'p_condition','interpreter.py',349),
  ('condition -> ID LT value','condition',3,'p_condition','interpreter.py',350),
  ('condition -> ID EQ value','condition',3,'p_condition','interpreter.py',351),
  ('condition -> ID GE value','condition',3,'p_condition','interpreter.py',352),
  ('condition -> ID LE value','condition',3,'p_condition','interpreter.py',353),
  ('condition -> ID NE value','condition',3,'p_condition','interpreter.py',354),
  ('delete_all -> DELETE FROM ID SEMICOLON','delete_all',4,'p_delete_all','interpreter.py',363),
  ('conditional_delete -> DELETE FROM ID WHERE conditions SEMICOLON','conditional_delete',6,'p_conditional_delete','interpreter.py',373),
  ('drop_table -> DROP TABLE ID SEMICOLON','drop_table',4,'p_drop_table','interpreter.py',385),
  ('drop_index -> DROP INDEX ID SEMICOLON','drop_index',4,'p_drop_index','interpreter.py',395),
  ('execute_statement -> EXECUTE ID SEMICOLON','execute_statement',3,'p_execute_statement','interpreter.py',405),
  ('execute_statement -> EXECUTE ID DOT ID SEMICOLON','execute_statement',5,'p_execute_statement','interpreter.py',406),
]
