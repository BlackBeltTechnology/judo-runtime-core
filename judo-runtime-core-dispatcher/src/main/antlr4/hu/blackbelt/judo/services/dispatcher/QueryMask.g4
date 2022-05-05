grammar QueryMask;

/* ============= */
/* Grammar rules */
/* ============= */
parse
 : LCLBRACKET memberList? RCLBRACKET EOF
 ;

memberList
  : member ( COMMA member )*
  ;

member
  : attribute
  | relation
  ;

attribute
  : IDENTIFIER
  ;

relation
  : IDENTIFIER LCLBRACKET memberList? RCLBRACKET
  ;


/* ============= */
/* Lexical rules */
/* ============= */

COMMA : ',' ;

LCLBRACKET : '{' ;
RCLBRACKET : '}' ;

IDENTIFIER : [a-zA-Z_][a-zA-Z_0-9]* ;
WS  :   [ \t\n\r]+ -> skip ;
