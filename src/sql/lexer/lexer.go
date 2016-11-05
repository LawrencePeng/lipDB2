package lexer

import (
	"strconv"
	"strings"
)

type (
	Token struct {
		TypeInfo string
		Value    interface{}
	}

	Lexer interface {
		Init() error
		NextToken() error
		Token() (tks Token, err error)
	}

	LexerImp struct {
		text                       string
		pos, mark, textLen, bufPos int
		token                      Token
	}

	LexerInitError struct{}

	LexerParseError struct{}
)

func (LexerInitError) Error() string {
	return "Lexer's len is less than or equal to 0."
}

func (LexerParseError) Error() string {
	return "Lexing error"
}

// The keywords Tokens
var (
	tokens map[string]string = map[string]string{
		"SELECT": "SELECT",
		"DELETE": "DELETE",
		"INSERT": "INSERT",
		"UPDATE": "UPDATE",

		"FROM":   "FROM",
		"HAVING": "HAVING",
		"WHERE":  "WHERE",
		"ORDER":  "ORDER",
		"BY":     "BY",
		"GROUP":  "GROUP",
		"INTO":   "INTO",
		"AS":     "AS",

		"CREATE": "CREATE",
		"ALTER":  "ALTER",
		"DROP":   "DROP",
		"SET":    "SET",

		"NULL":     "NULL",
		"NOT":      "NOT",
		"DISTINCT": "DISTINCT",

		"TABLE":      "TABLE",
		"TABLESPACE": "TABLESPACE",
		"VIEW":       "VIEW",
		"SEQUENCE":   "SEQUENCE",
		"TRIGGER":    "TRIGGER",
		"USER":       "USER",
		"INDEX":      "INDEX",
		"SESSION":    "SESSION",
		"PROCEDURE":  "PROCEDURE",
		"FUNCTION":   "FUNCTION",

		"PRIMARY":    "PRIMARY",
		"KEY":        "KEY",
		"DEFAULT":    "DEFAULT",
		"CONSTRAINT": "CONSTRAINT",
		"CHECK":      "CHECK",
		"UNIQUE":     "UNIQUE",
		"FOREIGN":    "FOREIGN",
		"REFERENCES": "REFERENCES",

		"EXPLAIN": "EXPLAIN",
		"FOR":     "FOR",
		"IF":      "IF",

		"ALL":       "ALL",
		"UNION":     "UNION",
		"EXCEPT":    "EXCEPT",
		"INTERSECT": "INTERSECT",
		"MINUS":     "MINUS",
		"INNER":     "INNER",
		"LEFT":      "LEFT",
		"RIGHT":     "RIGHT",
		"FULL":      "FULL",
		"OUTER":     "OUTER",
		"JOIN":      "JOIN",
		"ON":        "ON",
		"SCHEMA":    "SCHEMA",
		"CAST":      "CAST",
		"COLUMN":    "COLUMN",
		"USE":       "USE",
		"DATABASE":  "DATABASE",
		"TO":        "TO",

		"AND":    "AND",
		"OR":     "OR",
		"XOR":    "XOR",
		"CASE":   "CASE",
		"WHEN":   "WHEN",
		"THEN":   "THEN",
		"ELSE":   "ELSE",
		"END":    "END",
		"EXISTS": "EXISTS",
		"IN":     "IN",

		"NEW":      "NEW",
		"ASC":      "ASC",
		"DESC":     "DESC",
		"IS":       "IS",
		"LIKE":     "LIKE",
		"ESCAPE":   "ESCAPE",
		"BETWEEN":  "BETWEEN",
		"VALUES":   "VALUES",
		"INTERVAL": "INTERVAL",

		"LOCK":     "LOCK",
		"SOME":     "SOME",
		"ANY":      "ANY",
		"TRUNCATE": "TRUNCATE",

		"TRUE":       "TRUE",
		"FALSE":      "FALSE",
		"LIMIT":      "LIMIT",
		"KILL":       "KILL",
		"IDENTIFIED": "IDENTIFIED",
		"PASSWORD":   "PASSWORD",
		"DUAL":       "DUAL",
		"BINARY":     "BINARY",
		"SHOW":       "SHOW",
		"REPLACE":    "REPLACE",

		"WHILE":     "WHILE",
		"DO":        "DO",
		"LEAVE":     "LEAVE",
		"ITERATE":   "ITERATE",
		"REPEAT":    "REPEAT",
		"UNTIL":     "UNTIL",
		"OPEN":      "OPEN",
		"CLOSE":     "CLOSE",
		"OUT":       "OUT",
		"INOUT":     "INOUT",
		"EXIT":      "EXIT",
		"UNDO":      "UNDO",
		"SQLSTATE":  "SQLSTATE",
		"CONDITION": "CONDITION",
	}
)

func IsField(tok Token) bool {
	return tokens[tok.TypeInfo] != "" && IsLetter(tok.Value)
}

func (imp *LexerImp) Init() error {
	text := strings.TrimSpace(imp.text)
	imp.text = text
	imp.textLen = len(text)

	imp.pos = 0
	imp.mark = 0
	imp.bufPos = 0

	if imp.textLen <= 0 {
		return LexerInitError{}
	}

	imp.token = Token{"BEGIN", ""}
	return nil
}

func IsWhiteSpace(ch byte) bool {
	return ch == ' ' ||
		ch == '\n' ||
		ch == '\t' ||
		ch == '\b' ||
		ch == '\r'
}

func IsNumber(ch byte) bool {
	return ch >= '0' && ch <= '9'
}

func IsLetter(ch byte) bool { return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') }

func (imp *LexerImp) IsEOF() bool { return imp.pos == imp.textLen }

func (imp *LexerImp) ScanNumber() error {
	imp.mark = imp.pos

	text := imp.text
	textLen := imp.textLen
	pos := imp.pos
	bufPos := imp.bufPos

	//if the first character is '-', accept it.
	if text[pos] == '-' {
		bufPos++
		pos++
	}

	for {
		if pos == textLen {
			break
		}

		if !IsNumber(text[pos]) {
			break
		}

		bufPos++
		pos++
	}

	if pos == textLen {
		imp.pos = pos
		parsedInt, err := strconv.ParseInt(text[(imp.mark):(imp.mark+bufPos)], 10, 64)

		if err != nil {
			return LexerParseError{}
		}

		imp.token = Token{"INT", parsedInt}

		return nil
	}

	isDouble := false

	if text[pos] != '.' {
		return LexerParseError{}
	}

	bufPos++
	pos++
	isDouble = true

	for {
		if imp.pos == textLen {
			break
		}

		if !IsNumber(text[pos]) {
			break
		}

		bufPos++
		pos++
	}

	value := text[imp.mark : imp.mark+bufPos]

	if isDouble {
		parsedFloat, err := strconv.ParseFloat(value, 64)

		if err != nil {
			return LexerParseError{}
		}

		imp.token = Token{"DOUBLE", parsedFloat}
	} else {
		parsedInt, err := strconv.ParseInt(value, 10, 64)

		if err != nil {
			return LexerParseError{}
		}

		imp.token = Token{"INT", parsedInt}
	}

	imp.pos = pos

	return nil
}

func (imp *LexerImp) ScanIdentifier() error {
	imp.mark = imp.pos

	text := imp.text
	pos := imp.pos
	bufPos := 1

	for {
		pos += 1

		if pos == imp.textLen {
			break
		}

		if !IsLetter(text[pos]) {
			break
		}

		bufPos += 1
	}

	idtfier := text[(imp.mark):(imp.mark + bufPos)]
	tokString := tokens[idtfier]

	if tokString != "" {
		imp.token = Token{tokString, tokString}
	} else {
		imp.token = Token{"IDENTIFIER", idtfier}
	}

	imp.pos = pos

	return nil
}

func (imp *LexerImp) Token() Token {
	return imp.token
}

func (imp *LexerImp) NextToken() error {
	text := imp.text
	textLen := imp.textLen
	imp.bufPos = 0

	if imp.pos > textLen {
		return LexerParseError{}
	}

	if imp.pos == textLen {
		imp.token = Token{"EOF", nil}
		return nil
	}

	for ; IsWhiteSpace(text[imp.pos]); imp.pos += 1 {
	}

	switch text[imp.pos] {
	case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '-':
		imp.ScanNumber()
	case ',':
		imp.pos += 1
		imp.token = Token{"COMMA", ","}
	case '\'':
		imp.pos += 1
		imp.token = Token{"QUOTE", "'"}
	case '"':
		imp.pos += 1
		imp.token = Token{"DQUOTE", "\""}
	case '(':
		imp.pos += 1
		imp.token = Token{"LPAREN", "("}
	case ')':
		imp.pos += 1
		imp.token = Token{"RPAREN", ")"}
	case '[':
		imp.pos += 1
		imp.token = Token{"LBRACKET", "["}
	case ']':
		imp.pos += 1
		imp.token = Token{"RBRACKET", "]"}
	case '{':
		imp.pos += 1
		imp.token = Token{"LBRACE", "{"}
	case '}':
		imp.pos += 1
		imp.token = Token{"RBRACE", "}"}
	case '*':
		imp.pos += 1
		imp.token = Token{"STAR", "*"}
	case '?':
		imp.pos += 1
		imp.token = Token{"QUOS", "?"}
	case ';':
		imp.pos += 1
		imp.token = Token{"SEMI", ":"}
	case '=':
		imp.pos += 1
		if text[imp.pos] == '=' {
			imp.pos += 1
			imp.token = Token{"EQEQ", "=="}
		} else {
			imp.token = Token{"EQ", "="}
		}
	default:
		if IsLetter(text[imp.pos]) {
			imp.ScanIdentifier()
		} else {
			return LexerInitError{}
		}
	}
	return nil
}
