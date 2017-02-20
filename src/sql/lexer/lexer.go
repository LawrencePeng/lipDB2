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
		Text                       string
		Pos, Mark, TextLen, BufPos int
		Tken                       Token
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
	return tokens[tok.TypeInfo] != "" && IsLetter(tok.Value.(byte))
}

func (imp *LexerImp) Init() error {
	text := strings.TrimSpace(imp.Text)
	imp.Text = text
	imp.TextLen = len(text)

	imp.Pos = 0
	imp.Mark = 0
	imp.BufPos = 0

	if imp.TextLen <= 0 {
		return LexerInitError{}
	}

	imp.Tken = Token{"BEGIN", ""}
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

func (imp *LexerImp) IsEOF() bool { return imp.Pos == imp.TextLen }

func (imp *LexerImp) ScanNumber() error {
	imp.Mark = imp.Pos

	text := imp.Text
	textLen := imp.TextLen
	pos := imp.Pos
	bufPos := imp.BufPos

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
		imp.Pos = pos
		parsedInt, err := strconv.ParseInt(text[(imp.Mark):(imp.Mark+bufPos)], 10, 64)

		if err != nil {
			return LexerParseError{}
		}

		imp.Tken = Token{"INT", parsedInt}

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
		if imp.Pos == textLen {
			break
		}

		if !IsNumber(text[pos]) {
			break
		}

		bufPos++
		pos++
	}

	value := text[imp.Mark : imp.Mark+bufPos]

	if isDouble {
		parsedFloat, err := strconv.ParseFloat(value, 64)

		if err != nil {
			return LexerParseError{}
		}

		imp.Tken = Token{"DOUBLE", parsedFloat}
	} else {
		parsedInt, err := strconv.ParseInt(value, 10, 64)

		if err != nil {
			return LexerParseError{}
		}

		imp.Tken = Token{"INT", parsedInt}
	}

	imp.Pos = pos

	return nil
}

func (imp *LexerImp) ScanIdentifier() error {
	imp.Mark = imp.Pos

	text := imp.Text
	pos := imp.Pos
	bufPos := 1

	for {
		pos += 1

		if pos == imp.TextLen {
			break
		}

		if !IsLetter(text[pos]) {
			break
		}

		bufPos += 1
	}

	idtfier := text[(imp.Mark):(imp.Mark + bufPos)]
	tokString := tokens[idtfier]

	if tokString != "" {
		imp.Tken = Token{tokString, tokString}
	} else {
		imp.Tken = Token{"IDENTIFIER", idtfier}
	}

	imp.Pos = pos

	return nil
}

func (imp *LexerImp) Token() Token {
	return imp.Tken
}

func (imp *LexerImp) NextToken() error {
	text := imp.Text
	textLen := imp.TextLen
	imp.BufPos = 0

	if imp.Pos > textLen {
		return LexerParseError{}
	}

	if imp.Pos == textLen {
		imp.Tken = Token{"EOF", nil}
		return nil
	}

	for ; IsWhiteSpace(text[imp.Pos]); imp.Pos += 1 {
	}

	switch text[imp.Pos] {
	case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '-':
		imp.ScanNumber()
	case ',':
		imp.Pos += 1
		imp.Tken = Token{"COMMA", ","}
	case '\'':
		imp.Pos += 1
		imp.Tken = Token{"QUOTE", "'"}
	case '"':
		imp.Pos += 1

		for imp.BufPos = imp.Pos; imp.Text[imp.BufPos] != '"' && !imp.IsEOF(); {
			imp.BufPos++
		}
		imp.Tken = Token{"", imp.Text[imp.Pos:imp.BufPos]}
		imp.Pos = imp.BufPos + 1
	case '(':
		imp.Pos += 1
		imp.Tken = Token{"LPAREN", "("}
	case ')':
		imp.Pos += 1
		imp.Tken = Token{"RPAREN", ")"}
	case '[':
		imp.Pos += 1
		imp.Tken = Token{"LBRACKET", "["}
	case ']':
		imp.Pos += 1
		imp.Tken = Token{"RBRACKET", "]"}
	case '{':
		imp.Pos += 1
		imp.Tken = Token{"LBRACE", "{"}
	case '}':
		imp.Pos += 1
		imp.Tken = Token{"RBRACE", "}"}
	case '*':
		imp.Pos += 1
		imp.Tken = Token{"STAR", "*"}
	case '?':
		imp.Pos += 1
		imp.Tken = Token{"QUOS", "?"}
	case ';':
		imp.Pos += 1
		imp.Tken = Token{"SEMI", ";"}
	case '=':
		imp.Pos += 1
		if text[imp.Pos] == '=' {
			imp.Pos += 1
			imp.Tken = Token{"EQEQ", "=="}
		} else {
			imp.Tken = Token{"EQ", "="}
		}
	default:
		if IsLetter(text[imp.Pos]) {
			imp.ScanIdentifier()
		} else {
			return LexerInitError{}
		}
	}

	return nil
}
