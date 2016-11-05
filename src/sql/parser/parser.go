package parser

import (
	. "../lexer"
	. "./statements"
	"errors"
)

const (
	ParsedErr = errors.New("Parsed Err")
)

type (
	Parser struct {
		lexer *LexerImp
	}
)

func (parser *Parser) Init() error {
	parser.lexer.Init()
	return parser.lexer.NextToken()
}

func (parser *Parser) ParseLine() (Appliable, error) {
	if err := parser.lexer.NextToken(); err != nil {
		return nil, ParsedErr
	}

	tok := parser.lexer.Token()

	switch tok {
	case parser.matchSimple(tok, "SELECT"):
		return parser.ParseSelect()
	case parser.matchSimple(tok, "CREATE"):
		return parser.ParseCreate()
	case parser.matchSimple(tok, "UPDATE"):
		return parser.ParseUpdate()
	case parser.matchSimple(tok, "DELETE"):
		return parser.ParseDelete()
	case parser.matchSimple(tok, "INSERT"):
		return parser.ParseInsert()
	case parser.matchSemi(tok):
		return nil, nil
	default:
		return nil, ParsedErr
	}
}

func (parser *Parser) ParseSelect() (Appliable, error) {
	selStat := SelectStatement{}

	uniq := parser.lexer.Token()
	if !parser.matchSimple(uniq, "UNIQUE") {
		return nil, ParsedErr
	} else {
		selStat.Unique = Unique {
			Unique : uniq,
		}
	}

	aster := parser.lexer.Token()
	if parser.match(aster, "STAR", "*") {
		selStat.Star = Star {
			Star : aster,
		}
	} else if parser.matchSimple(aster, "ALL") {
		selStat.All = All {
			All : aster,
		}
	} else {
		fields, err := parser.ParseFields()
		if err != nil {
			return nil, ParsedErr
		}
		
		selStat.Fields = fields
	}

	from, err := parser.ParseFrom()
	if err != nil {
		return nil, ParsedErr
	}
	selStat.From = from

	where, err := parser.ParseWhere()
	if err != nil {
		if parser.matchSemi(parser.lexer.Token()) {
			return selStat, nil
		}
		return nil, ParsedErr
	}
	selStat.Where = where

	return selStat, nil
}

func (parser *Parser) ParseFields() (Fields, error) {
	fields := make([]Field, 0)
	for {
		field := parser.lexer.Token()
		if !IsField(field) {
			return nil, ParsedErr
		}
		
		append(fields, Field{
			Token : field,
		})

		if err := parser.lexer.NextToken(); err != nil {
			return nil, ParsedErr
		}

		if !parser.matchValue(parser.lexer.Token(), ",") {
			break
		}
	}

	return Fields{
		Idfs : fields,
	}, nil
}

func (parser *Parser) ParseStar() (Star, error) {
	if parser.ma
}


func (parser *Parser) ParseFrom() (From, error) {
	from := parser.lexer.Token()
	if !parser.matchSimple(from, "FROM") {
		return nil, ParsedErr
	}

	parser.lexer.NextToken()
	table := parser.lexer.Token()

	fromStat := From{
		Table : table,
	}

	if !IsFromStatement(fromStat) {
		return nil, ParsedErr
	}
	return fromStat, nil
}

func (parser *Parser) ParseWhere() (Where, error) {
	return nil, nil
}

func (parser *Parser) ParseCreate() (Appliable, error) {
	return nil, nil
}

func (parser *Parser) ParseUpdate() (Appliable, error) {
	return nil, nil
}

func (parser *Parser) ParseDelete() (Appliable, error) {
	return nil, nil
}

func (parser *Parser) ParseInsert() (Appliable, error) {
	return nil, nil
}


func (parser *Parser) match(token Token, typeInfo string, value string) bool {
	matched := token.TypeInfo == typeInfo && token.Value == value
	if !matched {
		return false
	} else {
		parser.lexer.NextToken()
		return true
	}
}

func (parser *Parser) matchValue(token Token, value string) bool {
	matched := token.Value == value

	if !matched {
		return false
	} else {
		parser.lexer.NextToken()
		return true
	}
}

func (parser *Parser) matchSimple(token Token, typeInfo string) bool {
	return parser.match(token, typeInfo, typeInfo)
}

func (parser *Parser) matchSemi(token Token) bool {
	return parser.match(token, "SEMI", ";")
}

