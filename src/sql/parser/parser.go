package parser

import (
	. "../lexer"
	. "./statements"
	"errors"
)

var (
	ParsedErr = errors.New("Parsed Err")
)

type (
	Parser struct {
		Lexer *LexerImp
	}
)

func Parse(s string) (*Appliable, error) {
	parser := &Parser{
		&LexerImp{
			s,
			0,0,0,0,
			Token{"BEGIN", nil},
		},
	}
	parser.Init()
	return parser.ParseLine()
}

func (parser *Parser) Init() error {
	parser.Lexer.Init()
	return parser.Lexer.NextToken()
}

func (parser *Parser) ParseLine() (*Appliable, error) {
	tok := parser.Lexer.Token()

	if parser.matchSimple(tok, "SELECT") {
		return parser.ParseSelect()
	}

	if parser.matchSimple(tok, "CREATE") {
		return parser.ParseCreate()
	}

	if parser.matchSimple(tok, "UPDATE") {
		return parser.ParseUpdate()
	}

	if parser.matchSimple(tok, "DELETE") {
		return parser.ParseDelete()
	}

	if parser.matchSimple(tok, "INSERT") {
		return parser.ParseInsert()
	}

	if parser.matchSimple(tok, "DROP") {
		return parser.ParseDrop()
	}

	if parser.matchSemi(tok) {
		return nil, nil
	}

	return nil, ParsedErr

}

func (parser *Parser) ParseDrop() (*Appliable, error) {
	dropStat := &DropStatement{}

	tableName := parser.Lexer.Token()
	if !parser.matchType(tableName,"IDENTIFIER") {
		return nil, ParsedErr
	}

	dropStat.TableName = tableName

	if !parser.matchSemi(parser.Lexer.Token()) {
		return nil, ParsedErr
	}
	return dropStat, nil
}

func (parser *Parser) ParseSelect() (*Appliable, error) {
	selectStat := SelectStatement{}

	uniq := parser.Lexer.Token()
	if parser.matchSimple(uniq, "UNIQUE") {
		selectStat.Unique = Unique{
			Unique: uniq,
		}
	} else if parser.match(uniq, "STAR", "*") {
		selectStat.Star = Star{
			Star: uniq,
		}
	} else if parser.matchSimple(uniq, "ALL") {
		selectStat.All = All{
			All: uniq,
		}
	} else {
		fields, err := parser.ParseFields()
		if err != nil {
			return nil, ParsedErr
		}

		selectStat.Fields = fields
	}

	from, err := parser.ParseFrom()
	if err != nil  { return nil, ParsedErr }
	selectStat.From = from


	if where, err := parser.ParseWhere();
		err == nil { selectStat.Where = where }

	if !parser.matchSemi(parser.Lexer.Token()) {
		return nil, ParsedErr
	}
	parser.Lexer.NextToken()

	return selectStat, nil
}

func (parser *Parser) ParseUpdate() (*Appliable, error) {
	upStat := &UpdateStatement{}

	if !parser.match(parser.Lexer.Token(), "LPAREN", "(") {
		return nil, ParsedErr
	}

	col := parser.Lexer.Token()
	if !parser.matchType(col, "IDENTIFIER") {
		return nil, ParsedErr
	}
	upStat.Col = col

	if !parser.match(parser.Lexer.Token(), "EQ", "=") {
		return nil, ParsedErr
	}

	value := parser.Lexer.Token()
	if parser.matchType(value, "STRING") ||
		parser.matchType(value, "INT") ||
		parser.matchType(value, "DOUBLE") {
		upStat.Value = value
	} else {
		return nil, ParsedErr
	}

	if !parser.match(parser.Lexer.Token(), "RPAREN", ")") {
		return nil, ParsedErr
	}

	if !parser.matchSimple(parser.Lexer.Token(), "FROM") {
		return nil, ParsedErr
	}

	tableName := parser.Lexer.NextToken()
	if !parser.matchType(tableName, "IDENTIFIER") {
		return nil, ParsedErr
	}
	upStat.TableName = tableName

	where, err := parser.ParseWhere()
	if err != nil {
		return nil, ParsedErr
	}
	upStat.Where = where

	if !parser.matchSemi(parser.Lexer.Token()) {
		return nil, ParsedErr
	}
	return upStat, nil
}

func (parser *Parser) ParseDelete() (*Appliable, error) {
	delStat := &DeleteStatement{}

	from := parser.Lexer.Token()
	if !parser.matchSimple(from, "FROM") {
		return nil, ParsedErr
	}

	tableName := parser.Lexer.Token()
	if !parser.matchType(tableName, "IDENTIFIER") {
		return nil, ParsedErr
	}
	delStat.TableName = tableName

	where, err := parser.ParseWhere()
	if err == nil {
		delStat.Where = where
	}

	if !parser.matchSemi(parser.Lexer.Token()) {
		return nil, ParsedErr
	}
	return delStat, nil
}

func (parser *Parser) ParseFields() (Fields, error) {
	fields := make([]Field, 0)
	for {
		field := parser.Lexer.Token()
		if !IsField(field) {
			return Fields{}, ParsedErr
		}

		fields = append(fields, Field{field})

		if !parser.matchValue(parser.Lexer.Token(), ",") {
			break
		}
	}

	return Fields{
		Idfs: fields,
	}, nil
}

func (parser *Parser) ParseStar() (Star, error) {
	star := parser.Lexer.Token()
	if !parser.match(star, "STAR", "*") {
		return Star{}, ParsedErr
	}

	return Star{Star: star}, nil
}

func (parser *Parser) ParseFrom() (From, error) {
	from := parser.Lexer.Token()
	if !parser.matchSimple(from, "FROM") {
		return From{}, ParsedErr
	}

	table := parser.Lexer.Token()
	if !parser.matchSimple(table, "IDENTIFIER") {
		return From{}, ParsedErr
	}

	fromStat := From{
		Table: Table{table},
	}

	if !IsFromStatement(fromStat) {
		return From{}, ParsedErr
	}
	return fromStat, nil
}

func (parser *Parser) ParseWhere() (Where, error) {
	where := parser.Lexer.Token()

	if !parser.matchSimple(where, "WHERE") {
		return Where{}, ParsedErr
	}

	expr, err := parser.ParseExpr()
	if err != nil {
		return Where{}, ParsedErr
	}

	whereStat := Where{
		expr,
	}

	if !IsWhereStatement(whereStat) {
		return Where{}, ParsedErr
	}

	return whereStat, nil
}

func (parser *Parser) ParseCreate() (*Appliable, error) {
	createStat := CreateStatement{}

	tableName := parser.Lexer.Token()
	if tableName.TypeInfo != "IDENTIFIER" {
		return nil, ParsedErr
	}
	parser.Lexer.NextToken()
	createStat.TableName = tableName

	if openBlock := parser.Lexer.Token();
		!parser.match(openBlock, "LBRACE", "{") {
		return nil, ParsedErr
	}

	for ;; {
		col := parser.Lexer.Token()
		if !parser.matchType(col, "IDENTIFIER") {
			return nil, ParsedErr
		}
		createStat.Cols = append(createStat.Cols, col)

		t := parser.Lexer.Token()
		if  (t.Value != "STRING" &&
				t.Value != "INT"&&
				t.Value != "DOUBLE") ||
			!parser.matchType(t,"IDENTIFIER") {
			return nil, ParsedErr
		}

		if t.Value == "STRING" {
			num := parser.Lexer.Token()
			if num.TypeInfo != "INT" || num <= 0 || num > 1024 {
				return nil, ParsedErr
			}

			createStat.Lens = append(createStat.Lens, num)
			parser.Lexer.NextToken()
		} else {
			if t.Value == "INT"{
				createStat.Lens = append(createStat.Lens, 2)
			} else {
				createStat.Lens = append(createStat.Lens, 4)
			}
		}

		createStat.Types = append(createStat.Types, t)

		nullable := parser.Lexer.Token()
		if nullable.Value == "Nullable" {
			createStat.Nullable = append(createStat.Nullable, true)
			parser.Lexer.NextToken()
		} else {
			createStat.Nullable = append(createStat.Nullable, false)
		}

		comma := parser.Lexer.Token()
		if parser.match(comma, "COMMA", ",") {
			continue
		} else if comma.TypeInfo == "SEMI" && comma.Value == ";" {
			break
		}
		return nil, ParsedErr

	}

	index := parser.Lexer.Token()
	if index.Value == "Index" {
		parser.Lexer.NextToken()

		for ;; {
			indexCol := parser.Lexer.Token()
			if indexCol.TypeInfo != "IDENTIFIER" &&
				indexCol.TypeInfo != "SEMI" {
				return nil, ParsedErr
			}

			createStat.Indexes = append(createStat.Indexes,
				indexCol.Value)
		}
	}

	if !parser.matchSemi(parser.Lexer.Token()) {
		return nil, ParsedErr
	}

	return &createStat, nil
}

func (parser *Parser) ParseInsert() (Appliable, error) {
	insertStat := &InsertStatement{}
	if !parser.matchSimple(parser.Lexer.Token(), "INTO") {
		return nil, ParsedErr
	}
	tableName := parser.Lexer.Token()
	if !parser.matchType(tableName, "IDENTIFIER") {
		return nil, ParsedErr
	}

	insertStat.TableName = tableName

	if !parser.matchSimple(parser.Lexer.Token(), "VALUES") {
		return nil, ParsedErr
	}

	if !parser.match(parser.Lexer.Token(), "LPAREN", "(") {
		return nil, ParsedErr
	}

	for ;; {
		v := parser.Lexer.Token()
		if parser.match(v, "RPAREN", ")") {
			break
		}
		if v.TypeInfo != "INT" && v.TypeInfo != "DOUBLE" &&
			v.TypeInfo != "STRING" && v.TypeInfo != "NULL" {
			return nil, ParsedErr
		}

		insertStat.Values = append(insertStat.Values, v)

		parser.Lexer.NextToken()
	}

	if !parser.matchSemi(parser.Lexer.Token()) {
		return nil, ParsedErr
	}
	return insertStat, nil
}

func (parser *Parser) ParseExpr() (Expr, error) {
	expr := Expr{}

	for {
		condition, err := parser.ParseCondition()
		if err != nil {
			break
		}
		expr.Conditions = append(expr.Conditions, condition)

		inter := parser.Lexer.Token()
		if inter.TypeInfo != "AND" &&
			inter.TypeInfo != "OR" &&
			inter.TypeInfo != "NOT" {
			break
		}

		expr.InterOP = append(expr.InterOP,
			LogicOperation{inter.Value.(string)})

		parser.Lexer.NextToken()
	}

	if !IsExpr(expr) {
		return Expr{}, ParsedErr
	}

	return expr, nil
}
func (parser *Parser) ParseCondition() (Condition, error) {
	condition := Condition{}

	lval, err := parser.ParseValue()
	if err != nil {
		return condition, ParsedErr
	}
	condition.LVal = lval

	lop, err := parser.ParseLogicOperation()
	if err != nil {
		return condition, ParsedErr
	}
	condition.Op = lop

	rval, err := parser.ParseValue()
	if err != nil {
		return condition, ParsedErr
	}
	condition.RVal = rval

	if !IsCondition(condition) {
		return condition, ParsedErr
	}

	return condition, nil
}
func (parser *Parser) ParseLogicOperation() (LogicOperation, error) {
	op := parser.Lexer.Token()
	if op.Value != ">" &&
		op.Value != "<" &&
		op.Value != ">=" &&
		op.Value != "<=" &&
		op.Value != "==" {
		return LogicOperation{}, ParsedErr
	}

	parser.Lexer.NextToken()
	return LogicOperation{op.Value.(string)}, nil
}

func (parser *Parser) ParseValue() (Value, error) {
	op := parser.Lexer.Token()
	if op.TypeInfo != "INT" &&
		op.TypeInfo != "DOUBLE" &&
		op.TypeInfo != "STRING" &&
		op.TypeInfo != "IDENTIFIER" {
		return Value{}, ParsedErr
	}

	parser.Lexer.NextToken()

	return Value{op.Value}, nil
}

func (parser *Parser) match(token Token, typeInfo string, value string) bool {
	matched := token.TypeInfo == typeInfo && token.Value == value
	if !matched {
		return false
	} else {
		parser.Lexer.NextToken()
		return true
	}
}

func (parser *Parser) matchValue(token Token, value string) bool {
	matched := token.Value == value

	if !matched {
		return false
	} else {
		parser.Lexer.NextToken()
		return true
	}
}

func (parser *Parser) matchSimple(token Token, typeInfo string) bool {
	if parser.match(token, typeInfo, typeInfo) {
		return true
	}
	if token.TypeInfo == typeInfo && typeInfo == "IDENTIFIER" {
		parser.Lexer.NextToken()
		return true
	}
	return false
}

func (parser *Parser) matchSemi(token Token) bool {
	return parser.match(token, "SEMI", ";")
}

func (parser *Parser) matchType(token Token, t string) bool {
	if token.TypeInfo != t {
		return false
	}
	parser.Lexer.NextToken()
	return true
}