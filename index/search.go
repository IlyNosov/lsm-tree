package index

import (
	"fmt"
	"strings"

	"github.com/RoaringBitmap/roaring"
)

type tokenType int

const (
	tokenIdent tokenType = iota
	tokenAnd
	tokenOr
	tokenNot
	tokenLParen
	tokenRParen
)

type token struct {
	typ tokenType
	val string
}

// tokenizeQuery разбивает строку запроса на токены
func tokenizeQuery(q string) []token {
	q = strings.ReplaceAll(q, "(", " ( ")
	q = strings.ReplaceAll(q, ")", " ) ")
	parts := strings.Fields(q)
	var tokens []token
	for _, p := range parts {
		switch strings.ToUpper(p) {
		case "AND":
			tokens = append(tokens, token{typ: tokenAnd})
		case "OR":
			tokens = append(tokens, token{typ: tokenOr})
		case "NOT":
			tokens = append(tokens, token{typ: tokenNot})
		case "(":
			tokens = append(tokens, token{typ: tokenLParen})
		case ")":
			tokens = append(tokens, token{typ: tokenRParen})
		default:
			// Сохраняем сырое слово, нормализация будет позже с учетом языка
			tokens = append(tokens, token{typ: tokenIdent, val: p})
		}
	}
	return tokens
}

// precedence возвращает приоритет оператора
func precedence(typ tokenType) int {
	switch typ {
	case tokenNot:
		return 3
	case tokenAnd:
		return 2
	case tokenOr:
		return 1
	default:
		return 0
	}
}

// infixToRPN преобразует инфиксную последовательность токенов в обратную польскую нотацию
func infixToRPN(tokens []token) ([]token, error) {
	var output []token
	var stack []token

	for _, tok := range tokens {
		switch tok.typ {
		case tokenIdent:
			output = append(output, tok)
		case tokenAnd, tokenOr, tokenNot:
			// пока стек не пуст и верхушка стека оператор с приоритетом >= текущего
			for len(stack) > 0 {
				top := stack[len(stack)-1]
				if top.typ == tokenLParen {
					break
				}
				if precedence(top.typ) >= precedence(tok.typ) {
					output = append(output, top)
					stack = stack[:len(stack)-1]
				} else {
					break
				}
			}
			stack = append(stack, tok)
		case tokenLParen:
			stack = append(stack, tok)
		case tokenRParen:
			// выталкиваем до левой скобки
			found := false
			for len(stack) > 0 {
				top := stack[len(stack)-1]
				stack = stack[:len(stack)-1]
				if top.typ == tokenLParen {
					found = true
					break
				}
				output = append(output, top)
			}
			if !found {
				return nil, fmt.Errorf("mismatched parentheses")
			}
		}
	}
	// выталкиваем оставшиеся операторы
	for len(stack) > 0 {
		top := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if top.typ == tokenLParen || top.typ == tokenRParen {
			return nil, fmt.Errorf("mismatched parentheses")
		}
		output = append(output, top)
	}
	return output, nil
}

// Search выполняет поиск по булевому запросу с учетом языка
// Поддерживаются операторы AND, OR, NOT и круглые скобки
func (idx *Indexer) Search(query string) ([]uint32, error) {
	// Токенизируем запрос
	rawTokens := tokenizeQuery(query)

	// Нормализуем идентификаторы с языком idx.lang
	normalizedTokens := make([]token, 0, len(rawTokens))
	for _, tok := range rawTokens {
		if tok.typ == tokenIdent {
			norm, ok := normalizeWord(tok.val, idx.lang)
			if !ok {
				norm = ""
			}
			normalizedTokens = append(normalizedTokens, token{typ: tokenIdent, val: norm})
		} else {
			normalizedTokens = append(normalizedTokens, tok)
		}
	}

	// Преобразуем в RPN
	rpn, err := infixToRPN(normalizedTokens)
	if err != nil {
		return nil, err
	}

	allDocs, err := idx.getAllDocs()
	if err != nil {
		return nil, err
	}

	var stack []*roaring.Bitmap

	for _, tok := range rpn {
		switch tok.typ {
		case tokenIdent:
			bm, err := idx.getBitmap(tok.val)
			if err != nil {
				return nil, err
			}
			stack = append(stack, bm)
		case tokenNot:
			if len(stack) < 1 {
				return nil, fmt.Errorf("NOT requires one operand")
			}
			operand := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			// NOT x = allDocs AND NOT x
			result := roaring.AndNot(allDocs, operand)
			stack = append(stack, result)
		case tokenAnd:
			if len(stack) < 2 {
				return nil, fmt.Errorf("AND requires two operands")
			}
			right := stack[len(stack)-1]
			left := stack[len(stack)-2]
			stack = stack[:len(stack)-2]
			result := roaring.And(left, right)
			stack = append(stack, result)
		case tokenOr:
			if len(stack) < 2 {
				return nil, fmt.Errorf("OR requires two operands")
			}
			right := stack[len(stack)-1]
			left := stack[len(stack)-2]
			stack = stack[:len(stack)-2]
			result := roaring.Or(left, right)
			stack = append(stack, result)
		}
	}

	if len(stack) != 1 {
		return nil, fmt.Errorf("invalid query evaluation")
	}

	return stack[0].ToArray(), nil
}
