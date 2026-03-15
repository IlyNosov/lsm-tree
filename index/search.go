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
	tokenDateRange // DATE(from,to)
)

type token struct {
	typ tokenType
	val string
}

// tokenizeQuery разбивает строку запроса на токены
func tokenizeQuery(q string) []token {
	var tokens []token
	i := 0

	for i < len(q) {
		// пропускаем пробелы
		if q[i] == ' ' || q[i] == '\t' {
			i++
			continue
		}

		// проверяем DATE(...)
		if i+5 <= len(q) && strings.ToUpper(q[i:i+5]) == "DATE(" {
			// ищем закрывающую скобку
			end := strings.Index(q[i:], ")")
			if end != -1 {
				// содержимое между DATE( и )
				inner := q[i+5 : i+end]
				tokens = append(tokens, token{typ: tokenDateRange, val: inner})
				i = i + end + 1
				continue
			}
		}

		// скобки
		if q[i] == '(' {
			tokens = append(tokens, token{typ: tokenLParen})
			i++
			continue
		}
		if q[i] == ')' {
			tokens = append(tokens, token{typ: tokenRParen})
			i++
			continue
		}

		// читаем слово до пробела или скобки
		j := i
		for j < len(q) && q[j] != ' ' && q[j] != '\t' && q[j] != '(' && q[j] != ')' {
			j++
		}
		word := q[i:j]
		i = j

		switch strings.ToUpper(word) {
		case "AND":
			tokens = append(tokens, token{typ: tokenAnd})
		case "OR":
			tokens = append(tokens, token{typ: tokenOr})
		case "NOT":
			tokens = append(tokens, token{typ: tokenNot})
		default:
			tokens = append(tokens, token{typ: tokenIdent, val: word})
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
		case tokenIdent, tokenDateRange:
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
// Поддерживаются операторы AND, OR, NOT, круглые скобки
// и DATE(YYYY-MM-DD,YYYY-MM-DD) для поиска по диапазону дат
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

		case tokenDateRange:
			// парсим "YYYY-MM-DD,YYYY-MM-DD" и получаем битмап документов в диапазоне
			from, to, err := parseDateRange(tok.val)
			if err != nil {
				return nil, fmt.Errorf("invalid DATE range: %w", err)
			}
			bm, err := idx.GetDateBitmap(from, to)
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
