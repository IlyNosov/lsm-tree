package index

import (
	"strings"
	"unicode"

	"github.com/kljensen/snowball"
)

// Стоп-слова для английского
var stopWordsEn = map[string]bool{
	"the": true, "a": true, "an": true, "and": true, "or": true, "not": true,
	"in": true, "on": true, "at": true, "to": true, "for": true, "of": true,
	"with": true, "by": true, "from": true, "as": true, "is": true, "are": true,
	"was": true, "were": true, "be": true, "been": true, "being": true,
	"have": true, "has": true, "had": true, "do": true, "does": true, "did": true,
	"but": true, "if": true, "else": true, "when": true,
	"up": true, "down": true, "out": true, "over": true, "under": true,
	"again": true, "further": true, "then": true, "once": true,
}

// Стоп-слова для русского
var stopWordsRu = map[string]bool{
	"и": true, "в": true, "во": true, "не": true, "что": true, "он": true, "на": true,
	"я": true, "с": true, "со": true, "как": true, "а": true, "то": true, "все": true,
	"она": true, "так": true, "его": true, "но": true, "да": true, "ты": true, "к": true,
	"у": true, "же": true, "вы": true, "за": true, "бы": true, "по": true, "только": true,
	"ее": true, "мне": true, "было": true, "вот": true, "от": true, "меня": true, "еще": true,
	"нет": true, "о": true, "из": true, "ему": true, "теперь": true, "когда": true,
	"даже": true, "ну": true, "вдруг": true, "ли": true, "если": true, "уже": true,
	"или": true, "быть": true, "был": true, "него": true, "до": true, "вас": true,
	"нибудь": true, "опять": true, "уж": true, "вам": true, "ведь": true, "там": true,
	"потом": true, "себя": true, "ничего": true, "ей": true, "может": true, "они": true,
	"тут": true, "где": true, "есть": true, "надо": true, "ней": true, "для": true,
	"мы": true, "тебя": true, "их": true, "чем": true, "была": true, "сам": true,
	"чтоб": true, "без": true, "будто": true, "чего": true, "раз": true, "тоже": true,
	"себе": true, "под": true, "будет": true, "ж": true, "тогда": true, "кто": true,
	"этот": true, "того": true, "потому": true, "этого": true, "какой": true,
	"совсем": true, "ним": true, "здесь": true, "этом": true, "один": true,
	"почти": true, "мой": true, "тем": true, "чтобы": true, "нее": true,
}

// normalizeWord приводит слово к нормальной форме с учетом языка
func normalizeWord(word, lang string) (string, bool) {
	word = strings.ToLower(word)
	word = strings.TrimFunc(word, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsNumber(r)
	})
	if word == "" {
		return "", false
	}
	// Проверка на стоп-слово в зависимости от языка
	if (lang == "en" && stopWordsEn[word]) || (lang == "ru" && stopWordsRu[word]) {
		return "", false
	}
	// Стемминг
	var stemmed string
	var err error
	switch lang {
	case "en":
		stemmed, err = snowball.Stem(word, "english", true)
	case "ru":
		stemmed, err = snowball.Stem(word, "russian", true)
	default:
		// Если язык не поддерживается, возвращаем исходное слово
		return word, true
	}
	if err != nil {
		// В случае ошибки возвращаем исходное слово
		return word, true
	}
	return stemmed, true
}

// tokenize разбивает текст на слова
func tokenize(text string) []string {
	return strings.FieldsFunc(text, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsNumber(r)
	})
}

// detectLanguage пытается определить язык текста по первому слову
// если первый символ кириллица - русский, иначе английский
func detectLanguage(text string) string {
	for _, r := range text {
		if unicode.Is(unicode.Cyrillic, r) {
			return "ru"
		}
		if unicode.IsLetter(r) {
			break
		}
	}
	return "en"
}
