package bloom

import (
	"encoding/binary"
	"hash/fnv"
	"math"
)

type BloomFilter struct {
	bits []byte
	m    uint64 // кол-во бит
	k    uint64 // кол-во хэш-функций
}

func New(n uint64, fp float64) *BloomFilter {
	// n - ожидаемое количество ключей
	// fp - желаемая вероятность ложного срабатывания
	// m = -(n * ln(fp)) / (ln(2)^2)
	// k = (m/n) * ln(2)
	if n == 0 {
		n = 1
	}
	if fp <= 0 || fp >= 1 {
		fp = 0.01 // поставим значение по-умолчанию
	}
	mFloat := -1 * float64(n) * math.Log(fp) / (math.Ln2 * math.Ln2)
	m := uint64(math.Ceil(mFloat)) // округляем вверх

	kFloat := (float64(m) / float64(n)) * math.Ln2
	k := uint64(math.Ceil(kFloat)) // округляем вверх

	byteSize := (m + 7) / 8

	return &BloomFilter{
		bits: make([]byte, byteSize),
		m:    m,
		k:    k,
	}
}

// Add добавляет ключ в фильтр
// Используем двойное хеширование
func (bf *BloomFilter) Add(key string) {
	h1, h2 := hashKey(key)

	for i := uint64(0); i < bf.k; i++ {
		combined := h1 + i*h2
		bitIndex := combined % bf.m
		bf.setBit(bitIndex)
	}
}

// MightContain проверяет, может ли вообще ключ присутствовать
// Если один из k-битов равен 0, значит ключа точно нет
// Если все равны 1, значит ключ может быть
func (bf *BloomFilter) MightContain(key string) bool {
	h1, h2 := hashKey(key)

	for i := uint64(0); i < bf.k; i++ {
		combined := h1 + i*h2
		bitIndex := combined % bf.m
		if !bf.getBit(bitIndex) {
			return false
		}
	}

	return true
}

// setBit устанавливает бит по индексу
func (bf *BloomFilter) setBit(index uint64) {
	byteIndex := index / 8
	bitOffset := index % 8

	bf.bits[byteIndex] |= (1 << bitOffset)
}

// getBit проверяет установлен ли бит
func (bf *BloomFilter) getBit(index uint64) bool {
	byteIndex := index / 8
	bitOffset := index % 8

	return (bf.bits[byteIndex] & (1 << bitOffset)) != 0
}

func hashKey(key string) (uint64, uint64) {
	// Первый хеш
	h1 := fnv.New64a()
	h1.Write([]byte(key))
	sum1 := h1.Sum64()

	// Второй хеш (добавляем небольшой salt)
	h2 := fnv.New64()
	h2.Write([]byte("salt"))
	h2.Write([]byte(key))
	sum2 := h2.Sum64()

	// Если вдруг h2 получился 0 — избегаем проблем
	if sum2 == 0 {
		sum2 = 1
	}

	return sum1, sum2
}

// Serialize возвращает байтовое представление фильтра
func (bf *BloomFilter) Serialize() []byte {
	buf := make([]byte, 16+len(bf.bits))

	binary.LittleEndian.PutUint64(buf[0:8], bf.m)
	binary.LittleEndian.PutUint64(buf[8:16], bf.k)
	copy(buf[16:], bf.bits)

	return buf
}

// Deserialize восстанавливает фильтр из байтов
func Deserialize(data []byte) *BloomFilter {
	m := binary.LittleEndian.Uint64(data[0:8])
	k := binary.LittleEndian.Uint64(data[8:16])

	bits := make([]byte, len(data)-16)
	copy(bits, data[16:])

	return &BloomFilter{
		bits: bits,
		m:    m,
		k:    k,
	}
}
