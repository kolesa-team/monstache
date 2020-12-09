package internal

// Здесь находится костыль для исправления ситуации,
// когда после перезапуска monstache он не отправляет на индексацию документы,
// обновлённые в mongodb после его остановки.

import (
	"go.mongodb.org/mongo-driver/bson/primitive"
	"log"
	"sync"
	"time"
)

// Структура для определения даты, с которой нужно начинать чтение оплога
type OplogResumeTs struct {
	// сколько шардов mongodb есть в кластере
	shardsTotal int
	// из скольки шардов уже получена дата последней проиндексированной операции
	shardsQueried int
	// минимальная дата последней проиндексированной операции
	// среди всех шардов в кластере
	ts         primitive.Timestamp
	m          sync.Mutex
	resultChan chan primitive.Timestamp
	logger     *log.Logger
}

// Создаёт новую структуру, подготавливая ее для использования с заданным числом шардов
func NewOplogResumeTs(shardCount int, logger *log.Logger) OplogResumeTs {
	return OplogResumeTs{
		shardsTotal: shardCount,
		resultChan:  make(chan primitive.Timestamp, shardCount),
		logger:      logger,
	}
}

// Возвращает канал, из которого можно получить дату, с которой нужно начать чтение оплога. В этот канал ответ будет отправлен только после того, как функция GetResumeTimestamp была вызвана для всех доступных шардов.
// Принимает функцию, которая вычисляет дату последней проиндексированной операции в одном из шардов.
//
func (oplogResume *OplogResumeTs) GetResumeTimestamp(cb func() primitive.Timestamp) chan primitive.Timestamp {
	oplogResume.m.Lock()
	defer oplogResume.m.Unlock()

	// значение уже посчитано, просто вернём его в новом канале
	if oplogResume.shardsQueried == oplogResume.shardsTotal {
		oplogResume.logger.Printf(
			"Oplog resume timestamp is %s",
			(time.Unix(int64(oplogResume.ts.T), 0)).Format(time.RFC3339),
		)

		tmpResultChan := make(chan primitive.Timestamp, 1)
		tmpResultChan <- oplogResume.ts

		return tmpResultChan
	}

	oplogResume.shardsQueried++

	currentTs := cb()

	// если вычисленная дата последней операции меньше текущей,
	// то запомним её
	if oplogResume.ts.T == 0 || oplogResume.ts.T > currentTs.T {
		oplogResume.logger.Printf(
			"Oplog resume timestamp is updated with value %s",
			(time.Unix(int64(currentTs.T), 0)).Format(time.RFC3339),
		)
		oplogResume.ts = currentTs
	}

	// если функция вызвана столько раз, сколько у нас шардов, значит мы можем отправить финальную дату в канал с ответом
	if oplogResume.shardsQueried == oplogResume.shardsTotal {
		oplogResume.logger.Printf(
			"Final oplog resume timestamp calculated: %s", (time.Unix(int64(currentTs.T), 0)).Format(time.RFC3339),
		)

		for i := 0; i < oplogResume.shardsTotal; i++ {
			oplogResume.resultChan <- oplogResume.ts
		}
	}

	return oplogResume.resultChan
}
