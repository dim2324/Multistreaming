package main

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"
)

// Job представляет задание для обработки
type Job struct {
	ID  int
	URL string
}

// Result представляет результат обработки задания
type Result struct {
	Job      Job
	Status   string
	Duration time.Duration
}

// worker выполняет задания из канала jobs и отправляет результаты в results
func worker(id int, jobs <-chan Job, results chan<- Result, wg *sync.WaitGroup) {
	defer wg.Done()

	for job := range jobs {
		// Имитация HTTP-запроса со случайной задержкой
		start := time.Now()
		randomDuration := time.Duration(rand.Intn(1000)+100) * time.Millisecond
		time.Sleep(randomDuration)
		elapsed := time.Since(start)

		// Определяем статус на основе случайного фактора
		status := "успешно"
		if rand.Float64() < 0.1 { // 10% шанс ошибки
			status = "ошибка"
		}

		// Отправляем результат
		results <- Result{
			Job:      job,
			Status:   status,
			Duration: elapsed,
		}

		fmt.Printf("Воркер %d обработал URL: %s (статус: %s, время: %v)\n",
			id, job.URL, status, elapsed)
	}
}

func main() {
	// Инициализация генератора случайных чисел
	rand.Seed(time.Now().UnixNano())

	// Список URL для обработки
	urls := []string{
		"https://yandex.ru/page1",
		"https://google.com/page2",
		"https://mail.ru/page3",
		"https://rambler.ru/page4",
		"https:///netology/page5",
		"https://rbc.ru/page6",
		"https://dzen.ru/page7",
		"https://vk.com/page8",
		"https://github.com/page9",
		"https://example.com/page10",
	}

	// Количество воркеров в пуле
	numWorkers := 5

	// Создание каналов для заданий и результатов
	// Буферизированные каналы помогают избежать блокировок
	jobs := make(chan Job, len(urls))
	results := make(chan Result, len(urls))

	// WaitGroup для ожидания завершения всех воркеров
	var wg sync.WaitGroup

	// Запуск пула воркеров (Fan-out)
	fmt.Printf("Запуск %d воркеров...\n", numWorkers)
	for i := 1; i <= numWorkers; i++ {
		wg.Add(1)
		go worker(i, jobs, results, &wg)
	}

	// Отправка заданий в канал jobs
	fmt.Println("Отправка заданий воркерам...")
	for i, url := range urls {
		jobs <- Job{
			ID:  i + 1,
			URL: url,
		}
	}
	close(jobs) // Закрываем канал, сигнализируя об окончании заданий

	// Запуск горутины для ожидания завершения всех воркеров и закрытия канала результатов
	go func() {
		wg.Wait()
		close(results) // Закрываем канал результатов после завершения всех воркеров
	}()

	// Сбор результатов (Fan-in)
	fmt.Println("\nСбор результатов...")
	var allResults []Result
	successfulCount := 0
	errorCount := 0
	totalDuration := time.Duration(0)

	for result := range results {
		allResults = append(allResults, result)

		// Подсчет статистики
		if result.Status == "успешно" {
			successfulCount++
		} else {
			errorCount++
		}
		totalDuration += result.Duration
	}

	// Вывод итогового отчета
	fmt.Println("\n" + strings.Repeat("=", 50))
	fmt.Println("ИТОГОВЫЙ ОТЧЕТ")
	fmt.Println(strings.Repeat("=", 50))

	fmt.Println("\nДетальная информация по каждому URL:")
	fmt.Println(strings.Repeat("-", 50))
	for _, result := range allResults {
		fmt.Printf("ID: %d | URL: %-35s | Статус: %-10s | Время: %v\n",
			result.Job.ID, result.Job.URL, result.Status, result.Duration)
	}

	fmt.Println("\n" + strings.Repeat("-", 50))
	fmt.Println("ОБЩАЯ СТАТИСТИКА:")
	fmt.Printf("Всего обработано URL: %d\n", len(allResults))
	fmt.Printf("Успешно: %d (%.1f%%)\n",
		successfulCount,
		float64(successfulCount)/float64(len(allResults))*100)
	fmt.Printf("С ошибками: %d (%.1f%%)\n",
		errorCount,
		float64(errorCount)/float64(len(allResults))*100)

	if len(allResults) > 0 {
		avgDuration := totalDuration / time.Duration(len(allResults))
		fmt.Printf("Среднее время обработки: %v\n", avgDuration)
		fmt.Printf("Общее время обработки: %v\n", totalDuration)
	}

	fmt.Println("\nПрограмма успешно завершена!")
}
