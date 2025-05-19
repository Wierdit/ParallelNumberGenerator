#include <pthread.h>
#include <string.h>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cstdio>
#include <iostream>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>

const char* FILENAME = "shared_data.txt";
const int NJ_MIN = 1;
const int NJ_MAX = 999999;  // Максимальное количество чисел, генерируемых потоком (10^6)
const size_t READ_BUFFER_SIZE = 16384;
const size_t MAX_TOKEN_LENGTH = 20;  // Максимальная длина числа uint64_t (2^64-1)

static std::atomic<bool> global_should_exit(false); // Флаг для аварийной остановки всех потоков

typedef struct {
  int thread_id;
  int N;
  const char* filename;
  pthread_mutex_t* file_mutex;
  pthread_cond_t* data_available_cond; // Условие для уведомления читателя о новых данных
  std::atomic<int>* writers_finished_count;
} writer_data_t;

typedef struct {
  int k; // Параметр для порога частоты (n/k)
  int N;
  const char* filename;
  pthread_mutex_t* file_mutex;
  pthread_cond_t* data_available_cond;
  std::atomic<int>* writers_finished_count;
  long long* total_elements_processed;
  std::vector<uint64_t>* result_set;
} reader_data_t;

// Генерация чисел
static std::atomic<int> seed_counter(0);

uint64_t get_random_uint64() {
  thread_local std::mt19937_64 rng(
      std::chrono::high_resolution_clock::now().time_since_epoch().count() +
      reinterpret_cast<uint64_t>(pthread_self()) + seed_counter.fetch_add(1));
  std::uniform_int_distribution<uint64_t> dist(
      1, std::numeric_limits<uint64_t>::max());
  return dist(rng);
}

int get_random_nj() {
  thread_local std::mt19937 rng(
      std::chrono::high_resolution_clock::now().time_since_epoch().count() +
      reinterpret_cast<uint64_t>(pthread_self()) + seed_counter.fetch_add(1));
  std::uniform_int_distribution<int> dist(NJ_MIN, NJ_MAX);
  return dist(rng);
}

// Конвертация числа в строку
std::string uint64_to_string(uint64_t num) {
  char buf[21];
  snprintf(buf, sizeof(buf), "%llu", (unsigned long long)num);
  std::string result = buf;
  if (result.length() > MAX_TOKEN_LENGTH) {
    fprintf(stderr, "[Писатель] Ошибка: слишком длинная строка '%s'\n",
            result.c_str());
    return "1";
  }
  return result;
}

// Писатель
void* writer_func(void* arg) {
  writer_data_t* data = (writer_data_t*)arg;

  if (global_should_exit.load()) {
    data->writers_finished_count->fetch_add(1);
    pthread_cond_broadcast(data->data_available_cond);
    return NULL;
  }

  int n_j = get_random_nj();
  std::string buffer_str;
  buffer_str.reserve(21);

  for (int i = 0; i < n_j; ++i) {
    buffer_str = uint64_to_string(get_random_uint64());
    buffer_str += ",";

    pthread_mutex_lock(data->file_mutex);
    FILE* outfile = fopen(data->filename, "a");
    if (!outfile) {
      fprintf(stderr, "[Писатель %d] Ошибка открытия '%s': %s\n",
              data->thread_id, data->filename, strerror(errno));
      data->writers_finished_count->fetch_add(1);
      pthread_cond_broadcast(data->data_available_cond);
      pthread_mutex_unlock(data->file_mutex);
      return NULL;
    }
    fprintf(outfile, "%s", buffer_str.c_str());
    fflush(outfile); // Гарантирует немедленную запись в файл
    fclose(outfile);

    pthread_cond_broadcast(data->data_available_cond); // Уведомляем читателя о новых данных
    pthread_mutex_unlock(data->file_mutex);
  }
  data->writers_finished_count->fetch_add(1);
  return NULL;
}

// Алгоритм Мисры-Гриса
void process_number(uint64_t num, std::unordered_map<uint64_t, int>& candidates,
                    size_t k_misra, long long& n) {
  if (n >= std::numeric_limits<long long>::max() - 1) {
    fprintf(stderr, "[Читатель] Переполнение счетчика чисел\n");
    global_should_exit.store(true);
    return;
  }
  n++;
  auto it = candidates.find(num);
  if (it != candidates.end()) {
    it->second++;
  } else if (candidates.size() < k_misra) {
    candidates[num] = 1; // Добавляем новое число, если есть место
  } else { // Уменьшаем счётчики всех кандидатов, удаляем с нулевым счётчиком
    for (auto it = candidates.begin(); it != candidates.end();) {
      if (--(it->second) == 0) {
        it = candidates.erase(it);
      } else {
        ++it;
      }
    }
  }
}

// Читатель
void* reader_func(void* arg) {
  reader_data_t* data = (reader_data_t*)arg;
  size_t k_misra = (data->k > 1) ? (size_t)(data->k - 1) : 1;
  std::unordered_map<uint64_t, int> candidates;
  long long n = 0; // Общее количество обработанных чисел, не используется в алгоритме

  // Открываем файл для чтения
  FILE* infile = fopen(data->filename, "r");
  if (!infile) {
    fprintf(stderr, "[Читатель] Не удалось открыть '%s': %s\n", data->filename,
            strerror(errno));
    return (void*)1;  // Возвращаем ненулевой указатель для обозначения ошибки
  }

  char buffer[READ_BUFFER_SIZE];
  std::string token;  // Читаемое число
  bool active = true; // Флаг продолжения чтения

  pthread_mutex_lock(data->file_mutex);
  while (active && !global_should_exit.load()) {
    bool writers_active = (data->writers_finished_count->load() < data->N);
    clearerr(infile);
    size_t bytes_read = fread(buffer, 1, READ_BUFFER_SIZE - 1, infile);
    buffer[bytes_read] = '\0'; // Завершаем строку

    if (bytes_read > 0) {
      for (size_t i = 0; i < bytes_read; ++i) {
        char c = buffer[i];

        if (isdigit(c)) {
          token += c;

        } else if (c == ',') {
          if (!token.empty()) {
            bool is_number = true;
            for (char ch : token) {
              if (!isdigit(ch)) {
                is_number = false;
                break;
              }
            }
            if (!is_number || token.length() > MAX_TOKEN_LENGTH) {
              fprintf(stderr, "[Читатель] Пропущен токен '%s' (не число)\n",
                      token.c_str());
            } else {
              try {
                uint64_t num = std::stoull(token);
                process_number(num, candidates, k_misra, n);
              } catch (...) {
                fprintf(stderr,
                        "[Читатель] Пропущен токен '%s' (не uint64_t)\n",
                        token.c_str());
              }
            }
            token.clear();
          }
        }
      }
    }

    if (ferror(infile)) {
      fprintf(stderr, "[Читатель] Ошибка чтения: %s\n", strerror(errno));
      active = false;
    } else if (!writers_active && bytes_read == 0) { // Все писатели закончили и данных больше нет
      active = false;
    }

    // Ждём новых данных, если писатели активны
    if (writers_active && bytes_read == 0 && active) {
      pthread_cond_wait(data->data_available_cond, data->file_mutex);
    } else if (!writers_active && !active) {
      break;
    }
  }
  pthread_mutex_unlock(data->file_mutex);

  // Закрываем файл
  if (infile) fclose(infile);
  *data->total_elements_processed = n;

  fprintf(stderr, "[Читатель] Завершено чтение, обработано чисел: %lld\n", n);

  // Проверяем корректность k < n
  if (data->k >= n && n > 0) {
    fprintf(stderr, "[Читатель] Ошибка: k=%d >= n=%lld\n", data->k, n);
    return (void*)1;  // Возвращаем ненулевой указатель для обозначения ошибки
  }

  // Формируем результат
  if (n > 0 && data->k > 1) {
    double nk = (double)n / data->k;
    double threshold = nk;
    fprintf(stderr, "[Читатель] n=%lld, threshold=%f\n", n, threshold);
    for (const auto& pair : candidates) {
      if (pair.second > threshold) {
        data->result_set->push_back(pair.first);
      }
    }
  }

  return NULL;  // Успешное завершение
}

int main(int argc, char* argv[]) {
  if (argc != 3) {
    fprintf(stderr, "Аргументы запуска: %s N k (1<=N<=50, k>1)\n", argv[0]);
    return 1;
  }

  int N = 0, k = 0;
  try {
    N = std::stoi(argv[1]);
    k = std::stoi(argv[2]);
    if (N < 1 || N > 50 || k <= 1) {
      fprintf(stderr,
              "Ошибка: N=%d должно быть в [1,50], k=%d должно быть >1\n", N, k);
      return 1;
    }
  } catch (...) {
    fprintf(stderr, "Ошибка: Неверные параметры N='%s', k='%s'\n", argv[1],
            argv[2]);
    return 1;
  }

  // Создаём пустой файл
  FILE* outfile = fopen(FILENAME, "w");
  if (!outfile) {
    fprintf(stderr, "Ошибка: Не удалось создать '%s': %s\n", FILENAME,
            strerror(errno));
    return 1;
  }
  fclose(outfile);

  // Инициализация мьютекса и условия
  pthread_mutex_t file_mutex;
  pthread_cond_t data_available_cond;
  if (pthread_mutex_init(&file_mutex, NULL) != 0 ||
      pthread_cond_init(&data_available_cond, NULL) != 0) {
    fprintf(stderr, "Ошибка инициализации синхронизации: %s\n",
            strerror(errno));
    return 1;
  }

  std::atomic<int> writers_finished_count(0);
  long long total_elements_processed = 0;
  std::vector<uint64_t> result_set;
  std::vector<writer_data_t> writer_data(N);
  reader_data_t reader_data = {k,
                               N,
                               FILENAME,
                               &file_mutex,
                               &data_available_cond,
                               &writers_finished_count,
                               &total_elements_processed,
                               &result_set};
  std::vector<pthread_t> writer_threads(N);
  pthread_t reader_thread = 0;

  // Создаём читающий поток
  if (pthread_create(&reader_thread, NULL, reader_func, &reader_data) != 0) {
    fprintf(stderr, "Ошибка: Не удалось создать читатель: %s\n",
            strerror(errno));
    pthread_mutex_destroy(&file_mutex);
    pthread_cond_destroy(&data_available_cond);
    return 1;
  }

  // Создаём пишущие потоки
  for (int i = 0; i < N; ++i) {
    writer_data[i] = {i + 1,
                      N,
                      FILENAME,
                      &file_mutex,
                      &data_available_cond,
                      &writers_finished_count};
    if (pthread_create(&writer_threads[i], NULL, writer_func,
                       &writer_data[i]) != 0) {
      fprintf(stderr, "Ошибка: Не удалось создать писатель %d: %s\n", i + 1,
              strerror(errno));
      global_should_exit.store(true);
      pthread_mutex_lock(&file_mutex);
      pthread_cond_broadcast(&data_available_cond);
      pthread_mutex_unlock(&file_mutex);
      break;
    }
  }

  // Ожидаем завершения пишущих потоков
  for (int i = 0; i < N; ++i) {
    pthread_join(writer_threads[i], NULL);
  }
  pthread_mutex_lock(&file_mutex);
  pthread_cond_broadcast(&data_available_cond);
  pthread_mutex_unlock(&file_mutex);

  // Ожидаем завершения читающего потока и проверяем результат
  void* reader_result;
  pthread_join(reader_thread, &reader_result);
  if (reader_result != NULL) {
    fprintf(stderr, "Ошибка: Читающий поток завершился с ошибкой\n");
    pthread_mutex_destroy(&file_mutex);
    pthread_cond_destroy(&data_available_cond);
    return 1;  // Завершаем программу с ошибкой
  }

  // Выводим результат
  std::sort(result_set.begin(), result_set.end());
  printf("{");
  bool first = true;
  for (uint64_t val : result_set) {
    if (!first) printf(", ");
    printf("%llu", (unsigned long long)val);
    first = false;
  }
  printf("}\n");

  // Очистка ресурсов
  pthread_mutex_destroy(&file_mutex);
  pthread_cond_destroy(&data_available_cond);
  return 0;
}