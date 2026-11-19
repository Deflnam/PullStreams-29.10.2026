#include <queue>
#include <deque>
#include <vector>
#include <memory>
#include <thread>
#include <mutex>
#include <shared_mutex>
#include <condition_variable>
#include <functional>
#include <stdexcept>
#include <atomic>

struct th_pool_t {
  using f_t = std::function<void(void)>;

  static constexpr size_t BATCH_SIZE = 8;

  explicit th_pool_t(unsigned int k);
  ~th_pool_t();

  void submit(f_t f);
  void wait();
  void pause();
  void play();
  void stop();

private:
  std::queue<f_t> global_queue_;
  std::shared_mutex global_mx_;
  std::vector<std::deque<f_t>> tasks_;
  std::vector<std::unique_ptr<std::shared_mutex>> mxs_;
  std::mutex wait_mx_;
  std::condition_variable cd_;
  std::condition_variable wait_cd_;
  std::atomic<bool> pause_{false};
  std::atomic<bool> stop_{false};
  std::atomic<unsigned int> active_tasks_{0};
  std::vector<std::thread> ths_;

  void execute(size_t thread_id);
  bool fetchBatch(size_t thread_id, f_t &task_out);
  bool trySteal(size_t thread_id, f_t &task_out);
  bool hasPendingTasks();
};

th_pool_t::th_pool_t(unsigned int k) {
  if (k == 0) {
    throw std::invalid_argument("Pool size must be greater than 0");
  }
  tasks_.resize(k);
  mxs_.reserve(k);
  for (unsigned int i = 0; i < k; ++i) {
    mxs_.push_back(std::make_unique<std::shared_mutex>());
  }
  ths_.reserve(k);
  for (unsigned int i = 0; i < k; ++i) {
    ths_.emplace_back([this, i]() {
      execute(i);
    });
  }
}

th_pool_t::~th_pool_t() {
  stop();
  for (auto && th : ths_) {
    if (th.joinable()) {
      th.join();
    }
  }
}

void th_pool_t::submit(f_t f) {
  {
    std::unique_lock<std::shared_mutex> wl(global_mx_);
    if (stop_) {
      throw std::runtime_error("submit on stopped pool");
    }
    global_queue_.push(std::move(f));
  }
  {
    std::lock_guard<std::mutex> lk(wait_mx_);
  }
  cd_.notify_all();
}

bool th_pool_t::hasPendingTasks() {
  {
    std::shared_lock<std::shared_mutex> rl(global_mx_);
    if (!global_queue_.empty()) return true;
  }
  for (size_t i = 0; i < tasks_.size(); ++i) {
    std::shared_lock<std::shared_mutex> rl(*mxs_[i]);
    if (!tasks_[i].empty()) return true;
  }
  return false;
}

void th_pool_t::wait() {
  std::unique_lock<std::mutex> lck(wait_mx_);
  wait_cd_.wait(lck, [this]() {
    return stop_ || (active_tasks_ == 0 && !hasPendingTasks());
  });
}

void th_pool_t::pause() {
  std::lock_guard<std::mutex> lk(wait_mx_);
  pause_ = true;
}

void th_pool_t::play() {
  {
    std::lock_guard<std::mutex> lk(wait_mx_);
    pause_ = false;
  }
  cd_.notify_all();
}

void th_pool_t::stop() {
  {
    std::unique_lock<std::shared_mutex> wl(global_mx_);
    std::lock_guard<std::mutex> lk(wait_mx_);
    stop_ = true;
  }
  cd_.notify_all();
  wait_cd_.notify_all();
}

bool th_pool_t::fetchBatch(size_t thread_id, f_t &task_out) {
  {
    std::shared_lock<std::shared_mutex> rl(global_mx_);
    if (global_queue_.empty()) return false;
  }

  std::unique_lock<std::shared_mutex> g_wl(global_mx_, std::defer_lock);
  std::unique_lock<std::shared_mutex> l_wl(*mxs_[thread_id], std::defer_lock);
  std::lock(g_wl, l_wl);

  if (global_queue_.empty()) return false;

  active_tasks_++;
  task_out = std::move(global_queue_.front());
  global_queue_.pop();

  size_t count = 1;
  while (count < BATCH_SIZE && !global_queue_.empty()) {
    tasks_[thread_id].push_back(std::move(global_queue_.front()));
    global_queue_.pop();
    ++count;
  }
  return true;
}

bool th_pool_t::trySteal(size_t thread_id, f_t &task_out) {
  const size_t num_queues = tasks_.size();
  for (size_t i = 1; i < num_queues; ++i) {
    const size_t victim_id = (thread_id + i) % num_queues;
    {
      std::shared_lock<std::shared_mutex> rl(*mxs_[victim_id]);
      if (tasks_[victim_id].size() < 2) continue;
    }

    std::unique_lock<std::shared_mutex> v_wl(*mxs_[victim_id], std::defer_lock);
    std::unique_lock<std::shared_mutex> t_wl(*mxs_[thread_id], std::defer_lock);
    std::lock(v_wl, t_wl);

    if (tasks_[victim_id].size() < 2) continue;

    active_tasks_++;
    size_t steal_count = tasks_[victim_id].size() / 2;

    task_out = std::move(tasks_[victim_id].back());
    tasks_[victim_id].pop_back();

    for (size_t j = 1; j < steal_count; ++j) {
      tasks_[thread_id].push_back(std::move(tasks_[victim_id].back()));
      tasks_[victim_id].pop_back();
    }
    return true;
  }
  return false;
}

void th_pool_t::execute(size_t thread_id) {
  while (!stop_) {
    if (pause_) {
      std::unique_lock<std::mutex> lck(wait_mx_);
      cd_.wait(lck, [this]() { return stop_ || !pause_; });
      continue;
    }

    f_t task{};
    bool found = false;

    {
      std::unique_lock<std::shared_mutex> wl(*mxs_[thread_id]);
      if (!tasks_[thread_id].empty()) {
        active_tasks_++;
        task = std::move(tasks_[thread_id].front());
        tasks_[thread_id].pop_front();
        found = true;
      }
    }

    if (!found) {
      found = fetchBatch(thread_id, task);
    }

    if (!found) {
      found = trySteal(thread_id, task);
    }

    if (found) {
      try {
        task();
      } catch (...) {
      }

      if (--active_tasks_ == 0) {
        std::lock_guard<std::mutex> lck(wait_mx_);
        wait_cd_.notify_all();
      }
    } else {
      std::unique_lock<std::mutex> lck(wait_mx_);
      cd_.wait(lck, [this]() {
        return stop_ || (!pause_ && hasPendingTasks());
      });
    }
  }
}