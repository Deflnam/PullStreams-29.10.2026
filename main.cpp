#include <queue>
#include <deque>
#include <vector>
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
  std::vector<std::shared_mutex> mxs_;
  std::mutex wait_mx_;
  std::condition_variable cd_;
  std::condition_variable wait_cd_;
  std::atomic<bool> pause_{false};
  std::atomic<bool> stop_{false};
  std::atomic<unsigned int> active_tasks_{0};
  std::vector<std::thread> ths_;

  void execute(size_t thread_id);
  bool fetchBatch(size_t thread_id);
  bool trySteal(size_t thread_id, f_t &task_out);
};

th_pool_t::th_pool_t(unsigned int k) {
  if (k == 0) {
    throw std::invalid_argument("Pool size must be > 0");
  }
  tasks_.resize(k);
  mxs_.resize(k);
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
  if (stop_) {
    throw std::runtime_error("submit on stopped pool");
  }
  {
    std::unique_lock<std::shared_mutex> wl(global_mx_);
    global_queue_.push(std::move(f));
  }
  cd_.notify_all();
}

void th_pool_t::wait() {
  std::unique_lock<std::mutex> lck(wait_mx_);
  wait_cd_.wait(lck, [this]() {
    if (active_tasks_ != 0) return false;
    {
      std::shared_lock<std::shared_mutex> rl(global_mx_);
      if (!global_queue_.empty()) return false;
    }
    for (size_t i = 0; i < tasks_.size(); ++i) {
      std::shared_lock<std::shared_mutex> rl(mxs_[i]);
      if (!tasks_[i].empty()) return false;
    }
    return true;
  });
}

void th_pool_t::pause() {
  pause_ = true;
}

void th_pool_t::play() {
  pause_ = false;
  cd_.notify_all();
}

void th_pool_t::stop() {
  stop_ = true;
  cd_.notify_all();
  wait_cd_.notify_all();
}

bool th_pool_t::fetchBatch(size_t thread_id) {
  {
    std::shared_lock<std::shared_mutex> rl(global_mx_);
    if (global_queue_.empty()) return false;
  }
  {
    std::unique_lock<std::shared_mutex> wl(global_mx_);
    if (global_queue_.empty()) return false;
    size_t count = 0;
    while (count < BATCH_SIZE && !global_queue_.empty()) {
      tasks_[thread_id].push_back(std::move(global_queue_.front()));
      global_queue_.pop();
      ++count;
    }
  }
  return true;
}

bool th_pool_t::trySteal(size_t thread_id, f_t &task_out) {
  const size_t num_queues = tasks_.size();
  for (size_t i = 1; i < num_queues; ++i) {
    const size_t victim_id = (thread_id + i) % num_queues;
    {
      std::shared_lock<std::shared_mutex> rl(mxs_[victim_id]);
      if (tasks_[victim_id].size() < 2) continue;
    }
    {
      std::unique_lock<std::shared_mutex> wl(mxs_[victim_id]);
      if (tasks_[victim_id].size() < 2) continue;
      size_t steal_count = tasks_[victim_id].size() / 2;
      auto it = tasks_[victim_id].end();
      for (size_t j = 0; j < steal_count; ++j) --it;
      task_out = std::move(tasks_[victim_id].back());
      tasks_[victim_id].pop_back();
      for (; it != tasks_[victim_id].end(); ++it) {
        tasks_[thread_id].push_back(std::move(*it));
      }
      tasks_[victim_id].erase(it, tasks_[victim_id].end());
      return true;
    }
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
      std::unique_lock<std::shared_mutex> wl(mxs_[thread_id]);
      if (!tasks_[thread_id].empty()) {
        task = std::move(tasks_[thread_id].front());
        tasks_[thread_id].pop_front();
        found = true;
      }
    }

    if (!found) {
      if (fetchBatch(thread_id)) {
        std::unique_lock<std::shared_mutex> wl(mxs_[thread_id]);
        task = std::move(tasks_[thread_id].front());
        tasks_[thread_id].pop_front();
        found = true;
      }
    }

    if (!found) {
      found = trySteal(thread_id, task);
    }

    if (found) {
      active_tasks_++;
      try {
        task();
      } catch (...) {
      }
      active_tasks_--;

      if (active_tasks_ == 0) {
        wait_cd_.notify_all();
      }
    } else {
      std::unique_lock<std::mutex> lck(wait_mx_);
      cd_.wait(lck, [this, thread_id]() {
        return stop_ || (!pause_ && !tasks_[thread_id].empty());
      });
    }
  }
}