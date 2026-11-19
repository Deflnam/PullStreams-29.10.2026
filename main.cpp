#include <queue>
#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <stdexcept>
#include <atomic>

struct th_pool_t {
  using f_t = std::function<void(void)>;

  explicit th_pool_t(unsigned int k);
  ~th_pool_t();

  void submit(f_t f);
  void wait();
  void pause();
  void play();
  void stop();

private:
  std::vector<std::queue<f_t>> tasks_;
  std::vector<std::mutex> mxs_;
  std::mutex wait_mx_;
  std::condition_variable cd_;
  std::condition_variable wait_cd_;
  std::atomic<bool> pause_{false};
  std::atomic<bool> stop_{false};
  std::atomic<unsigned int> active_tasks_{0};
  std::vector<std::thread> ths_;
  std::atomic<size_t> submit_idx_{0};

  void execute(size_t thread_id);
  bool trySteal(size_t thread_id, f_t & task_out);
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
  const size_t target = submit_idx_ % tasks_.size();
  submit_idx_++;
  {
    std::lock_guard<std::mutex> lck(mxs_[target]);
    tasks_[target].push(std::move(f));
  }
  cd_.notify_all();
}

void th_pool_t::wait() {
  std::unique_lock<std::mutex> lck(wait_mx_);
  wait_cd_.wait(lck, [this]() {
    if (active_tasks_ != 0) {
      return false;
    }
    for (size_t i = 0; i < tasks_.size(); ++i) {
      std::lock_guard<std::mutex> queue_lck(mxs_[i]);
      if (!tasks_[i].empty()) {
        return false;
      }
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

void th_pool_t::execute(size_t thread_id) {
  while (!stop_) {
    f_t task{};
    bool found = false;

    {
      std::lock_guard<std::mutex> lck(mxs_[thread_id]);
      if (!tasks_[thread_id].empty()) {
        task = std::move(tasks_[thread_id].front());
        tasks_[thread_id].pop();
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

bool th_pool_t::trySteal(size_t thread_id, f_t & task_out) {
  const size_t num_queues = tasks_.size();
  for (size_t i = 1; i < num_queues; ++i) {
    const size_t victim_id = (thread_id + i) % num_queues;
    std::lock_guard<std::mutex> lck(mxs_[victim_id]);
    if (!tasks_[victim_id].empty()) {
      task_out = std::move(tasks_[victim_id].front());
      tasks_[victim_id].pop();
      return true;
    }
  }
  return false;
}