#include <queue>
#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>

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
    void execute();
    std::queue<f_t> tasks_;
    std::mutex mx_;
    std::condition_variable cd_;
    std::condition_variable wait_cd_;
    bool pause_{false};
    bool stop_{false};
    unsigned int active_tasks_{0};
    std::vector<std::thread> ths_;
};

void th_pool_t::submit(f_t f) {
    mx_.lock(); tasks_.push(std::move(f)); mx_.unlock();
    cd_.notify_one();
}

th_pool_t::th_pool_t(unsigned int k) {
    auto thf = [this]() { execute(); };
    for (; k; --k) ths_.emplace_back(thf);
}

th_pool_t::~th_pool_t() {
    stop();
    for (auto && th : ths_) {
        if (th.joinable()) th.join();
    }
}

void th_pool_t::wait() {
    std::unique_lock<std::mutex> lck(mx_);
    wait_cd_.wait(lck, [this]() {
        return tasks_.empty() && active_tasks_ == 0;
    });
}

void th_pool_t::pause() {
    std::lock_guard<std::mutex> lck(mx_);
    pause_ = true;
}

void th_pool_t::play() {
    {
        std::lock_guard<std::mutex> lck(mx_);
        pause_ = false;
    }
    cd_.notify_all();
}


void th_pool_t::stop() {
    {
        std::lock_guard<std::mutex> lck(mx_);
        stop_ = true;
    }
    cd_.notify_all();

}

void th_pool_t::execute() {
    std::unique_lock lck(mx_);
    while (true) {
        cd_.wait(lck, [this]() {
            return stop_ || (!pause_ && !tasks_.empty());
        });

        if (stop_) break;

        if (pause_) continue;

        auto task = std::move(tasks_.front());
        tasks_.pop();
        active_tasks_++;
        lck.unlock();
        try { task(); } catch (...) {}
        lck.lock();
        active_tasks_--;
        if (tasks_.empty() && active_tasks_ == 0) {
            wait_cd_.notify_all();
        }
    }
}