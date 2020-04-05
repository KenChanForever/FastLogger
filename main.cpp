#include<fstream>
#include<iostream>
#include<thread>
#include<cstdint>
#include<chrono>
#include<mutex>
#include<future>
#include<queue>
#include<vector>

using namespace std;

uint64_t time_now() {
	return std::chrono::high_resolution_clock::now().time_since_epoch() / std::chrono::microseconds(1);
}

class SpinLock {
    std::atomic_flag locked = ATOMIC_FLAG_INIT ;
public:
    void lock() {
        while (locked.test_and_set(memory_order_acquire)) ;
    }
    void unlock() {
        locked.clear(memory_order_release);
    }
};

class LogItem {
public:
	LogItem(uint64_t time, int value): time(time), value(value) {;}
	LogItem() {;}
	uint64_t get_time() const {
		return time;
	}
	int get_value() const {
		return value;
	}
private:
	uint64_t time;
	int value;
};

class RingBuffer {
public:
	RingBuffer(): cur_size(0), tail_pos(0x7fffffff) {
	};

	void move_tail_to_non_empty_bucket() {
		while (buffer[tail_pos].size() == 0) tail_pos = (tail_pos + 1) % MAX_SIZE;
	}
	LogItem pop_front(SpinLock& lock) {
		if (cur_size == 0) {
			return LogItem(-1,-1);
		}
		LogItem ret;
		lock.lock();
		cur_size--;
		move_tail_to_non_empty_bucket();
		ret = buffer[tail_pos].front();
		buffer[tail_pos].pop();
		lock.unlock();
		return ret;
	}

	void push_back(SpinLock& lock, const LogItem& item) {
		lock.lock();
		int index = item.get_time() % MAX_SIZE;
		if (cur_size != 0) move_tail_to_non_empty_bucket();

		if (cur_size == 0) tail_pos = index;
		else if (buffer[tail_pos].front().get_time() > item.get_time()) tail_pos = index;

		buffer[index].push(item);
		cur_size++;
		lock.unlock();
	}

	bool empty() {
		return cur_size == 0;
	}

	int getCurSize() const {
		return cur_size;
	}
private:
	static const int MAX_SIZE = 1e5;
	queue<LogItem> buffer[MAX_SIZE];
	int tail_pos;
	int cur_size;
};

class IntLogger {
public:
	IntLogger(string filename, int window): window(window), stop_consumer(false) {
		out_file.open(filename);
		consumer_thread = thread(&IntLogger::consume_log, this);
	}

	void Log(uint64_t time, int value);
	~IntLogger() {
		stop_consumer = true;
		consumer_thread.join();
		out_file.close();
	}

	int get_window() const {
		return window;
	}

	void consume_log() {
		while (true) {
			uint64_t begin = time_now();
			LogItem item = buffer.pop_front(spinLock);

        	if (item.get_time() != -1) {
			    out_file << item.get_time() << "," << item.get_value() << '\n';
        	}

			while (time_now() - begin < 1L) ;
			if (stop_consumer && buffer.empty()) break;
		}
	}

private:
	int window;
	ofstream out_file;
	SpinLock spinLock;
	thread consumer_thread;
	RingBuffer buffer;
	bool stop_consumer;
};

void IntLogger::Log(uint64_t time, int value) {
    uint64_t start = time_now();
    buffer.push_back(spinLock, LogItem(time, value));
}

void producer(IntLogger& logger) {
	for (int i = 0; i < 1000000 / 2; ++i) {
		logger.Log(time_now(), i);
	}
}

void app(IntLogger& logger) {
}

void test_run() {
	string file_name = "test.csv";
	IntLogger logger(file_name, 100000);
    long long start = time_now();
	thread t1(producer, ref(logger));
	thread t2(producer, ref(logger));
	t1.join();
	t2.join();
	long long end = time_now();

	// 2 thread(each 500k records) total time = 1.23
	// 1 thread(1m records) total time = 1.17
	cout << (end - start) / 1e6 << "us" << endl;
	cout << "end" << endl;
}

int main() {
    test_run();
	return 0;
}



