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
	uint64_t getTime() {
		return time;
	}
	int getValue() {
		return value;
	}
private:
	uint64_t time;
	int value;
};

auto cmp = [](LogItem first, LogItem second) { return first.getTime() < second.getTime(); };

class RingBuffer {
public:
	RingBuffer(): // buffer(new LogItem[MAX_SIZE]), curSize(0), headPos(0), tailPos(0),
	minHeap(priority_queue<LogItem, vector<LogItem>, decltype(cmp)> (cmp)) {
	};
	LogItem pop_front(SpinLock& lock) {
		if (minHeap.size() == 0) {
			return LogItem(-1,-1);
		}
		LogItem ret;
		lock.lock();
//		curSize--;
//		ret = &buffer[headPos];
//		headPos = (headPos - 1) < 0 ? headPos + MAX_SIZE - 1 :  headPos - 1;
		ret = (minHeap.top());
		minHeap.pop();
		lock.unlock();
		return ret;
	}

	void push_back(SpinLock& lock, LogItem* item) {
//		cout << minHeap.size() << endl;
		if (minHeap.size() == MAX_SIZE) return;
		lock.lock();

//		if (curSize == 0 && headPos == tailPos) ;
//		else tailPos = tailPos - 1 < 0 ? tailPos + MAX_SIZE - 1 : tailPos - 1;
//
//		buffer[tailPos] = *item;
//		curSize++;
		minHeap.push(*item);
		lock.unlock();
	}

	bool empty() {
		return minHeap.size() == 0;
	}

	int getCurSize() {
		return minHeap.size();
	}
private:
	static const int MAX_SIZE = 4e6;
//	LogItem* buffer;
//	int headPos;
//	int tailPos;
//	int curSize;
	priority_queue<LogItem, vector<LogItem>, decltype(cmp)> minHeap;
};

class IntLogger {
public:
	IntLogger(string& filename, int window): window(window), stopConsumer(false) {
		outFile.open(filename);
		thread a = thread(&IntLogger::consumeLog, this);
		list.push_back(move(a));
	}

	void Log(uint64_t time, int value);
	~IntLogger() {
		stopConsumer = true;
		for (thread& i : list) i.join();
		outFile.close();
	}

	SpinLock& getSpinLock() {
		return spinLock;
	}

	ofstream& getOutFile() {
		return outFile;
	}

	int getWindow() {
		return window;
	}

	void consumeLog() {
		while (true) {
			uint64_t begin = time_now();
			LogItem item = buffer.pop_front(spinLock);

        	if (item.getTime() != -1) {
			    outFile << item.getTime() << "," << item.getValue() << endl;
        	}

			while (time_now() - begin < 1L) ;
			if (stopConsumer && buffer.empty()) break;
		}
	}

private:
	int window;
	ofstream outFile;
	SpinLock spinLock;
	vector<thread> list;
	thread consumerThread;
	RingBuffer buffer;
	bool stopConsumer;
};

void IntLogger::Log(uint64_t time, int value) {
    uint64_t start = time_now();
    buffer.push_back(spinLock, new LogItem(time, value));
}

void producer(IntLogger& logger) {
	for (int i = 0; i < 500000; ++i) {
		logger.Log(time_now(), i);
	}
}

void run() {
	string fileName = "test.csv";
	IntLogger logger(fileName, 10000000);

	thread t1(producer, ref(logger));
	thread t2(producer, ref(logger));

	t1.join();
	t2.join();
}

int main() {
    long long start = time_now();
    run();
	cout << (time_now() - start) / 1000000.0 << endl;
	return 0;
}



