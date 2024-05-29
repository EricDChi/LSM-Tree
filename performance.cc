#include <iostream>
#include <cstdint>
#include <string>
#include <assert.h>

#include "test.h"

class PerformanceTest : public Test
{
private:
	const uint64_t SIMPLE_TEST_MAX = 512;
	const uint64_t LARGE_TEST_MAX = 1024 * 2;
	const uint64_t GC_TEST_MAX = 1024 * 48;

	void regular_test(uint64_t max)
	{
		uint64_t i;
        uint64_t key_size = sizeof(uint64_t);
        std::vector<double> put_latencies, get_latencies, del_latencies, scan_latencies;
        std::chrono::time_point<std::chrono::high_resolution_clock> start, end;

        // Test a single key
        start = std::chrono::high_resolution_clock::now();
        store.put(1, "SE");
        end = std::chrono::high_resolution_clock::now();
        put_latencies.push_back(std::chrono::duration<double, std::micro>(end - start).count());

        start = std::chrono::high_resolution_clock::now();
        store.get(1);
        end = std::chrono::high_resolution_clock::now();
        get_latencies.push_back(std::chrono::duration<double, std::micro>(end - start).count());

        start = std::chrono::high_resolution_clock::now();
        store.del(1);
        end = std::chrono::high_resolution_clock::now();
        del_latencies.push_back(std::chrono::duration<double, std::micro>(end - start).count());

        // Test multiple key-value pairs
        for (i = 0; i < max; ++i)
        {
            start = std::chrono::high_resolution_clock::now();
            store.put(i, std::string(i + 1, 's'));
            end = std::chrono::high_resolution_clock::now();
            put_latencies.push_back(std::chrono::duration<double, std::micro>(end - start).count());
        }

        // Measure Get latencies
        for (i = 0; i < max; ++i)
        {
            start = std::chrono::high_resolution_clock::now();
            store.get(i);
            end = std::chrono::high_resolution_clock::now();
            get_latencies.push_back(std::chrono::duration<double, std::micro>(end - start).count());
        }

        // Measure Scan latencies
        std::list<std::pair<uint64_t, std::string>> list_ans;
        std::list<std::pair<uint64_t, std::string>> list_stu;

        for (i = 0; i < max / 2; ++i)
        {
            list_ans.emplace_back(std::make_pair(i, std::string(i + 1, 's')));
        }

        start = std::chrono::high_resolution_clock::now();
        store.scan(0, max / 2 - 1, list_stu);
        end = std::chrono::high_resolution_clock::now();
        scan_latencies.push_back(std::chrono::duration<double, std::micro>(end - start).count());

        // Test deletions and measure Delete latencies
        for (i = 0; i < max; i += 2)
        {
            start = std::chrono::high_resolution_clock::now();
            store.del(i);
            end = std::chrono::high_resolution_clock::now();
            del_latencies.push_back(std::chrono::duration<double, std::micro>(end - start).count());
        }

        for (i = 1; i < max; ++i)
        {
            start = std::chrono::high_resolution_clock::now();
            store.del(i);
            end = std::chrono::high_resolution_clock::now();
            del_latencies.push_back(std::chrono::duration<double, std::micro>(end - start).count());
        }

        // Report latencies and throughput
        auto average_latency = [](const std::vector<double>& latencies) {
            double sum = 0;
            for (const auto& latency : latencies) sum += latency;
            return sum / latencies.size();
        };

        auto throughput = [](const std::vector<double>& latencies) {
            double total_time = 0;
            for (const auto& latency : latencies) total_time += latency;
            return latencies.size() * 1e6 / total_time; // Operations per second
        };

        std::cout << "Average PUT latency: " << average_latency(put_latencies) << " microseconds\n";
        std::cout << "PUT throughput: " << throughput(put_latencies) << " ops/sec\n";
        
        std::cout << "Average GET latency: " << average_latency(get_latencies) << " microseconds\n";
        std::cout << "GET throughput: " << throughput(get_latencies) << " ops/sec\n";
        
        std::cout << "Average DELETE latency: " << average_latency(del_latencies) << " microseconds\n";
        std::cout << "DELETE throughput: " << throughput(del_latencies) << " ops/sec\n";
        
        std::cout << "Average SCAN latency: " << average_latency(scan_latencies) << " microseconds\n";
        std::cout << "SCAN throughput: " << throughput(scan_latencies) << " ops/sec\n";
	}

	void cache_test(uint64_t max)
	{
		uint64_t i;
        uint64_t key_size = sizeof(uint64_t);
        std::vector<double> get_latencies, get_without_cache_latencies, get_without_bloomfilter_latencies;
        std::chrono::time_point<std::chrono::high_resolution_clock> start, end;

        for (int i = 0; i < max; i++)
        {
            store.put(i, std::string(i + 1, 's'));
        }

        for (uint64_t i = 0; i < max; i++)
        {
            start = std::chrono::high_resolution_clock::now();
            store.get(i);
            end = std::chrono::high_resolution_clock::now();
            get_latencies.push_back(std::chrono::duration<double, std::micro>(end - start).count());
        }

        for (uint64_t i = 0; i < max; i++)
        {
            start = std::chrono::high_resolution_clock::now();
            store.get_without_cache(i);
            end = std::chrono::high_resolution_clock::now();
            get_without_cache_latencies.push_back(std::chrono::duration<double, std::micro>(end - start).count());
        }

        for (uint64_t i = 0; i < max; i++)
        {
            start = std::chrono::high_resolution_clock::now();
            store.get_without_bloomfilter(i);
            end = std::chrono::high_resolution_clock::now();
            get_without_bloomfilter_latencies.push_back(std::chrono::duration<double, std::micro>(end - start).count());
        }

        // Report latencies and throughput
        auto average_latency = [](const std::vector<double>& latencies) {
            double sum = 0;
            for (const auto& latency : latencies) sum += latency;
            return sum / latencies.size();
        };

        auto throughput = [](const std::vector<double>& latencies) {
            double total_time = 0;
            for (const auto& latency : latencies) total_time += latency;
            return latencies.size() * 1e6 / total_time; // Operations per second
        };

        std::cout << "Average GET latency: " << average_latency(get_latencies) << " microseconds\n";
        std::cout << "GET throughput: " << throughput(get_latencies) << " ops/sec\n";
        std::cout << "Average GET WITHOUT CACHE latency: " << average_latency(get_without_cache_latencies) << " microseconds\n";
        std::cout << "GET WITHOUT CACHE throughput: " << throughput(get_without_cache_latencies) << " ops/sec\n";
        std::cout << "Average GET WITHOUT BLOOMFILTER latency: " << average_latency(get_without_bloomfilter_latencies) << " microseconds\n";
        std::cout << "GET WITHOUT BLOOMFILTER throughput: " << throughput(get_without_bloomfilter_latencies) << " ops/sec\n";
	}

    void compaction_test(uint64_t max)
    {
        
    }

    void bloomfilter_test(uint64_t max)
    {
        std::cout << "Bloomfilter Size: " << bloomfilter_size << "bytes" << std::endl;
        uint64_t i;
        uint64_t key_size = sizeof(uint64_t);
        std::vector<double> put_latencies, get_latencies;
        std::chrono::time_point<std::chrono::high_resolution_clock> start, end;

        // Test a single key
        start = std::chrono::high_resolution_clock::now();
        store.put(1, "SE");
        end = std::chrono::high_resolution_clock::now();
        put_latencies.push_back(std::chrono::duration<double, std::micro>(end - start).count());

        start = std::chrono::high_resolution_clock::now();
        store.get(1);
        end = std::chrono::high_resolution_clock::now();
        get_latencies.push_back(std::chrono::duration<double, std::micro>(end - start).count());

        store.del(1);

        // Test multiple key-value pairs
        for (i = 0; i < max; ++i)
        {
            start = std::chrono::high_resolution_clock::now();
            store.put(i, std::string(i + 1, 's'));
            end = std::chrono::high_resolution_clock::now();
            put_latencies.push_back(std::chrono::duration<double, std::micro>(end - start).count());
        }

        // Measure Get latencies
        for (i = 0; i < max; ++i)
        {
            start = std::chrono::high_resolution_clock::now();
            store.get(i);
            end = std::chrono::high_resolution_clock::now();
            get_latencies.push_back(std::chrono::duration<double, std::micro>(end - start).count());
        }

        // Measure Scan latencies
        std::list<std::pair<uint64_t, std::string>> list_ans;
        std::list<std::pair<uint64_t, std::string>> list_stu;

        for (i = 0; i < max / 2; ++i)
        {
            list_ans.emplace_back(std::make_pair(i, std::string(i + 1, 's')));
        }

        // Report latencies and throughput
        auto average_latency = [](const std::vector<double>& latencies) {
            double sum = 0;
            for (const auto& latency : latencies) sum += latency;
            return sum / latencies.size();
        };

        auto throughput = [](const std::vector<double>& latencies) {
            double total_time = 0;
            for (const auto& latency : latencies) total_time += latency;
            return latencies.size() * 1e6 / total_time; // Operations per second
        };

        std::cout << "Average PUT latency: " << average_latency(put_latencies) << " microseconds\n";
        std::cout << "PUT throughput: " << throughput(put_latencies) << " ops/sec\n";
        
        std::cout << "Average GET latency: " << average_latency(get_latencies) << " microseconds\n";
        std::cout << "GET throughput: " << throughput(get_latencies) << " ops/sec\n";
    }

public:
	PerformanceTest(const std::string &dir, const std::string &vlog, bool v = true) : Test(dir, vlog, v)
	{
	}

	void start_test(void *args = NULL) override
	{
		std::cout << "KVStore Performance Test" << std::endl;

		store.reset();

		// std::cout << "[Regular Test]" << std::endl;
		// regular_test(SIMPLE_TEST_MAX);

        // store.reset();

        // std::cout << "[Large Regular Test]" << std::endl;
        // regular_test(LARGE_TEST_MAX);

		// store.reset();

		// std::cout << "[Cache Test]" << std::endl;
		// cache_test(SIMPLE_TEST_MAX);

		// store.reset();

        // std::cout << "[Large Cache Test]" << std::endl;
        // cache_test(LARGE_TEST_MAX);

        // store.reset();

		// std::cout << "[Compaction Test]" << std::endl;
        // compaction_test(LARGE_TEST_MAX);

        store.reset();

        std::cout << "[Bloomfilter Test]" << std::endl;
        bloomfilter_test(LARGE_TEST_MAX);
	}
};

int main(int argc, char *argv[])
{
	bool verbose = (argc == 2 && std::string(argv[1]) == "-v");

	std::cout << "Usage: " << argv[0] << " [-v]" << std::endl;
	std::cout << "  -v: print extra info for failed tests [currently ";
	std::cout << (verbose ? "ON" : "OFF") << "]" << std::endl;
	std::cout << std::endl;
	std::cout.flush();

	PerformanceTest test("./data", "./data/vlog", verbose);

	test.start_test();

	return 0;
}
