#include <chrono>
#include <iostream>
#include <string>
#include <vector>

int main()
{
    std::string input_json;
    std::getline(std::cin, input_json, '\0');

    long long start_val = 0;
    long long end_val = 0;

    size_t start_pos = input_json.find("\"start\":");
    if (start_pos != std::string::npos) {
        start_val = std::stoll(input_json.substr(start_pos + 8));
    }

    size_t end_pos = input_json.find("\"end\":");
    if (end_pos != std::string::npos) {
        end_val = std::stoll(input_json.substr(end_pos + 6));
    }

    auto t_start = std::chrono::high_resolution_clock::now();

    std::vector<long long> found_primes;
    for (long long i = start_val; i <= end_val; ++i) {
        if (i < 2) {
            continue;
        }
        bool is_prime = true;
        for (long long j = 2; j * j <= i; ++j) {
            if (i % j == 0) {
                is_prime = false;
                break;
            }
        }
        if (is_prime) {
            found_primes.push_back(i);
        }
    }

    auto t_end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> compute_diff = t_end - t_start;

    std::cout << "{\n"
              << "  \"start\": " << start_val << ",\n"
              << "  \"end\": " << end_val << ",\n"
              << "  \"count\": " << found_primes.size() << ",\n"
              << "  \"compute_time_sec\": " << compute_diff.count() << ",\n"
              << "  \"primes\": [";

    for (size_t i = 0; i < found_primes.size(); ++i) {
        std::cout << found_primes[i];
        if (i != found_primes.size() - 1) {
            std::cout << ", ";
        }
    }
    std::cout << "]\n}\n";

    return 0;
}
