#pragma once

#include <cmath>
#include <cstdint>
#include <random>
#include <stdexcept>
#include <string>


class ZipfianGenerator {
public:
  ZipfianGenerator(uint64_t n, double theta) : n_(n), theta_(theta) {
    if (n < 1) {
      throw std::invalid_argument("n must be >= 1");
    }
    if (theta <= 0.0 || theta >= 1.0) {
      throw std::invalid_argument("theta must be in (0, 1)");
    }

    zeta_n_ = zeta(n, theta);
    zeta_2_ = zeta(2, theta);

    alpha_ = 1.0 / (1.0 - theta);
    eta_ = (1.0 - std::pow(2.0 / static_cast<double>(n), 1.0 - theta)) /
           (1.0 - zeta_2_ / zeta_n_);
  }

  template <typename RNG>
  uint64_t next(RNG &rng) {
    std::uniform_real_distribution<double> dist(0.0, 1.0);
    double u = dist(rng);
    double uz = u * zeta_n_;

    if (uz < 1.0) {
      return 1;
    }
    if (uz < 1.0 + std::pow(0.5, theta_)) {
      return 2;
    }

    uint64_t result = 1 + static_cast<uint64_t>(
        static_cast<double>(n_) *
        std::pow(eta_ * u - eta_ + 1.0, alpha_));

    if (result > n_) {
      result = n_;
    }
    return result;
  }

 
  template <typename RNG>
  std::string next_key(RNG &rng) {
    return "key_" + std::to_string(next(rng));
  }

  uint64_t n() const { return n_; }
  double theta() const { return theta_; }

private:
  static double zeta(uint64_t n, double theta) {
    double sum = 0.0;
    for (uint64_t i = 1; i <= n; ++i) {
      sum += 1.0 / std::pow(static_cast<double>(i), theta);
    }
    return sum;
  }

  uint64_t n_;
  double theta_;
  double zeta_n_;
  double zeta_2_;
  double alpha_;
  double eta_;
};
