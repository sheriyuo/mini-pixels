//
// Created by yuly on 05.04.23.
//

#include "vector/DecimalColumnVector.h"
#include "duckdb/common/types/decimal.hpp"

/**
 * The decimal column vector with precision and scale.
 * The values of this column vector are the unscaled integer value
 * of the decimal. For example, the unscaled value of 3.14, which is
 * of the type decimal(3,2), is 314. While the precision and scale
 * of this decimal are 3 and 2, respectively.
 *
 * <p><b>Note: it only supports short decimals with max precision
 * and scale 18.</b></p>
 *
 * Created at: 05/03/2022
 * Author: hank
 */

DecimalColumnVector::DecimalColumnVector(int precision, int scale,
                                         bool encoding)
    : ColumnVector(VectorizedRowBatch::DEFAULT_SIZE, encoding) {
    DecimalColumnVector(VectorizedRowBatch::DEFAULT_SIZE, precision, scale,
                        encoding);
}

DecimalColumnVector::DecimalColumnVector(uint64_t len, int precision, int scale,
                                         bool encoding)
    : ColumnVector(len, encoding) {
    // decimal column vector has no encoding so we don't allocate memory to
    // this->vector
    posix_memalign(reinterpret_cast<void **>(&this->vector), 32,
                   len * sizeof(int64_t));
    this->precision = precision;
    this->scale = scale;
    memoryUsage += (uint64_t)sizeof(uint64_t) * len + sizeof(int) * 2;
}

void DecimalColumnVector::close() {
    if (!closed) {
        ColumnVector::close();
        if (physical_type_ == PhysicalType::INT16 ||
            physical_type_ == PhysicalType::INT32) {
            free(vector);
        }
        vector = nullptr;
    }
}

void DecimalColumnVector::print(int rowCount) {
    //    throw InvalidArgumentException("not support print
    //    Decimalcolumnvector.");
    for (int i = 0; i < rowCount; i++) {
        std::cout << vector[i] << std::endl;
    }
}

void DecimalColumnVector::add(std::string &val) {
    if (writeIndex >= length) {
        ensureSize(writeIndex * 2, true);
    }

    long a = 0, b = 0, cnt = 0;
    std::sscanf(val.c_str(), "%d.%d", &a, &b);
    for (long t = b; t > 0; t /= 10) {
        cnt++;
        a *= 10;
    }

    long value = a > 0 ? a + b : a - b;
    while (cnt > scale + 1) {
        value /= 10;
        cnt--;
    }
    if (cnt > scale) {
        long t = value % 10;
        if (value > 0) {
            value = value / 10 + (t >= 5);
        } else {
            value = value / 10 - (t <= -5);
        }
    }
    while (cnt < scale) {
        value *= 10;
        cnt++;
    }
    vector[writeIndex] = value;
    isNull[writeIndex] = false;
    writeIndex++;
}

void DecimalColumnVector::add(int64_t value) {
    throw std::invalid_argument("Invalid argument type");
}

void DecimalColumnVector::add(int value) {
    throw std::invalid_argument("Invalid argument type");
}

void DecimalColumnVector::ensureSize(uint64_t size, bool preserveData) {
    ColumnVector::ensureSize(size, preserveData);
    if (length < size) {
        long *oldVector = vector;
        posix_memalign(reinterpret_cast<void **>(&vector), 32,
                       size * sizeof(int64_t));
        if (preserveData) {
            std::copy(oldVector, oldVector + length, vector);
        }
        delete[] oldVector;
        memoryUsage += (uint64_t)sizeof(uint64_t) * (size - length);
        resize(size);
    }
}

DecimalColumnVector::~DecimalColumnVector() {
    if (!closed) {
        DecimalColumnVector::close();
    }
}

void *DecimalColumnVector::current() {
    if (vector == nullptr) {
        return nullptr;
    } else {
        return vector + readIndex;
    }
}

int DecimalColumnVector::getPrecision() { return precision; }

int DecimalColumnVector::getScale() { return scale; }
