//
// Created by liyu on 12/23/23.
//

#include "vector/TimestampColumnVector.h"
#include <ctime>
#include <stdio.h>

TimestampColumnVector::TimestampColumnVector(int precision, bool encoding)
    : ColumnVector(VectorizedRowBatch::DEFAULT_SIZE, encoding) {
    TimestampColumnVector(VectorizedRowBatch::DEFAULT_SIZE, precision,
                          encoding);
}

TimestampColumnVector::TimestampColumnVector(uint64_t len, int precision,
                                             bool encoding)
    : ColumnVector(len, encoding) {
    this->precision = precision;
    // if (encoding) {
    posix_memalign(reinterpret_cast<void **>(&this->times), 64,
                   len * sizeof(long));
    // } else {
    //     this->times = nullptr;
    // }
}

void TimestampColumnVector::close() {
    if (!closed) {
        ColumnVector::close();
        if (encoding && this->times != nullptr) {
            free(this->times);
        }
        this->times = nullptr;
    }
}

void TimestampColumnVector::print(int rowCount) {
    throw InvalidArgumentException("not support print longcolumnvector.");
    //    for(int i = 0; i < rowCount; i++) {
    //        std::cout<<longVector[i]<<std::endl;
    //		std::cout<<intVector[i]<<std::endl;
    //    }
}

void TimestampColumnVector::ensureSize(uint64_t size, bool preserveData) {
    ColumnVector::ensureSize(size, preserveData);
    if (length < size) {
        long *oldTimes = times;
        posix_memalign(reinterpret_cast<void **>(&times), 64,
                       size * sizeof(long));
        if (preserveData) {
            std::copy(oldTimes, oldTimes + length, times);
        }
        delete[] oldTimes;
        memoryUsage += (long)sizeof(long) * (size - length);
        resize(size);
    }
}

void TimestampColumnVector::add(std::string &val) {
    int year = 0, month = 0, day = 0, hour = 0, minute = 0, second = 0;
    int us = 0;
    std::sscanf(val.c_str(), "%d-%d-%d %d:%d:%d.%d", &year, &month, &day, &hour,
                &minute, &second, &us);

    std::tm time = {0};
    time.tm_year = year - 1900;
    time.tm_mon = month - 1;
    time.tm_mday = day;
    time.tm_hour = hour;
    time.tm_min = minute;
    time.tm_sec = second;
    time.tm_isdst = -1;
    time.tm_zone = "CST";
    time_t time_ts = mktime(&time);

    std::tm epoch = {0};
    epoch.tm_year = 70;
    epoch.tm_mon = 0;
    epoch.tm_mday = 1;
    long epoch_ts = mktime(&epoch);
    time_ts -= epoch_ts;

    if (time_ts == -1) {
        throw InvalidArgumentException("Error converting to timestamp!");
    }
    add(time_ts * 1000000 + us);
}

void TimestampColumnVector::add(int64_t value) {
    if (writeIndex >= length) {
        ensureSize(writeIndex * 2, true);
    }
    set(writeIndex++, value);
}

TimestampColumnVector::~TimestampColumnVector() {
    if (!closed) {
        TimestampColumnVector::close();
    }
}

void *TimestampColumnVector::current() {
    if (this->times == nullptr) {
        return nullptr;
    } else {
        return this->times + readIndex;
    }
}

/**
 * Set a row from a value, which is the days from 1970-1-1 UTC.
 * We assume the entry has already been isRepeated adjusted.
 *
 * @param elementNum
 * @param days
 */
void TimestampColumnVector::set(int elementNum, long ts) {
    if (elementNum >= writeIndex) {
        writeIndex = elementNum + 1;
    }
    times[elementNum] = ts;
    // TODO: isNull
}