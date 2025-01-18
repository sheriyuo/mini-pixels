//
// Created by yuly on 06.04.23.
//

#include "vector/DateColumnVector.h"
#include <chrono>
#include <ctime>
#include <stdio.h>

DateColumnVector::DateColumnVector(uint64_t len, bool encoding)
    : ColumnVector(len, encoding) {
    // if (encoding) {
    posix_memalign(reinterpret_cast<void **>(&dates), 32,
                    len * sizeof(int32_t));
    // } else {
    //     this->dates = nullptr;
    // }
    memoryUsage += (long)sizeof(int) * len;
}

void DateColumnVector::close() {
    if (!closed) {
        if (encoding && dates != nullptr) {
            free(dates);
        }
        dates = nullptr;
        ColumnVector::close();
    }
}

void DateColumnVector::print(int rowCount) {
    for (int i = 0; i < rowCount; i++) {
        std::cout << dates[i] << std::endl;
    }
}

DateColumnVector::~DateColumnVector() {
    if (!closed) {
        DateColumnVector::close();
    }
}

/**
 * Set a row from a value, which is the days from 1970-1-1 UTC.
 * We assume the entry has already been isRepeated adjusted.
 *
 * @param elementNum
 * @param days
 */
void DateColumnVector::set(int elementNum, int days) {
    if (elementNum >= writeIndex) {
        writeIndex = elementNum + 1;
    }
    dates[elementNum] = days;
    // TODO: isNull
    isNull[elementNum] = false;
}

void *DateColumnVector::current() {
    if (dates == nullptr) {
        return nullptr;
    } else {
        return dates + readIndex;
    }
}

void DateColumnVector::ensureSize(uint64_t size, bool preserveData) {
    ColumnVector::ensureSize(size, preserveData);
    if (length < size) {
        int *oldDates = dates;
        posix_memalign(reinterpret_cast<void **>(&dates), 32,
                       size * sizeof(int));
        if (preserveData) {
            std::copy(oldDates, oldDates + length, dates);
        }
        delete[] oldDates;
        memoryUsage += (long)sizeof(long) * (size - length);
        resize(size);
    }
}

void DateColumnVector::add(std::string &val) {
    int year, month, day;
    if (std::sscanf(val.c_str(), "%d-%d-%d", &year, &month, &day) < 3) {
        std::cerr << "Invalid date format!\n";
        return;
    }

    std::tm time = {};
    time.tm_year = year - 1900;
    time.tm_mon = month - 1;
    time.tm_mday = day;
    time.tm_isdst = -1;
    time_t time_ts = mktime(&time);

    std::tm epoch = {0};
    epoch.tm_year = 70;
    epoch.tm_mon = 0;
    epoch.tm_mday = 1;
    long epoch_ts = mktime(&epoch);

    auto time_tp = std::chrono::system_clock::from_time_t(time_ts);
    auto epoch_tp = std::chrono::system_clock::from_time_t(epoch_ts);
    auto days =
        std::chrono::duration_cast<std::chrono::hours>(time_tp - epoch_tp)
            .count() /
        24;
    add((int)days);
}

void DateColumnVector::add(bool value) { add(value ? 1 : 0); }

void DateColumnVector::add(int value) {
    if (writeIndex >= length) {
        ensureSize(writeIndex * 2, true);
    }
    set(writeIndex++, value);
}