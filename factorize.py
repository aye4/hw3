import multiprocessing
import concurrent.futures
import time

NUMBER_OF_PROCESSES = multiprocessing.cpu_count()


def factorize_better(number: int) -> int:
    if number <= 0:
        return []
    square_root = int(number**0.5)
    result = [i for i in range(1, square_root + 1) if number % i == 0]
    result2 = list(map(lambda x: number // x, result[::-1]))
    return result + result2[1:] if result[-1] == result2[0] else result + result2


def factorize_simple(number: int) -> int:
    return [i for i in range(1, number + 1) if number % i == 0] if number >= 0 else []


def factorize(*number) -> list[int]:
    return [factorize_simple(i) for i in number]


def factorize_pool_imap(*number) -> list[int]:
    with multiprocessing.Pool(processes=NUMBER_OF_PROCESSES) as pool:
        return list(pool.imap(factorize_simple, number))


def factorize_pool_map(*number) -> list[int]:
    with multiprocessing.Pool(processes=NUMBER_OF_PROCESSES) as pool:
        return list(pool.map(factorize_simple, number))


def factorize_futures(*number) -> list[int]:
    with concurrent.futures.ProcessPoolExecutor(NUMBER_OF_PROCESSES) as pool:
        return list(pool.map(factorize_simple, number))


def test(func) -> None:
    start = time.time()
    a, b, c, d, e, f = func(128, 255, 99999, 10651060, 10651060, 10651060)
    
    assert a == [1, 2, 4, 8, 16, 32, 64, 128]
    assert b == [1, 3, 5, 15, 17, 51, 85, 255]
    assert c == [1, 3, 9, 41, 123, 271, 369, 813, 2439, 11111, 33333, 99999]
    assert d == e == f == [1, 2, 4, 5, 7, 10, 14, 20, 28, 35, 70, 140, 76079, 152158, 304316, 380395, 532553, 760790, 1065106, 1521580, 2130212, 2662765, 5325530, 10651060]

    finish = time.time()

    print(f"{func.__name__:<20} : {finish - start:6.4f}")


if __name__ == "__main__":
    test(factorize)
    print(f"NUMBER_OF_PROCESSES = {NUMBER_OF_PROCESSES}")
    test(factorize_pool_imap)
    test(factorize_pool_map)
    test(factorize_futures)
